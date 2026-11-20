package event

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"ojj/internal/cancelerr"
)

// Subscription 是临时绑定。Close/父 context 取消只解绑，订阅和未来积压继续存在。
// Done 表示调度退出，不表示超时回调已退出。旧回调未退出时重绑返回 ErrBound。
type Subscription struct {
	store   *Store
	id      string
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	handler Handler
	// 以下字段由 store.mu 保护。
	running bool
	stopped bool
	err     error
}

func (sub *Subscription) Close()                { sub.cancel() }
func (sub *Subscription) Done() <-chan struct{} { return sub.done }
func (sub *Subscription) Err() error {
	sub.store.mu.Lock()
	defer sub.store.mu.Unlock()
	return sub.err
}

func canonicalFilter(f Filter) (Filter, string, error) {
	normalize := func(a []string) ([]string, error) {
		if len(a) == 0 {
			return nil, nil
		}
		if len(a) > 256 {
			return nil, fmt.Errorf("%w: too many filter values", ErrInvalid)
		}
		b := slices.Clone(a)
		for _, v := range b {
			if !validLabel(v) {
				return nil, fmt.Errorf("%w: filter value", ErrInvalid)
			}
		}
		slices.Sort(b)
		return slices.Compact(b), nil
	}
	var err error
	if f.ProducerIDs, err = normalize(f.ProducerIDs); err != nil {
		return f, "", err
	}
	if f.EventTypes, err = normalize(f.EventTypes); err != nil {
		return f, "", err
	}
	b, err := json.Marshal(f)
	return f, string(b), err
}

// Subscribe 原子注册（仅未来事件）或按相同过滤器恢复已有订阅。
// 同一 ID 的过滤器不可变；需要新过滤器时使用新 ID，显式停用旧 ID。
func (s *Store) Subscribe(ctx context.Context, spec SubscriptionSpec, handler Handler) (sub *Subscription, err error) {
	if !validLabel(spec.ID) || handler == nil {
		return nil, fmt.Errorf("%w: subscription ID or handler", ErrInvalid)
	}
	_, raw, err := canonicalFilter(spec.Filter)
	if err != nil {
		return nil, err
	}
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	unlock, err := s.lockRegistry(op)
	if err != nil {
		return nil, err
	}
	defer unlock()
	s.mu.Lock()
	_, exists := s.bindings[spec.ID]
	s.mu.Unlock()
	if exists {
		return nil, ErrBound
	}
	tx, err := s.db.BeginTx(op, nil)
	if err != nil {
		return nil, err
	}
	defer rollback(tx, &err)
	var existing string
	var enabled bool
	err = tx.QueryRowContext(op, "SELECT filter,enabled FROM subscriptions WHERE id=?", spec.ID).Scan(&existing, &enabled)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		used, e := logicalUsage(op, tx)
		if e != nil {
			return nil, e
		}
		size := int64(len(spec.ID) + len(raw) + 128)
		if size > s.cfg.MaxStorageBytes-used {
			return nil, ErrCapacity
		}
		_, err = tx.ExecContext(op, "INSERT INTO subscriptions(id,filter,enabled,created_at,logical_bytes) VALUES(?,?,1,?,?)", spec.ID, raw, time.Now().UnixNano(), size)
	case err != nil:
		return nil, err
	case existing != raw:
		return nil, ErrFilterConflict
	case !enabled:
		return nil, ErrDisabled
	}
	if err != nil {
		return nil, err
	}
	// 上次调度若因写确认失败退出，重绑时恢复，而不重置尝试次数。
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(op, `UPDATE deliveries SET status=CASE WHEN attempts>=? THEN 'failed' ELSE 'retry' END,
 token='',next_at=?,completed_at=CASE WHEN attempts>=? THEN ? ELSE 0 END,
 error_count=error_count+1,last_error='interrupted binding',error_at=?
 WHERE subscription_id=? AND status='processing'`, s.cfg.MaxAttempts, now, s.cfg.MaxAttempts, now, now, spec.ID)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	root, stop := context.WithCancel(ctx)
	sub = &Subscription{store: s, id: spec.ID, ctx: root, cancel: stop, done: make(chan struct{}), handler: handler}
	s.bindings[spec.ID] = sub
	s.wg.Add(1)
	go sub.run()
	return sub, nil
}

func (sub *Subscription) releaseLocked() {
	if sub.stopped && !sub.running && sub.store.bindings[sub.id] == sub {
		delete(sub.store.bindings, sub.id)
	}
}

func (sub *Subscription) run() {
	s := sub.store
	logger := s.cfg.Logger.With("subscription_id", sub.id)
	logger.Info("event subscription started")
	defer s.wg.Done()
	defer logger.Info("event subscription stopped")
	defer func() {
		sub.cancel()
		s.mu.Lock()
		sub.stopped = true
		sub.releaseLocked()
		close(sub.done)
		s.mu.Unlock()
	}()
	fail := func(err error) {
		s.mu.Lock()
		sub.err = err
		s.mu.Unlock()
		s.recordError("delivery", err)
	}
	for sub.ctx.Err() == nil {
		e, d, ok, err := sub.claim()
		if err != nil {
			if !cancelerr.Only(err, sub.ctx.Err()) {
				fail(err)
			}
			return
		}
		if !ok {
			if !wait(sub.ctx, s.cfg.PollInterval) {
				return
			}
			continue
		}
		callCtx, cancel := context.WithTimeout(sub.ctx, s.cfg.HandlerTimeout)
		result := make(chan error, 1)
		exited := make(chan struct{})
		s.mu.Lock()
		sub.running = true
		s.mu.Unlock()
		go func() {
			// defer 也处理回调 runtime.Goexit，不能遗留 running 标记。
			outcome := errors.New("handler exited without returning")
			defer func() {
				// 在任何内部锁等待之前记录结果及截止状态。
				if callCtx.Err() != nil {
					outcome = errors.Join(outcome, callCtx.Err())
				}
				result <- outcome
				s.mu.Lock()
				sub.running = false
				sub.releaseLocked()
				s.mu.Unlock()
				close(exited)
			}()
			if callCtx.Err() != nil {
				outcome = callCtx.Err()
				return
			}
			logger.Debug("event delivery started", "event_id", e.ID, "attempt", d.Attempts)
			outcome = invoke(callCtx, sub.handler, e)
		}()
		var outcome error
		select {
		case outcome = <-result:
		case <-callCtx.Done():
			// 调度延迟不应覆盖已经及时提交的回调结果。
			select {
			case outcome = <-result:
			default:
				outcome = callCtx.Err()
			}
		}
		cancel()
		if err = sub.finish(d, outcome); err != nil {
			fail(err)
			return
		}
		// 下一次调用必须等待旧回调及绑定清理完成，避免 running 标记交错。
		select {
		case <-exited:
		case <-sub.ctx.Done():
			return
		}
	}
}

// invoke 也隔离异常 Error 方法；不保存 panic 值，避免意外泄露正文。
func invoke(ctx context.Context, h Handler, e Event) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("handler panicked")
		}
	}()
	err = h(ctx, e)
	if err != nil {
		text := err.Error()
		if len(text) > 1024 {
			text = text[:1024]
		}
		// Snapshot untrusted error text while retaining only safe cancellation identity.
		switch {
		case cancelerr.Only(err, context.Canceled):
			err = context.Canceled
		case cancelerr.Only(err, context.DeadlineExceeded):
			err = context.DeadlineExceeded
		default:
			err = errors.New(text)
		}
	}
	return err
}

func wait(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
