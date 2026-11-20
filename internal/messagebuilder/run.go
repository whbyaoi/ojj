package messagebuilder

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"ojj/internal/cancelerr"
	"ojj/internal/minipi"
)

var errHandlerPanic = errors.New("messagebuilder: handler panicked")

// Run 同时运行窗口收束及一个串行投递器，慢下游不阻塞收束。
// 同一 Builder 仅允许一个 Run，退出前取消并等待旧回调；可取消后再次 Run。
// nil Handler 只收束，不领取消息。关闭不强制收束，存储错误终止 Run。
// 仅记录固定分类；返回的存储/回调错误可能含敏感信息，调用者不得直接记录。
func (b *Builder) Run(ctx context.Context, handler Handler) (err error) {
	started := time.Now()
	defer func() { minipi.LogOperation(ctx, "messagebuilder", "run_end", started, err, slog.LevelInfo) }()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.running {
		b.mu.Unlock()
		return ErrRunning
	}
	b.running = true
	b.wg.Add(1)
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.running = false; b.mu.Unlock(); b.wg.Done() }()
	minipi.LogOperation(ctx, "messagebuilder", "run_start", started, nil, slog.LevelInfo)
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := b.recoverProcessing(child); err != nil {
		if err == ErrClosed && b.ctx.Err() != nil {
			return b.ctx.Err()
		}
		return err
	}
	results := make(chan error, 2)
	workers := 1
	go func() { results <- b.buildLoop(child) }()
	if handler != nil {
		workers++
		go func() { results <- b.deliveryLoop(child, handler) }()
	}
	var result error
	for range workers {
		err := <-results
		// Close 与下一轮领取/收束竞争时，operation 可能先观察到 closed。
		// 只转换精确 ErrClosed，不能吞掉 joined 的其他真实错误。
		if err == ErrClosed && b.ctx.Err() != nil {
			err = b.ctx.Err()
		}
		if !cancelerr.Only(err, child.Err()) {
			result = errors.Join(result, err)
		}
		cancel()
	}
	return errors.Join(result, ctx.Err(), b.ctx.Err())
}

func (b *Builder) recoverProcessing(ctx context.Context) (err error) {
	started := time.Now()
	var count int64
	defer func() {
		if err != nil || count > 0 {
			minipi.LogOperation(ctx, "messagebuilder", "recover", started, err, slog.LevelInfo, slog.Int64("count", count))
		}
	}()
	op, done, err := b.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	result, err := b.db.ExecContext(op, `UPDATE messages SET status='retry',next_at=?,last_error='interrupted' WHERE status='processing'`, time.Now().UnixNano())
	if err == nil {
		count, _ = result.RowsAffected()
	}
	return err
}

func (b *Builder) buildLoop(ctx context.Context) error {
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.settle(ctx, time.Now); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Builder) claim(ctx context.Context) (m Message, found bool, err error) {
	started := time.Now()
	defer func() {
		if err != nil || found {
			minipi.LogOperation(ctx, "messagebuilder", "claim", started, err, slog.LevelDebug, slog.Int64("attempt", m.Attempts), slog.String("message_id", minipi.LogID(m.ID)))
		}
	}()
	op, done, err := b.operation(ctx)
	if err != nil {
		return m, false, err
	}
	defer done()
	tx, err := b.db.BeginTx(op, nil)
	if err != nil {
		return m, false, err
	}
	defer rollback(tx, &err)
	// 严格按序交付，前一条退避时不越过；避免下游观察到顺序反转。
	m, err = scanMessage(tx.QueryRowContext(op, "SELECT "+messageColumns+" FROM messages WHERE status!='succeeded' ORDER BY sequence LIMIT 1"))
	if errors.Is(err, sql.ErrNoRows) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	now := time.Now()
	if m.Status == StatusProcessing || m.NextAttemptAt.After(now) {
		return m, false, nil
	}
	_, err = tx.ExecContext(op, `UPDATE messages SET status='processing',attempts=attempts+1,started_at=?,next_at=0 WHERE sequence=?`, now.UnixNano(), m.Sequence)
	if err != nil {
		return m, false, err
	}
	if err = commit(op, tx); err != nil {
		return m, false, err
	}
	m.Status, m.StartedAt, m.NextAttemptAt = StatusProcessing, now.UTC(), time.Time{}
	m.Attempts++
	return m, true, nil
}

func (b *Builder) deliveryLoop(ctx context.Context, handler Handler) error {
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, found, err := b.claim(ctx)
		if err != nil {
			return err
		}
		if found {
			started := time.Now()
			minipi.LogOperation(ctx, "messagebuilder", "delivery_start", started, nil, slog.LevelDebug,
				slog.Int64("attempt", m.Attempts), slog.String("message_id", minipi.LogID(m.ID)))
			call, cancel := context.WithTimeout(ctx, b.cfg.DeliveryTimeout)
			outcome := invoke(call, handler, m)
			// 超时后即使回调迟到返回 nil 也不能确认成功；继续使用稳定 ID 重试。
			if call.Err() != nil {
				outcome = errors.Join(outcome, call.Err())
			}
			cancel()
			persistErr := b.finish(m, outcome)
			level := slog.LevelDebug
			if outcome != nil {
				level = slog.LevelWarn
			}
			if persistErr != nil {
				level = slog.LevelError
			}
			minipi.LogOperation(ctx, "messagebuilder", "delivery_end", started, errors.Join(outcome, persistErr), level,
				slog.Int64("attempt", m.Attempts), slog.String("message_id", minipi.LogID(m.ID)), slog.Bool("persisted", persistErr == nil))
			if persistErr != nil {
				return errors.Join(persistErr, outcome)
			}
			if ctx.Err() != nil {
				// 不用 errors.Is 吞掉 joined cancellation + 真实回调失败。
				if cancelerr.Only(outcome, ctx.Err()) {
					outcome = nil
				}
				return errors.Join(ctx.Err(), outcome)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func invoke(ctx context.Context, handler Handler, m Message) (err error) {
	defer func() {
		if recover() != nil {
			err = errHandlerPanic
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	return handler(ctx, m)
}

func (b *Builder) retryDelay(attempts int64) time.Duration {
	d := b.cfg.RetryInitial
	for i := int64(1); i < attempts && d < b.cfg.RetryMax; i++ {
		if d > b.cfg.RetryMax/2 {
			return b.cfg.RetryMax
		}
		d *= 2
	}
	return min(d, b.cfg.RetryMax)
}

// finish 由 Run 保护数据库生命周期；独立有界 context 允许停机时记录中断。
// 不存 outcome.Error()/panic 值/堆栈，不向日志泄漏正文。
func (b *Builder) finish(m Message, outcome error) (err error) {
	started := time.Now()
	defer func() {
		stage, level := "delivery_succeeded", slog.LevelInfo
		if outcome != nil {
			stage, level = "delivery_retry", slog.LevelWarn
		}
		if err != nil {
			stage, level = "delivery_persist", slog.LevelError
		}
		minipi.LogOperation(context.Background(), "messagebuilder", stage, started, errors.Join(err, outcome), level,
			slog.Int64("attempt", m.Attempts), slog.String("message_id", minipi.LogID(m.ID)), slog.Bool("persisted", err == nil))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.OperationTimeout)
	defer cancel()
	now := time.Now()
	status := StatusSucceeded
	var next int64
	completed := now.UnixNano()
	classification := ""
	if outcome != nil {
		status, completed, next = StatusRetry, 0, addNanos(now.UnixNano(), int64(b.retryDelay(m.Attempts)))
		classification = "handler_failed"
		switch {
		case errors.Is(outcome, errHandlerPanic):
			classification = "handler_panic"
		case cancelerr.Only(outcome, context.Canceled):
			classification = "canceled"
		case cancelerr.Only(outcome, context.DeadlineExceeded):
			classification = "timeout"
		}
	}
	res, err := b.db.ExecContext(ctx, `UPDATE messages SET status=?,next_at=?,completed_at=?,last_error=? WHERE sequence=? AND status='processing' AND attempts=?`, status, next, completed, classification, m.Sequence, m.Attempts)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return ErrConflict
	}
	return err
}
