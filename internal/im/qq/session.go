package qq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
	"unicode/utf8"

	"ojj/internal/cancelerr"

	"github.com/gorilla/websocket"
)

type payload struct {
	Op   *int            `json:"op"`
	Data json.RawMessage `json:"d"`
	Seq  *int64          `json:"s"`
	Type string          `json:"t"`
}

type session struct {
	id        string
	committed *int64
}
type readResult struct {
	p   payload
	err error
}
type processed struct {
	p   payload
	err error
}

// Run 阻塞至取消或不可恢复错误；连续不稳定连接最多重试配置次数。
// 会话只在内存恢复。坏消息/发布失败停止接收，交给调用者处理，不跳过游标。
func (c *Client) Run(ctx context.Context) (retErr error) {
	if !c.running.CompareAndSwap(false, true) {
		c.logger().Warn("QQ run rejected", "failure", "already_running")
		return ErrRunning
	}
	defer c.running.Store(false)
	started := time.Now()
	c.logger().Info("QQ client started")
	defer func() {
		if retErr != nil {
			logFailure(c.logger(), slog.LevelError, "run", retErr)
		}
		c.logger().Info("QQ client stopped", "duration", time.Since(started))
	}()
	state := &c.state
	delay := c.cfg.RetryInitial
	failures := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		err := c.connect(ctx, state)
		var fatal *permanentError
		if errors.As(err, &fatal) {
			return fatal.error
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err == nil || cancelerr.Only(err, ctxErr) {
				return ctxErr
			}
			return errors.Join(ctxErr, err)
		}
		if time.Since(start) >= time.Minute {
			failures = 0
			delay = c.cfg.RetryInitial
		}
		failures++
		if failures >= c.cfg.MaxReconnectAttempts {
			return fmt.Errorf("qq: reconnect attempts exhausted: %w", err)
		}
		jitter := time.Duration(rand.Int64N(int64(delay/4) + 1))
		c.logger().Warn("QQ 连接中断，等待重连", "attempt", failures, "retry_delay", delay+jitter, "failure", "connection_interrupted")
		if waitErr := sleep(ctx, delay+jitter); waitErr != nil {
			return errors.Join(waitErr, err)
		}
		if delay < c.cfg.RetryMax/2 {
			delay *= 2
		} else {
			delay = c.cfg.RetryMax
		}
	}
}

func (c *Client) connect(ctx context.Context, state *session) error {
	token, err := c.accessToken(ctx)
	if err != nil {
		return err
	}
	resume := state.id != "" && state.committed != nil
	endpoint, err := c.gateway(ctx, token, !resume)
	if err != nil {
		return err
	}
	if !resume {
		if wait := time.Until(c.identifyAfter); wait > 0 {
			if err := sleep(ctx, wait); err != nil {
				return err
			}
		}
	}
	started := time.Now()
	c.logger().Debug("QQ gateway connecting", "resume", resume)
	conn, response, err := c.dialer.DialContext(ctx, endpoint, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		if cancelerr.Only(err, ctx.Err()) {
			return ctx.Err()
		}
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		c.logger().Warn("QQ gateway connection failed", "failure", "connection_failure", "http_status", status, "duration", time.Since(started))
		return errors.New("qq: websocket connection failure")
	}
	defer conn.Close()
	c.logger().Info("QQ gateway connected", "resume", resume, "duration", time.Since(started))
	err = c.serve(ctx, conn, token, state)
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		code := closeErr.Code
		// 不将服务端关闭原因加入错误，避免正文或秘密进入日志。
		safe := fmt.Errorf("qq: gateway closed (%d)", code)
		switch {
		case code == 4006 || code == 4007 || (code >= 4900 && code <= 4913):
			*state = session{}
			c.logger().Warn("QQ 会话失效，重新鉴权可能存在消息缺口", "code", code)
			return safe
		case code == 4008 || code == 4009:
			return safe
		case code >= 4000:
			return permanent(safe)
		default:
			return safe
		}
	}
	return err
}

func write(conn *websocket.Conn, op int, data any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	if err := conn.WriteJSON(struct {
		Op   int `json:"op"`
		Data any `json:"d"`
	}{op, data}); err != nil {
		return errors.New("qq: websocket write failure")
	}
	return nil
}

func (c *Client) serve(parent context.Context, conn *websocket.Conn, token string, state *session) (resultErr error) {
	ctx, cancel := context.WithCancel(parent)
	conn.SetReadLimit(1 << 20)
	incoming := make(chan readResult, 1)
	jobs := make(chan payload, 64)
	results := make(chan processed, 1)
	readerDone, workerDone := make(chan struct{}), make(chan struct{})
	var workerErr error // 仅工作协程写入；等待 workerDone 后读取。
	go func() {
		defer close(readerDone)
		for {
			_, raw, err := conn.ReadMessage()
			var p payload
			if err == nil && (!utf8.Valid(raw) || json.Unmarshal(raw, &p) != nil || p.Op == nil) {
				err = permanent(ErrProtocol)
			}
			select {
			case incoming <- readResult{p, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-jobs:
				var err error
				if supported(p.Type) {
					c.logger().Debug("QQ message received", "event_type", p.Type)
					input, conversionErr := Convert(c.cfg.AppID, p.Type, p.Data)
					err = conversionErr
					if err == nil {
						publishCtx, stop := context.WithTimeout(ctx, 10*time.Second)
						started := time.Now()
						published, publishErr := c.publisher.Publish(publishCtx, input)
						err = publishErr
						stop()
						if err == nil {
							c.logger().Info("QQ message published", "event_type", p.Type, "event_id", published.Event.ID, "duplicate", published.Duplicate, "duration", time.Since(started))
						}
						if err != nil {
							// 只有会话生命周期确实取消且没有其他失败叶子时才忽略。
							// 发布自身超时及取消后的回滚失败仍须脱敏并停止接收。
							if cancelerr.Only(err, ctx.Err()) {
								return
							}
							err = ErrPublish
						}
					}
				}
				if err != nil {
					workerErr = err
				}
				select {
				case results <- processed{p, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		conn.Close()
		<-readerDone
		<-workerDone
		if workerErr != nil {
			resultErr = permanent(workerErr)
		}
	}()
	c.tokenMu.Lock()
	refreshAt := c.refreshAt
	if c.token != token {
		refreshAt = time.Now()
	}
	c.tokenMu.Unlock()
	refresh := time.NewTimer(time.Until(refreshAt))
	defer refresh.Stop()
	handshake := time.NewTimer(15 * time.Second)
	defer handshake.Stop()
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	var ticks <-chan time.Time
	var received *int64
	var lastQueued *int64
	awaitingACK, hello, ready := false, false, false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refresh.C:
			c.logger().Debug("QQ authentication refresh due")
			return errors.New("qq: refresh token and resume session")
		case <-handshake.C:
			c.logger().Warn("QQ gateway handshake timed out", "failure", "handshake_timeout")
			return errors.New("qq: gateway handshake timeout")
		case <-ticks:
			if awaitingACK {
				c.logger().Warn("QQ heartbeat acknowledgement timed out", "failure", "ack_timeout")
				return errors.New("qq: heartbeat acknowledgement timeout")
			}
			if err := write(conn, 1, received); err != nil {
				return err
			}
			awaitingACK = true
		case done := <-results:
			if done.err != nil {
				return permanent(done.err)
			}
			state.committed = done.p.Seq
		case result := <-incoming:
			if result.err != nil {
				return result.err
			}
			p := result.p
			if p.Seq != nil {
				received = p.Seq
			}
			switch *p.Op {
			case 10:
				if hello {
					return permanent(ErrProtocol)
				}
				var h struct {
					Interval int64 `json:"heartbeat_interval"`
				}
				if json.Unmarshal(p.Data, &h) != nil || h.Interval < 10 || h.Interval > 300000 {
					return permanent(ErrProtocol)
				}
				hello = true
				ticker = time.NewTicker(time.Duration(h.Interval) * time.Millisecond)
				ticks = ticker.C
				if state.id != "" && state.committed != nil {
					err := write(conn, 6, map[string]any{"token": "QQBot " + token, "session_id": state.id, "seq": *state.committed})
					if err != nil {
						return err
					}
					ready = true // 恢复阶段可在 RESUMED 之前接收补发事件。
				} else {
					c.identifyAfter = time.Now().Add(5 * time.Second)
					if err := write(conn, 2, map[string]any{"token": "QQBot " + token, "intents": 1 << 25, "shard": [2]int{0, 1}}); err != nil {
						return err
					}
				}
			case 11:
				awaitingACK = false
			case 1:
				if !hello {
					return permanent(ErrProtocol)
				}
				if err := write(conn, 1, received); err != nil {
					return err
				}
			case 7:
				return errors.New("qq: reconnect requested")
			case 9:
				var resumable bool
				if json.Unmarshal(p.Data, &resumable) != nil {
					return permanent(ErrProtocol)
				}
				if !resumable {
					*state = session{}
					c.logger().Warn("QQ 会话无效，重新鉴权可能存在消息缺口")
				}
				return errors.New("qq: invalid session")
			case 0:
				if !hello || p.Seq == nil || *p.Seq < 0 || p.Type == "" {
					return permanent(ErrProtocol)
				}
				if lastQueued != nil && *p.Seq <= *lastQueued {
					return permanent(ErrProtocol)
				}
				if p.Type == "READY" {
					var r struct {
						ID string `json:"session_id"`
					}
					if ready || json.Unmarshal(p.Data, &r) != nil || !validID(r.ID) {
						return permanent(ErrProtocol)
					}
					state.id, state.committed, ready = r.ID, p.Seq, true
					handshake.Stop()
					c.logger().Info("QQ gateway ready")
				} else if !ready {
					return permanent(ErrProtocol)
				}
				if p.Type == "RESUMED" {
					c.logger().Info("QQ gateway resumed")
				}
				// 首个有效派发表示恢复已响应；补发时长不受握手期限限制。
				handshake.Stop()
				// 即便是不支持的事件也排队推进游标，不能越过尚未提交的消息。
				select {
				case jobs <- p:
					lastQueued = p.Seq
				default:
					c.logger().Warn("QQ receive queue full", "failure", "queue_full", "queue_capacity", cap(jobs))
					return errors.New("qq: receive queue full; resume from committed cursor")
				}
			default:
				return permanent(ErrProtocol)
			}
		}
	}
}
