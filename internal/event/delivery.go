package event

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"ojj/internal/cancelerr"
	"time"
)

func (sub *Subscription) claim() (event Event, delivery Delivery, found bool, err error) {
	s := sub.store
	op, cancel, err := s.operation(sub.ctx)
	if err != nil {
		return event, delivery, false, err
	}
	defer cancel()
	tx, err := s.db.BeginTx(op, nil)
	if err != nil {
		return event, delivery, false, err
	}
	defer rollback(tx, &err)
	var next int64
	err = tx.QueryRowContext(op, `SELECT d.event_sequence,d.status,d.attempts,d.next_at FROM deliveries d
 JOIN subscriptions s ON s.id=d.subscription_id WHERE d.subscription_id=? AND s.enabled=1
 AND d.status IN ('pending','processing','retry') ORDER BY d.event_sequence LIMIT 1`, sub.id).Scan(&delivery.Sequence, &delivery.Status, &delivery.Attempts, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return event, delivery, false, nil
	}
	if err != nil {
		return event, delivery, false, err
	}
	now := time.Now()
	if delivery.Status == StatusProcessing || next > now.UnixNano() {
		return event, delivery, false, nil
	}
	if delivery.Attempts >= s.cfg.MaxAttempts {
		_, err = tx.ExecContext(op, `UPDATE deliveries SET status='failed',token='',completed_at=? WHERE event_sequence=? AND subscription_id=?`, now.UnixNano(), delivery.Sequence, sub.id)
		if err != nil {
			return event, delivery, false, err
		}
		err = tx.Commit()
		if err == nil {
			s.cfg.Logger.Error("event delivery failed", "subscription_id", sub.id, "event_sequence", delivery.Sequence, "attempt", delivery.Attempts, "failure", "attempts_exhausted")
		}
		return event, delivery, false, err
	}
	token, err := randomID()
	if err != nil {
		return event, delivery, false, err
	}
	var raw string
	var received int64
	err = tx.QueryRowContext(op, `SELECT id,sequence,producer_id,received_at,input FROM events WHERE sequence=?`, delivery.Sequence).Scan(&event.ID, &event.Sequence, &event.ProducerID, &received, &raw)
	if err != nil {
		return event, delivery, false, err
	}
	if err = json.Unmarshal([]byte(raw), &event.Input); err != nil {
		return event, delivery, false, err
	}
	event.ReceivedAt = fromNano(received)
	_, err = tx.ExecContext(op, `UPDATE deliveries SET status='processing',attempts=attempts+1,token=?,started_at=?,next_at=0 WHERE event_sequence=? AND subscription_id=?`, token, now.UnixNano(), delivery.Sequence, sub.id)
	if err != nil {
		return event, delivery, false, err
	}
	if err = tx.Commit(); err != nil {
		return event, delivery, false, err
	}
	delivery.Attempts++
	delivery.AttemptToken, delivery.SubscriptionID, delivery.EventID = token, sub.id, event.ID
	return event, delivery, true, nil
}

func (s *Store) retryDelay(attempts int) time.Duration {
	d := s.cfg.RetryInitial
	for i := 1; i < attempts && d < s.cfg.RetryMax; i++ {
		if d > s.cfg.RetryMax/2 {
			return s.cfg.RetryMax
		}
		d *= 2
	}
	return min(d, s.cfg.RetryMax)
}

// finish 不使用订阅 context：解绑/关闭时仍在有界时限内记录本次中断。
// 状态和随机 token 双重 CAS，取消或旧尝试的迟到结果永远不能覆盖新状态。
func (sub *Subscription) finish(d Delivery, outcome error) error {
	s := sub.store
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.OperationTimeout)
	defer cancel()
	now := time.Now()
	status := StatusSucceeded
	var next, completed, errorAt int64
	var text string
	var failures int
	completed = now.UnixNano()
	if outcome != nil {
		text, failures, errorAt = outcome.Error(), 1, now.UnixNano()
		if len(text) > 1024 {
			text = text[:1024]
		}
		if d.Attempts >= s.cfg.MaxAttempts {
			status = StatusFailed
		} else {
			status, completed, next = StatusRetry, 0, now.Add(s.retryDelay(d.Attempts)).UnixNano()
		}
	}
	r, err := s.db.ExecContext(ctx, `UPDATE deliveries SET status=?,token='',next_at=?,completed_at=?,
 error_count=error_count+?,last_error=CASE WHEN ?=1 THEN ? ELSE last_error END,
 error_at=CASE WHEN ?=1 THEN ? ELSE error_at END
 WHERE event_sequence=? AND subscription_id=? AND status='processing' AND token=?`, status, next, completed, failures, failures, text, failures, errorAt, d.Sequence, sub.id, d.AttemptToken)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected() // 0 是停用取消或旧尝试失效，不能再覆盖。
	if err != nil {
		return err
	}
	logger := s.cfg.Logger.With("subscription_id", sub.id, "event_id", d.EventID, "attempt", d.Attempts)
	if n == 0 {
		logger.Debug("event delivery result ignored")
		return nil
	}
	category := "handler_failure"
	if cancelerr.Only(outcome, context.Canceled) {
		category = "canceled"
	} else if cancelerr.Only(outcome, context.DeadlineExceeded) {
		category = "timeout"
	}
	if category == "canceled" && outcome != nil {
		logger.Debug("event delivery canceled", "failure", category, "status", status)
		return nil
	}
	switch status {
	case StatusSucceeded:
		logger.Info("event delivery succeeded")
	case StatusRetry:
		logger.Warn("event delivery retry scheduled", "failure", category, "retry_delay", s.retryDelay(d.Attempts))
	case StatusFailed:
		if category == "canceled" || category == "timeout" {
			logger.Warn("event delivery stopped", "failure", category)
		} else {
			logger.Error("event delivery failed", "failure", category)
		}
	}
	return nil
}
