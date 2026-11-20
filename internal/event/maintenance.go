package event

import (
	"context"
	"time"
)

// DisableSubscription 永久停用 ID，取消未完成积压并保留取消状态及时间。
// 不能撤销已运行回调的外部副作用；终态记录在保留期结束前不删除。
func (s *Store) DisableSubscription(ctx context.Context, id string) (err error) {
	defer func() {
		if err != nil {
			logFailure(s.cfg.Logger, "disable_subscription", err)
		} else {
			s.cfg.Logger.Info("event subscription disabled", "subscription_id", id)
		}
	}()
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	unlock, err := s.lockRegistry(op)
	if err != nil {
		return err
	}
	defer unlock()
	tx, err := s.db.BeginTx(op, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	now := time.Now().UnixNano()
	r, err := tx.ExecContext(op, `UPDATE subscriptions SET enabled=0,disabled_at=CASE WHEN disabled_at=0 THEN ? ELSE disabled_at END WHERE id=?`, now, id)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	_, err = tx.ExecContext(op, `UPDATE deliveries SET status='canceled',token='',next_at=0,completed_at=?
 WHERE subscription_id=? AND status IN ('pending','processing','retry')`, now, id)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub := s.bindings[id]; sub != nil {
		sub.cancel()
	}
	return nil
}

// Cleanup 每次最多删除 500 个事件及其投递，避免无限增长的清理事务。
// 仅事件已过 Retention 且所有投递终态也已过 Retention 才能删除。
// 无订阅事件按接收时间保留；去重键随事件一起删除，之后同键是新事件。
// 订阅身份/过滤器/停用记录永久保留，不自动 VACUUM，不承诺物理文件缩小。
func (s *Store) Cleanup(ctx context.Context) (deleted int64, err error) {
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	cutoff := time.Now().Add(-s.cfg.Retention).UnixNano()
	r, err := s.db.ExecContext(op, `DELETE FROM events WHERE sequence IN (
 SELECT e.sequence FROM events e WHERE e.received_at<? AND NOT EXISTS (
 SELECT 1 FROM deliveries d WHERE d.event_sequence=e.sequence
 AND (d.status IN ('pending','processing','retry') OR d.completed_at>=?))
 ORDER BY e.sequence LIMIT 500)`, cutoff, cutoff)
	if err != nil {
		return 0, err
	}
	deleted, err = r.RowsAffected()
	if err == nil && deleted > 0 {
		s.cfg.Logger.Info("event cleanup completed", "deleted_count", deleted)
	}
	return deleted, err
}
