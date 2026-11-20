package event

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type scanner interface{ Scan(...any) error }

func scanEvent(row scanner) (Event, error) {
	var e Event
	var received int64
	var raw string
	err := row.Scan(&e.ID, &e.Sequence, &e.ProducerID, &received, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, err
	}
	e.ReceivedAt = fromNano(received)
	err = json.Unmarshal([]byte(raw), &e.Input)
	return e, err
}

// Get 返回不可变事件的独立副本。查询 API 不提供认证，接入层必须保护正文访问。
func (s *Store) Get(ctx context.Context, id string) (Event, error) {
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return Event{}, err
	}
	defer cancel()
	return scanEvent(s.db.QueryRowContext(op, "SELECT id,sequence,producer_id,received_at,input FROM events WHERE id=?", id))
}

func queryLimit(n int) (int, error) {
	if n == 0 {
		return 100, nil
	}
	if n < 0 || n > 1000 {
		return 0, fmt.Errorf("%w: limit must be 1..1000", ErrInvalid)
	}
	return n, nil
}

func validStatus(s Status) bool {
	switch s {
	case "", StatusPending, StatusProcessing, StatusRetry, StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

func (s *Store) List(ctx context.Context, q EventQuery) (events []Event, err error) {
	limit, err := queryLimit(q.Limit)
	if err != nil {
		return nil, err
	}
	if !validStatus(q.Status) || q.AfterSequence < 0 {
		return nil, ErrInvalid
	}
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	where := []string{"e.sequence>?"}
	args := []any{q.AfterSequence}
	if q.ProducerID != "" {
		where = append(where, "e.producer_id=?")
		args = append(args, q.ProducerID)
	}
	if q.EventType != "" {
		where = append(where, "e.event_type=?")
		args = append(args, q.EventType)
	}
	if !q.ReceivedFrom.IsZero() {
		where = append(where, "e.received_at>=?")
		args = append(args, q.ReceivedFrom.UnixNano())
	}
	if !q.ReceivedBefore.IsZero() {
		where = append(where, "e.received_at<?")
		args = append(args, q.ReceivedBefore.UnixNano())
	}
	if q.SubscriptionID != "" || q.Status != "" {
		clause := "EXISTS (SELECT 1 FROM deliveries d WHERE d.event_sequence=e.sequence"
		if q.SubscriptionID != "" {
			clause += " AND d.subscription_id=?"
			args = append(args, q.SubscriptionID)
		}
		if q.Status != "" {
			clause += " AND d.status=?"
			args = append(args, q.Status)
		}
		where = append(where, clause+")")
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(op, "SELECT e.id,e.sequence,e.producer_id,e.received_at,e.input FROM events e WHERE "+strings.Join(where, " AND ")+" ORDER BY e.sequence LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	events = make([]Event, 0)
	for rows.Next() {
		e, eErr := scanEvent(rows)
		if eErr != nil {
			return nil, eErr
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) ListDeliveries(ctx context.Context, q DeliveryQuery) (deliveries []Delivery, err error) {
	limit, err := queryLimit(q.Limit)
	if err != nil {
		return nil, err
	}
	if !validStatus(q.Status) || q.AfterSequence < 0 || q.Offset < 0 {
		return nil, ErrInvalid
	}
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	where := []string{"d.event_sequence>?"}
	args := []any{q.AfterSequence}
	if q.EventID != "" {
		where = append(where, "e.id=?")
		args = append(args, q.EventID)
	}
	if q.SubscriptionID != "" {
		where = append(where, "d.subscription_id=?")
		args = append(args, q.SubscriptionID)
	}
	if q.Status != "" {
		where = append(where, "d.status=?")
		args = append(args, q.Status)
	}
	args = append(args, limit, q.Offset)
	rows, err := s.db.QueryContext(op, `SELECT e.id,d.event_sequence,d.subscription_id,d.status,d.attempts,d.error_count,
 d.token,d.next_at,d.last_error,d.error_at,d.started_at,d.completed_at FROM deliveries d
 JOIN events e ON e.sequence=d.event_sequence WHERE `+strings.Join(where, " AND ")+" ORDER BY d.event_sequence,d.subscription_id LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	deliveries = make([]Delivery, 0)
	for rows.Next() {
		var d Delivery
		var next, failed, started, completed int64
		if err = rows.Scan(&d.EventID, &d.Sequence, &d.SubscriptionID, &d.Status, &d.Attempts, &d.ErrorCount, &d.AttemptToken, &next, &d.LastError, &failed, &started, &completed); err != nil {
			return nil, err
		}
		d.NextAttemptAt, d.LastErrorAt, d.StartedAt, d.CompletedAt = fromNano(next), fromNano(failed), fromNano(started), fromNano(completed)
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

func (s *Store) GetSubscription(ctx context.Context, id string) (info SubscriptionInfo, err error) {
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return info, err
	}
	defer cancel()
	var raw string
	var created, disabled int64
	err = s.db.QueryRowContext(op, "SELECT id,filter,enabled,created_at,disabled_at FROM subscriptions WHERE id=?", id).Scan(&info.ID, &raw, &info.Enabled, &created, &disabled)
	if errors.Is(err, sql.ErrNoRows) {
		return info, ErrNotFound
	}
	if err != nil {
		return info, err
	}
	info.CreatedAt, info.DisabledAt = fromNano(created), fromNano(disabled)
	err = json.Unmarshal([]byte(raw), &info.Filter)
	return info, err
}

// StorageUsage 返回逻辑字节（事件规范化 JSON 和元数据、订阅配置、每投递 2048）。
// 不是物理磁盘使用量，终态记录在清理前也占容量。
func (s *Store) StorageUsage(ctx context.Context) (n int64, err error) {
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	tx, err := s.db.BeginTx(op, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx, &err)
	return logicalUsage(op, tx)
}
