package event

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Producer 是进程内能力句柄，不是认证系统；接入层必须验证身份后再绑定。
type Producer struct {
	store *Store
	id    string
}

func (s *Store) BindProducer(id string) (*Producer, error) {
	if !validLabel(id) {
		return nil, fmt.Errorf("%w: producer ID", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	return &Producer{store: s, id: id}, nil
}

func validLabel(v string) bool {
	return strings.TrimSpace(v) != "" && len(v) <= 256 && utf8.ValidString(v)
}

func normalizeInput(in Input, max int) (Input, []byte, error) {
	if in.SchemaVersion == 0 {
		in.SchemaVersion = 1
	}
	if !validLabel(in.EventType) || in.SchemaVersion != 1 || in.OccurredAt.IsZero() || in.OccurredAt.Year() < 1 || in.OccurredAt.Year() > 9999 || !utf8.ValidString(in.Content) || len(in.Content) > max {
		return in, nil, fmt.Errorf("%w: type, version, occurred time or content", ErrInvalid)
	}
	in.OccurredAt = in.OccurredAt.UTC()
	for _, v := range []string{in.DeduplicationKey, in.CorrelationID, in.CausationEventID} {
		if v != "" && !validLabel(v) {
			return in, nil, fmt.Errorf("%w: metadata", ErrInvalid)
		}
	}
	if in.Source != nil {
		copy := *in.Source
		in.Source = &copy
		for _, v := range []string{copy.Platform, copy.AccountID, copy.ConversationType, copy.ConversationID, copy.SenderID, copy.MessageID} {
			if (v != "" && !validLabel(v)) || (in.EventType == "im.message" && !validLabel(v)) {
				return in, nil, fmt.Errorf("%w: source", ErrInvalid)
			}
		}
	} else if in.EventType == "im.message" {
		return in, nil, fmt.Errorf("%w: im.message requires source", ErrInvalid)
	}
	b, err := json.Marshal(in)
	return in, b, err
}

// Publish 的成功仅表示事件和所有匹配投递已原子持久化。
// 去重比较规范化后的完整 Input，而不仅是正文；无订阅事件仍保留。
func (p *Producer) Publish(ctx context.Context, input Input) (result PublishResult, err error) {
	if p == nil || p.store == nil {
		return result, fmt.Errorf("%w: unbound producer", ErrInvalid)
	}
	s := p.store
	started := time.Now()
	defer func() {
		if err != nil {
			logFailure(s.cfg.Logger, "publish", err)
			return
		}
		if result.Duplicate {
			s.cfg.Logger.Debug("event deduplicated", "event_id", result.Event.ID, "matched_subscriptions", result.MatchedSubscriptions)
			return
		}
		s.cfg.Logger.Info("event published", "event_id", result.Event.ID, "matched_subscriptions", result.MatchedSubscriptions, "duration", time.Since(started))
	}()
	op, cancel, err := s.operation(ctx)
	if err != nil {
		return result, err
	}
	defer cancel()
	in, body, err := normalizeInput(input, s.cfg.MaxContentBytes)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(op, nil)
	if err != nil {
		return result, err
	}
	defer rollback(tx, &err)
	if in.DeduplicationKey != "" {
		var stored string
		var received int64
		e := Event{ProducerID: p.id}
		err = tx.QueryRowContext(op, `SELECT id,sequence,received_at,input FROM events WHERE producer_id=? AND dedup_key=?`, p.id, in.DeduplicationKey).Scan(&e.ID, &e.Sequence, &received, &stored)
		if err == nil {
			if stored != string(body) {
				return result, ErrConflict
			}
			e.Input, e.ReceivedAt = in, fromNano(received)
			result.Event, result.Duplicate = e, true
			if err = tx.QueryRowContext(op, "SELECT COUNT(*) FROM deliveries WHERE event_sequence=?", e.Sequence).Scan(&result.MatchedSubscriptions); err != nil {
				return result, err
			}
			return result, tx.Commit()
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return result, err
		}
	}
	rows, err := tx.QueryContext(op, "SELECT id,filter FROM subscriptions WHERE enabled=1")
	if err != nil {
		return result, err
	}
	var matches []string
	for rows.Next() {
		var id, raw string
		if err = rows.Scan(&id, &raw); err != nil {
			break
		}
		var filter Filter
		if err = json.Unmarshal([]byte(raw), &filter); err != nil {
			break
		}
		if filter.matches(p.id, in.EventType) {
			matches = append(matches, id)
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return result, err
	}
	for _, id := range matches {
		var count int
		if err = tx.QueryRowContext(op, `SELECT COUNT(*) FROM deliveries WHERE subscription_id=? AND status IN ('pending','processing','retry')`, id).Scan(&count); err != nil {
			return result, err
		}
		if count >= s.cfg.MaxPendingPerSubscription {
			return result, fmt.Errorf("%w: subscription backlog", ErrCapacity)
		}
	}
	size := int64(len(body) + len(p.id) + len(in.EventType) + len(in.DeduplicationKey) + 128)
	used, err := logicalUsage(op, tx)
	if err != nil {
		return result, err
	}
	if size > s.cfg.MaxStorageBytes-used || int64(len(matches))*2048 > s.cfg.MaxStorageBytes-used-size {
		return result, fmt.Errorf("%w: logical storage", ErrCapacity)
	}
	id, err := randomID()
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	var key any
	if in.DeduplicationKey != "" {
		key = in.DeduplicationKey
	}
	r, err := tx.ExecContext(op, `INSERT INTO events(id,producer_id,event_type,received_at,dedup_key,input,logical_bytes) VALUES(?,?,?,?,?,?,?)`, id, p.id, in.EventType, now.UnixNano(), key, string(body), size)
	if err != nil {
		return result, err
	}
	seq, err := r.LastInsertId()
	if err != nil {
		return result, err
	}
	for _, subID := range matches {
		if _, err = tx.ExecContext(op, `INSERT INTO deliveries(event_sequence,subscription_id,status) VALUES(?,?,'pending')`, seq, subID); err != nil {
			return result, err
		}
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	return PublishResult{Event: Event{ID: id, Sequence: seq, ProducerID: p.id, ReceivedAt: now, Input: in}, MatchedSubscriptions: len(matches)}, nil
}

func (f Filter) matches(producer, kind string) bool {
	contains := func(a []string, v string) bool {
		if len(a) == 0 {
			return true
		}
		for _, s := range a {
			if s == v {
				return true
			}
		}
		return false
	}
	return contains(f.ProducerIDs, producer) && contains(f.EventTypes, kind)
}

func rollback(tx *sql.Tx, err *error) {
	if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
		*err = errors.Join(*err, e)
	}
}
