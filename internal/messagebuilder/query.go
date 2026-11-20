package messagebuilder

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"ojj/internal/event"
	"ojj/internal/minipi"
)

const messageColumns = `id,sequence,text,event_ids,opened_at,closed_at,created_at,reason,status,attempts,next_at,started_at,completed_at,last_error,images`

type scanner interface{ Scan(...any) error }

func scanMessage(row scanner) (Message, error) {
	var m Message
	var ids, images string
	var opened, closed, created, next, started, completed int64
	err := row.Scan(&m.ID, &m.Sequence, &m.Text, &ids, &opened, &closed, &created, &m.CloseReason, &m.Status, &m.Attempts, &next, &started, &completed, &m.LastError, &images)
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal([]byte(ids), &m.EventIDs); err != nil {
		return m, err
	}
	if json.Unmarshal([]byte(images), &m.Images) != nil || minipi.ValidateImages(minipi.RoleUser, m.Images) != nil {
		return m, ErrInvalid
	}
	m.OpenedAt, m.ClosedAt, m.CreatedAt = fromNano(opened), fromNano(closed), fromNano(created)
	m.NextAttemptAt, m.StartedAt, m.CompletedAt = fromNano(next), fromNano(started), fromNano(completed)
	return m, nil
}

// Get 按稳定消息 ID 读取完整正文与当前投递状态。
func (b *Builder) Get(ctx context.Context, id string) (Message, error) {
	op, done, err := b.operation(ctx)
	if err != nil {
		return Message{}, err
	}
	defer done()
	m, err := scanMessage(b.db.QueryRowContext(op, "SELECT "+messageColumns+" FROM messages WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return m, err
}

// Events 按成品消息 ID 读取永久事件快照，保持窗口内的接收顺序。
// 每次调用独立解码，不与调用者或其他查询共享可变数据。
func (b *Builder) Events(ctx context.Context, id string) (events []event.Event, err error) {
	op, done, err := b.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	rows, err := b.db.QueryContext(op, `SELECT i.snapshot FROM messages m
LEFT JOIN inbox i ON i.message_sequence=m.sequence
WHERE m.id=? ORDER BY i.sequence`, id)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var e event.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, ErrNotFound
	}
	return events, nil
}

// List 按 Sequence 游标分页，包含正文；未闭合窗口尚无 Message。
func (b *Builder) List(ctx context.Context, q MessageQuery) (messages []Message, err error) {
	op, done, err := b.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if q.Limit == 0 {
		q.Limit = 100
	}
	if q.Limit < 1 || q.Limit > 1000 || q.AfterSequence < 0 {
		return nil, ErrInvalid
	}
	switch q.Status {
	case "", StatusPending, StatusProcessing, StatusRetry, StatusSucceeded:
	default:
		return nil, ErrInvalid
	}
	rows, err := b.db.QueryContext(op, "SELECT "+messageColumns+" FROM messages WHERE sequence>? AND (?='' OR status=?) ORDER BY sequence LIMIT ?", q.AfterSequence, q.Status, q.Status, q.Limit)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		m, e := scanMessage(rows)
		if e != nil {
			return nil, e
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}
