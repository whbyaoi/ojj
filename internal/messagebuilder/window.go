package messagebuilder

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"

	"time"
	"unicode/utf8"

	"ojj/internal/event"
	"ojj/internal/minipi"
)

type window struct {
	opened, last, deadline, maxAt, quiet int64
	count, size, maxEvents, maxBytes     int64
}

func loadWindow(ctx context.Context, tx *sql.Tx) (*window, error) {
	w := new(window)
	err := tx.QueryRowContext(ctx, `SELECT opened_at,last_at,deadline,max_at,quiet_ns,event_count,byte_count,max_events,max_bytes FROM active_window WHERE id=1`).Scan(
		&w.opened, &w.last, &w.deadline, &w.maxAt, &w.quiet, &w.count, &w.size, &w.maxEvents, &w.maxBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return w, err
}

// Handle 可直接作为 event.Handler；只有完整快照及去重摘要提交后才返回 nil。
// 摘要覆盖完整 Event，时间规范为 UTC；OccurredAt/上游 ReceivedAt 不参与窗口计时。
// 调用期间调用方不得并发修改 Event.Source。无效 UTF-8 拒收，避免 JSON 替换丢数据。
func (b *Builder) Handle(ctx context.Context, e event.Event) error {
	return b.handle(ctx, e, time.Now)
}

func (b *Builder) handle(ctx context.Context, e event.Event, clock func() time.Time) (err error) {
	started := time.Now()
	var sealed []sealLog
	duplicate := false
	defer func() {
		minipi.LogOperation(ctx, "messagebuilder", "receive", started, err, slog.LevelDebug, slog.Bool("duplicate", duplicate))
		if err == nil {
			logSealed(ctx, started, sealed)
		}
	}()
	op, done, err := b.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if e.ID == "" || !validStrings(reflect.ValueOf(e)) {
		return ErrInvalid
	}
	e.OccurredAt = e.OccurredAt.UTC()
	e.ReceivedAt = e.ReceivedAt.UTC()
	raw, err := json.Marshal(e)
	if err != nil {
		return ErrInvalid
	}
	digest := sha256.Sum256(raw)
	tx, err := b.db.BeginTx(op, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	var stored []byte
	err = tx.QueryRowContext(op, "SELECT digest FROM inbox WHERE id=?", e.ID).Scan(&stored)
	if err == nil {
		if !bytes.Equal(stored, digest[:]) {
			return ErrConflict
		}
		duplicate = true
		return commit(op, tx) // 重复不修改 last_at/deadline，也不受新的容量配置影响。
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	cost, err := eventCost(e, len(raw))
	if err != nil {
		return err
	}
	if cost > int64(b.cfg.MaxEventBytes) || cost > int64(b.cfg.MaxWindowBytes) {
		return ErrCapacity
	}
	var total int64
	if err = tx.QueryRowContext(op, "SELECT event_count FROM builder_meta WHERE id=1").Scan(&total); err != nil {
		return err
	}
	if total >= b.cfg.MaxStoredEvents {
		return ErrCapacity
	}
	w, err := loadWindow(op, tx)
	if err != nil {
		return err
	}
	now := clock().UTC().UnixNano() // 等待事务后才采集本模块接收时间。
	if w != nil && now < w.last {
		now = w.last
	} // 墙钟回退不倒置窗口内顺序。
	if w != nil && now >= w.deadline {
		if err = sealWindow(op, tx, &sealed, w, w.deadline, now, w.reason()); err != nil {
			return err
		}
		w = nil
	}
	if w != nil && (w.count >= w.maxEvents || cost > w.maxBytes-w.size) {
		if err = sealWindow(op, tx, &sealed, w, now, now, ReasonCapacity); err != nil {
			return err
		}
		w = nil
	}
	if w == nil {
		w = &window{opened: now, last: now, quiet: int64(b.cfg.QuietPeriod), maxAt: addNanos(now, int64(b.cfg.MaxWindow)), maxEvents: int64(b.cfg.MaxWindowEvents), maxBytes: int64(b.cfg.MaxWindowBytes)}
	}
	w.last = now
	w.deadline = min(addNanos(now, w.quiet), w.maxAt)
	w.count++
	w.size += cost
	_, err = tx.ExecContext(op, `INSERT INTO inbox(id,digest,snapshot,received_at) VALUES(?,?,?,?)`, e.ID, digest[:], raw, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(op, `INSERT OR REPLACE INTO active_window VALUES(1,?,?,?,?,?,?,?,?,?)`, w.opened, w.last, w.deadline, w.maxAt, w.quiet, w.count, w.size, w.maxEvents, w.maxBytes)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(op, "UPDATE builder_meta SET event_count=event_count+1 WHERE id=1")
	if err != nil {
		return err
	}
	// 到达容量即提前收束；最后一个事件仍完整保留，不等下一个事件触发。
	if w.count >= w.maxEvents || w.size >= w.maxBytes {
		if err = sealWindow(op, tx, &sealed, w, now, now, ReasonCapacity); err != nil {
			return err
		}
	}
	if err = commit(op, tx); err != nil {
		return err
	}
	// No transaction is held during downloads. Keep operation/Close cancellation
	// and ignore cache failure: the event is already durably accepted.
	b.prefetchImages(op, e)
	return nil
}

// 只检查公开字段，time.Time 自身由 JSON 编码验证。
func validStrings(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Pointer:
		return v.IsNil() || validStrings(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() && !validStrings(v.Field(i)) {
				return false
			}
		}
	}
	return true
}

func (w *window) reason() CloseReason {
	if w.deadline == w.maxAt {
		return ReasonMaxWindow
	}
	return ReasonQuiet
}

func (b *Builder) settle(ctx context.Context, clock func() time.Time) (err error) {
	started := time.Now()
	var sealed []sealLog
	defer func() {
		if err != nil {
			minipi.LogOperation(ctx, "messagebuilder", "settle", started, err, slog.LevelDebug)
		} else {
			logSealed(ctx, started, sealed)
		}
	}()
	op, done, err := b.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	tx, err := b.db.BeginTx(op, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	w, err := loadWindow(op, tx)
	if err != nil {
		return err
	}
	now := clock().UnixNano()
	if w != nil && now >= w.deadline {
		if err = sealWindow(op, tx, &sealed, w, w.deadline, now, w.reason()); err != nil {
			return err
		}
	} else if w != nil && (w.count >= w.maxEvents || w.size >= w.maxBytes) {
		// Migration may add image cost to an already accepted active window.
		now = max(now, w.last)
		if err = sealWindow(op, tx, &sealed, w, now, now, ReasonCapacity); err != nil {
			return err
		}
	}
	return commit(op, tx)
}

// sealLog contains only data generated by this store; emit after the outer commit.
type sealLog struct {
	id     string
	count  int64
	reason CloseReason
}

func logSealed(ctx context.Context, started time.Time, sealed []sealLog) {
	for _, s := range sealed {
		minipi.LogOperation(ctx, "messagebuilder", "window_sealed", started, nil, slog.LevelInfo,
			slog.String("message_id", s.id), slog.Int64("event_count", s.count), slog.String("reason", string(s.reason)))
	}
}

// sealWindow 必须在同一事务内完成文本、事件绑定及 active 删除，失败全部回滚。
func sealWindow(ctx context.Context, tx *sql.Tx, sealed *[]sealLog, w *window, closed, created int64, reason CloseReason) error {
	rows, err := tx.QueryContext(ctx, "SELECT snapshot FROM inbox WHERE message_sequence IS NULL ORDER BY sequence")
	if err != nil {
		return err
	}
	var events []event.Event
	var ids []string
	for rows.Next() {
		var raw []byte
		var e event.Event
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &e)
		}
		if err != nil {
			return errors.Join(err, rows.Close())
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if int64(len(events)) != w.count || len(events) == 0 {
		return ErrInvalid
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return err
	}
	encoded, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	text := formatEvents(events, fromNano(w.opened), fromNano(closed))
	images, err := json.Marshal(eventImages(events))
	if err != nil {
		return ErrInvalid
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO messages(id,text,event_ids,opened_at,closed_at,created_at,reason,status,images) VALUES(?,?,?,?,?,?,?,'pending',?)`,
		hex.EncodeToString(id[:]), text, string(encoded), w.opened, closed, created, reason, string(images))
	if err != nil {
		return err
	}
	sequence, err := result.LastInsertId()
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE inbox SET message_sequence=? WHERE message_sequence IS NULL", sequence); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM active_window WHERE id=1")
	if err == nil {
		*sealed = append(*sealed, sealLog{id: hex.EncodeToString(id[:]), count: w.count, reason: reason})
	}
	return err
}
