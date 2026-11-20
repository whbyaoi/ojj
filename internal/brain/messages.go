package brain

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
)

// Handle durably acknowledges ID+Text+Images; other builder fields are ignored.
// Duplicate acknowledgements precede current size/capacity checks (also on reopen).
func (r *Region) Handle(ctx context.Context, m messagebuilder.Message) (err error) {
	started := time.Now()
	duplicate := false
	defer func() {
		minipi.LogOperation(ctx, "brain", "receive", started, err, slog.LevelInfo,
			slog.Bool("duplicate", duplicate))
	}()
	op, done, err := r.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if strings.TrimSpace(m.ID) == "" || !utf8.ValidString(m.ID) {
		return ErrInvalid
	}
	tx, err := r.db.BeginTx(op, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	var text string
	err = tx.QueryRowContext(op, "SELECT text FROM inputs WHERE id=?", m.ID).Scan(&text)
	if err == nil {
		if text != m.Text {
			return ErrConflict
		}
		// Compare the durable initial message, not reconstructed attachments.
		// Legacy text-only records decode to no images; nil and empty are equal.
		var raw []byte
		if err = tx.QueryRowContext(op, "SELECT message FROM messages WHERE input_id=? AND turn=0 ORDER BY sequence LIMIT 1", m.ID).Scan(&raw); err != nil {
			return err
		}
		var stored Message
		if json.Unmarshal(raw, &stored) != nil {
			return ErrInvalid
		}
		if !slices.Equal(decodeMessage(stored).Images, m.Images) {
			return ErrConflict
		}
		duplicate = true
		return commit(op, tx)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !utf8.ValidString(m.Text) || len(m.ID) > r.cfg.MaxInputBytes || len(m.Text) > r.cfg.MaxInputBytes-len(m.ID) {
		return ErrInvalid
	}
	if minipi.ValidateImages(minipi.RoleUser, m.Images) != nil {
		return ErrInvalid
	}
	if len(m.Images) > 0 {
		encoded, marshalErr := json.Marshal(m.Images)
		if marshalErr != nil || len(encoded) > r.cfg.MaxInputBytes-len(m.ID)-len(m.Text) {
			return ErrInvalid
		}
	}
	raw, err := json.Marshal(encodeMessage(minipi.Message{Role: minipi.RoleUser, Content: m.Text, Images: m.Images}))
	if err != nil {
		return err
	}
	cost := int64(len(m.ID)) + int64(len(m.Text)) + int64(len(raw))
	if err = r.reserve(op, tx, cost); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(op, "INSERT INTO inputs(id,text,status,received_at) VALUES(?,?,'pending',?)", m.ID, m.Text, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(op, `INSERT INTO messages(input_id,message,turn,input_tokens,output_tokens,stop_reason,error,created_at) VALUES(?,?,0,0,0,'','',?)`, m.ID, raw, now)
	if err != nil {
		return err
	}
	return commit(op, tx)
}

func (r *Region) reserve(ctx context.Context, tx *sql.Tx, n int64) error {
	if n > r.cfg.MaxStoredBytes {
		return ErrCapacity
	}
	result, err := tx.ExecContext(ctx, "UPDATE brain_meta SET stored_bytes=stored_bytes+? WHERE id=1 AND stored_bytes<=?", n, r.cfg.MaxStoredBytes-n)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrCapacity
	}
	return nil
}

const inputColumns = `sequence,id,text,status,received_at,started_at,completed_at,last_error,turns,input_tokens,output_tokens,stop_reason`

func scanInput(row interface{ Scan(...any) error }) (Input, error) {
	var in Input
	var received, started, completed int64
	err := row.Scan(&in.Sequence, &in.ID, &in.Text, &in.Status, &received, &started, &completed, &in.LastError, &in.Turns, &in.Usage.InputTokens, &in.Usage.OutputTokens, &in.StopReason)
	in.ReceivedAt = fromNano(received)
	in.StartedAt = fromNano(started)
	in.CompletedAt = fromNano(completed)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return in, err
}

func (r *Region) Get(ctx context.Context, inputID string) (Input, error) {
	op, done, err := r.operation(ctx)
	if err != nil {
		return Input{}, err
	}
	defer done()
	return scanInput(r.db.QueryRowContext(op, "SELECT "+inputColumns+" FROM inputs WHERE id=?", inputID))
}

// ListMessages uses global, increasing log sequences; afterSequence is exclusive.
// Limit defaults to 100, maximum 1000. Unknown inputs return ErrNotFound.
func (r *Region) ListMessages(ctx context.Context, inputID string, afterSequence int64, limit int) ([]StoredMessage, error) {
	op, done, err := r.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if afterSequence < 0 || limit < 0 || limit > 1000 {
		return nil, ErrInvalid
	}
	if limit == 0 {
		limit = 100
	}
	var exists int
	err = r.db.QueryRowContext(op, "SELECT 1 FROM inputs WHERE id=?", inputID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(op, `SELECT sequence,message,turn,input_tokens,output_tokens,stop_reason,error,created_at FROM messages WHERE input_id=? AND sequence>? ORDER BY sequence LIMIT ?`, inputID, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StoredMessage{}
	for rows.Next() {
		var m StoredMessage
		var raw []byte
		var created int64
		m.InputID = inputID
		if err = rows.Scan(&m.Sequence, &raw, &m.Turn, &m.Usage.InputTokens, &m.Usage.OutputTokens, &m.StopReason, &m.Error, &created); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &m.Message); err != nil {
			return nil, err
		}
		m.CreatedAt = fromNano(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

// appendEvent is synchronous and independent of cancellation: already observed
// evidence gets a bounded persistence attempt even when Close/parent cancels.
func (r *Region) appendEvent(logCtx context.Context, id string, e minipi.Event) (err error) {
	started := time.Now()
	defer func() {
		minipi.LogOperation(logCtx, "brain", "evidence_persist", started, err, slog.LevelDebug,
			slog.Int("turn", e.Turn),
			slog.Bool("persisted", err == nil))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OperationTimeout)
	defer cancel()
	raw, err := json.Marshal(encodeMessage(e.Message))
	if err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	if err = r.reserve(ctx, tx, int64(len(raw))); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO messages(input_id,message,turn,input_tokens,output_tokens,stop_reason,error,created_at) VALUES(?,?,?,?,?,?,?,?)`, id, raw, e.Turn, e.Usage.InputTokens, e.Usage.OutputTokens, e.StopReason, errorText(e.Err), time.Now().UnixNano())
	if err != nil {
		return err
	}
	return commit(ctx, tx)
}

// Error metadata is bounded separately; original message blocks are not trimmed.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	// Opt-in diagnostic details are for persistence only, never logs/history.
	var diagnostic interface{ DatabaseDetail() string }
	if errors.As(err, &diagnostic) {
		if detail := diagnostic.DatabaseDetail(); detail != "" {
			s += "\nHTTP response detail: " + detail
		}
	}
	const limit = 4096
	const suffix = " [error metadata truncated]"
	if len(s) > limit {
		s = s[:limit-len(suffix)]
		return strings.ToValidUTF8(s, "") + suffix
	}
	return strings.ToValidUTF8(s, "")
}
