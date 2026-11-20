package brain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"ojj/internal/cancelerr"
	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
)

// Run exclusively drains pending inputs in receipt order, waiting when empty.
// Agent failures are recorded and skipped; storage failures stop Run. Neither
// failed nor interrupted work is retried. A parent cancellation interrupts work.
func (r *Region) Run(parent context.Context) (err error) {
	started := time.Now()
	defer func() {
		minipi.LogOperation(parent, "brain", "run_end", started, err, slog.LevelInfo)
	}()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if r.running {
		r.mu.Unlock()
		return ErrRunning
	}
	if parent == nil {
		r.mu.Unlock()
		return ErrInvalid
	}
	r.running = true
	r.wg.Add(1)
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.running = false; r.mu.Unlock(); r.wg.Done() }()
	minipi.LogOperation(parent, "brain", "run_start", started, nil, slog.LevelInfo)
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		in, err := r.claim(ctx)
		if errors.Is(err, ErrNotFound) {
			timer := time.NewTimer(r.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if err != nil {
			return err
		}
		if err = r.process(ctx, in); err != nil {
			return err
		}
	}
}

func (r *Region) claim(parent context.Context) (in Input, err error) {
	started := time.Now()
	defer func() {
		if !cancelerr.Only(err, ErrNotFound) {
			minipi.LogOperation(parent, "brain", "claim", started, err, slog.LevelDebug,
				slog.String("input_id", fmt.Sprintf("%032x", in.Sequence)))
		}
	}()
	ctx, cancel := context.WithTimeout(parent, r.cfg.OperationTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return in, err
	}
	defer rollback(tx, &err)
	in, err = scanInput(tx.QueryRowContext(ctx, "SELECT "+inputColumns+" FROM inputs WHERE status='pending' ORDER BY sequence LIMIT 1"))
	if err != nil {
		return in, err
	}
	_, err = tx.ExecContext(ctx, "UPDATE inputs SET status='processing',started_at=? WHERE id=?", time.Now().UnixNano(), in.ID)
	if err != nil {
		return in, err
	}
	return in, commit(ctx, tx)
}

func (r *Region) history(parent context.Context, in Input) (minipi.Context, error) {
	ctx, cancel := context.WithTimeout(parent, r.cfg.OperationTimeout)
	defer cancel()
	history := minipi.Context{SystemPrompt: r.cfg.SystemPrompt}
	remaining := r.cfg.MaxContextBytes - len(r.cfg.SystemPrompt)
	if remaining < 0 {
		return history, ErrContextCapacity
	}
	// Select newest message IDs first, then drop the oldest task if its initial
	// user message (turn 0) is outside the window. Select complete tasks before
	// filtering historical tools; never load unbounded bodies to discard them.
	rows, err := r.db.QueryContext(ctx, `
WITH recent AS (
 SELECT m.sequence, m.input_id, m.turn, i.sequence AS input_sequence, i.qq_delivery
 FROM messages m JOIN inputs i ON i.id=m.input_id
 WHERE (i.status='succeeded' AND i.sequence<?) OR i.id=?
 ORDER BY i.sequence DESC, m.sequence DESC LIMIT ?
)
SELECT m.input_id, m.message, r.qq_delivery FROM recent r JOIN messages m ON m.sequence=r.sequence
WHERE r.input_id IN (SELECT input_id FROM recent WHERE turn=0)
ORDER BY r.input_sequence, r.sequence`, in.Sequence, in.ID, r.cfg.MaxHistoryMessages)
	if err != nil {
		return history, err
	}
	defer rows.Close()
	for rows.Next() {
		var inputID string
		var delivery replydelivery.State
		var raw []byte
		if err = rows.Scan(&inputID, &raw, &delivery); err != nil {
			return history, err
		}
		var m Message
		if err = json.Unmarshal(raw, &m); err != nil {
			return history, err
		}
		message := decodeMessage(m)
		if inputID != in.ID {
			// Filter only the model's copy; current tool rounds and audit records
			// retain their complete calls and results, including tool errors.
			if message.Role == minipi.RoleTool ||
				(delivery == replydelivery.NotSent && message.Role == minipi.RoleAssistant) {
				continue
			}
			if len(message.ToolCalls) > 0 {
				message.ToolCalls = nil
				if message.Content == "" && len(message.Images) == 0 {
					continue
				}
			}
		}
		if inputID != in.ID && len(message.Images) > 0 {
			// Filter only the model's copy. Persisted snapshots remain intact for
			// recovery and idempotency; current-window tool rounds keep images.
			message.Images = nil
			if message.Content == "" {
				message.Content = "[历史图片未加载]"
			}
		}
		raw, err = json.Marshal(encodeMessage(message))
		if err != nil {
			return history, err
		}
		if len(raw) > remaining {
			return history, ErrContextCapacity
		}
		remaining -= len(raw)
		history.Messages = append(history.Messages, message)
	}
	if err := rows.Err(); err != nil {
		return history, err
	}
	if len(history.Messages) == 0 {
		// The current input must never disappear if its own records exceed the limit.
		return history, ErrContextCapacity
	}
	return history, nil
}

// inputContext treats the restorer as external code, just like models and tools.
// Never include its panic payload or the real input ID in diagnostics.
func (r *Region) inputContext(ctx context.Context, id string) (out context.Context, err error) {
	defer func() {
		if recover() != nil {
			out, err = nil, ErrPanic
		}
		if err != nil {
			err = fmt.Errorf("brain: restore input context: %w", err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.cfg.InputContext == nil {
		return ctx, nil
	}
	out, err = r.cfg.InputContext(ctx, id)
	if err == nil && out == nil {
		err = fmt.Errorf("nil context: %w", ErrInvalid)
	}
	return out, err
}

func (r *Region) process(parent context.Context, in Input) error {
	started := time.Now()
	// The receipt sequence is generated by this store, unlike the caller's ID.
	// Correlation is local to this region and never exposes an external identifier.
	parent = minipi.WithInputID(parent, fmt.Sprintf("%032x", in.Sequence))
	minipi.LogOperation(parent, "brain", "process_start", started, nil, slog.LevelDebug)
	ctx, agentErr := r.inputContext(parent, in.ID)
	// Recovery failures are storage failures, never recoverable agent failures.
	storageErr := agentErr
	var cancel context.CancelFunc
	if agentErr == nil {
		// A callback may return a detached context. Keep both cancellation chains
		// and the original deadline, and restore the store-local log correlation.
		if deadline, ok := parent.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline)
		} else {
			ctx, cancel = context.WithCancel(ctx)
		}
		stop := context.AfterFunc(parent, cancel)
		defer func() { stop(); cancel() }()
		if parent.Err() != nil {
			cancel()
		}
		ctx = minipi.WithInputID(ctx, fmt.Sprintf("%032x", in.Sequence))
	}
	skipped := false
	if agentErr == nil {
		// Cancellation wins even when no model/history work will run.
		agentErr = ctx.Err()
		skipped, _ = ctx.Value(modelSkippedKey{}).(bool)
		if skipped && agentErr == nil {
			minipi.LogOperation(parent, "brain", "model_skipped", started, nil, slog.LevelInfo)
		}
	}
	var history minipi.Context
	if agentErr == nil && !skipped {
		historyStarted := time.Now()
		history, agentErr = r.history(ctx, in)
		minipi.LogOperation(parent, "brain", "history", historyStarted, agentErr, slog.LevelDebug,
			slog.Int("message_count", len(history.Messages)))
		if agentErr != nil && !errors.Is(agentErr, ErrContextCapacity) && !(parent.Err() != nil && agentErr == parent.Err()) {
			storageErr = agentErr
		}
	}
	var result minipi.Result
	if agentErr == nil && !skipped {
		cfg := r.guardedConfig()
		agent, err := minipi.New(cfg)
		if err != nil {
			agentErr = err
		} else {
			result, agentErr = agent.Continue(ctx, history, func(e minipi.Event) {
				if e.Type != minipi.EventMessageEnd || storageErr != nil {
					return
				}
				if err := r.appendEvent(parent, in.ID, e); err != nil {
					storageErr = err
					cancel()
				}
			})
		}
	}
	if result.StopReason == minipi.StopLength {
		agentErr = errors.Join(agentErr, minipi.ErrInvalidResponse)
	}
	// A skipped input has no agent to propagate callback cancellation. Recheck
	// after the skip path too, before persisting a successful terminal state.
	if skipped && ctx.Err() != nil {
		agentErr = ctx.Err()
	}
	status := StatusSucceeded
	if agentErr != nil || storageErr != nil {
		status = StatusFailed
	}
	if parent.Err() != nil || (skipped && ctx.Err() != nil) {
		status = StatusInterrupted
	}
	finalErr := errors.Join(agentErr, storageErr, parent.Err())
	if finalErr != nil && result.StopReason == "" {
		result.StopReason = minipi.StopError
	}
	if status == StatusInterrupted {
		result.StopReason = minipi.StopCanceled
	}
	finishErr := r.finish(parent, in.ID, status, result, finalErr, replydelivery.Outcome(ctx))
	minipi.LogOperation(parent, "brain", "process_end", started, errors.Join(finalErr, finishErr), slog.LevelInfo,
		slog.Bool("persisted", finishErr == nil), slog.String("status", string(status)),
		slog.Int("turns", result.Turns), slog.Int64("input_tokens", result.Usage.InputTokens),
		slog.Int64("output_tokens", result.Usage.OutputTokens))
	return errors.Join(storageErr, finishErr)
}

func (r *Region) finish(logCtx context.Context, id string, status Status, result minipi.Result, cause error, delivery replydelivery.State) (err error) {
	started := time.Now()
	defer func() {
		minipi.LogOperation(logCtx, "brain", "finish", started, err, slog.LevelDebug,
			slog.Bool("persisted", err == nil))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OperationTimeout)
	defer cancel()
	res, err := r.db.ExecContext(ctx, `UPDATE inputs SET status=?,completed_at=?,last_error=?,turns=?,input_tokens=?,output_tokens=?,stop_reason=?,qq_delivery=? WHERE id=? AND status='processing'`, status, time.Now().UnixNano(), errorText(cause), result.Turns, result.Usage.InputTokens, result.Usage.OutputTokens, result.StopReason, delivery, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		err = ErrIdentity
	}
	return err
}

type guardedModel struct {
	model    minipi.Model
	maxBytes int
}

func (m guardedModel) Generate(ctx context.Context, req minipi.Request, update func(minipi.Message)) (response minipi.Response, err error) {
	// Never let minipi promote a streaming snapshot into final evidence on error.
	defer func() {
		if recover() != nil {
			err = ErrPanic
		}
		if err != nil && response.Message.Role == "" {
			response.Message.Role = minipi.RoleAssistant
		}
	}()
	remaining := m.maxBytes - len(req.SystemPrompt)
	if remaining < 0 {
		return response, ErrContextCapacity
	}
	for _, message := range req.Messages {
		raw, e := json.Marshal(encodeMessage(message))
		if e != nil {
			return response, e
		}
		if len(raw) > remaining {
			return response, ErrContextCapacity
		}
		remaining -= len(raw)
	}
	response, err = m.model.Generate(ctx, req, update)
	if response.StopReason == minipi.StopLength {
		err = errors.Join(err, minipi.ErrInvalidResponse)
	}
	return response, err
}

// Panic recovery is inside external adapters, so minipi still emits final
// message/tool-result events and statistics. Tool panics terminate this task,
// not merely become a recoverable result that could repeat a side effect.
func (r *Region) guardedConfig() minipi.Config {
	cfg := r.cfg.Agent
	cfg.Model = guardedModel{model: cfg.Model, maxBytes: r.cfg.MaxContextBytes}
	cfg.Tools = append([]minipi.Tool(nil), cfg.Tools...)
	for i := range cfg.Tools {
		original := cfg.Tools[i]
		cfg.Tools[i].Validate = func(raw json.RawMessage) (err error) {
			defer func() {
				if recover() != nil {
					err = errors.Join(ErrPanic, context.Canceled)
				}
			}()
			return original.Validate(raw)
		}
		cfg.Tools[i].Execute = func(ctx context.Context, call minipi.ToolCall, update func(minipi.ToolResult)) (out minipi.ToolResult, err error) {
			defer func() {
				if recover() != nil {
					out = minipi.ToolResult{IsError: true, Terminate: true}
					err = ErrPanic
				}
			}()
			return original.Execute(ctx, call, update)
		}

	}
	return cfg
}
