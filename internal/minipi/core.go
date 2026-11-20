package minipi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"ojj/internal/cancelerr"
)

// logInputKey carries only an application-generated input identifier.
type logInputKey struct{}

// LogID checks the shape of store-generated message identifiers. Callers must
// establish provenance first: format validation alone cannot make external IDs safe.
func LogID(id string) string {
	if len(id) != 32 {
		return ""
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
	}
	return id
}

// WithInputID associates diagnostics across the model and tool boundaries.
func WithInputID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, logInputKey{}, LogID(id))
}

// LogOperation emits only fixed labels and explicitly selected safe attributes.
// Callers must never pass errors, configuration, content or external identifiers
// as attributes. The error is inspected, never formatted or sent to the handler.
func LogOperation(ctx context.Context, component, stage string, started time.Time, err error, level slog.Level, attrs ...slog.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	failure := "none"
	switch {
	case err == nil:
	case cancelerr.Only(err, context.Canceled):
		failure, level = "canceled", slog.LevelDebug
	case cancelerr.Only(err, context.DeadlineExceeded):
		failure = "timeout"
		if level != slog.LevelWarn {
			level = slog.LevelError
		}
	default:
		failure = "failure"
		if level != slog.LevelWarn {
			level = slog.LevelError
		}
	}
	fields := []slog.Attr{slog.String("component", component), slog.String("stage", stage), slog.String("failure", failure), slog.Duration("duration", time.Since(started))}
	if id, _ := ctx.Value(logInputKey{}).(string); id != "" {
		fields = append(fields, slog.String("input_id", id))
	}
	fields = append(fields, attrs...)
	slog.Default().LogAttrs(ctx, level, "operation", fields...)
}

// Run appends user prompts to a detached history. At least one prompt is
// required. Input slices must not be mutated concurrently while being copied.
func (a *Agent) Run(ctx context.Context, history Context, prompts []Message, sink func(Event)) (Result, error) {
	return a.run(ctx, history, prompts, false, sink)
}

// Continue resumes a history ending in a user message or paired tool results.
// Empty histories and assistant tails are rejected rather than regenerated.
func (a *Agent) Continue(ctx context.Context, history Context, sink func(Event)) (Result, error) {
	return a.run(ctx, history, nil, true, sink)
}

type execution struct {
	agent  *Agent
	ctx    context.Context
	system string
	result Result
	sink   func(Event)
	emitMu sync.Mutex
}

func (a *Agent) run(parent context.Context, history Context, prompts []Message, resume bool, sink func(Event)) (result Result, err error) {
	started := time.Now()
	defer func() {
		LogOperation(parent, "minipi", "agent_end", started, err, slog.LevelInfo,
			slog.Int("turns", result.Turns), slog.Int64("input_tokens", result.Usage.InputTokens), slog.Int64("output_tokens", result.Usage.OutputTokens))
	}()
	if !a.running.CompareAndSwap(false, true) {
		return Result{Messages: append(cloneMessages(history.Messages), cloneMessages(prompts)...), StopReason: StopError}, ErrRunning
	}
	defer a.running.Store(false)
	if parent == nil {
		return Result{Messages: append(cloneMessages(history.Messages), cloneMessages(prompts)...), StopReason: StopError}, fmt.Errorf("%w: nil context", ErrInvalidContext)
	}
	LogOperation(parent, "minipi", "agent_start", started, nil, slog.LevelInfo)
	ctx, cancel := context.WithTimeout(parent, a.config.Timeout)
	defer cancel()
	run := &execution{agent: a, ctx: ctx, system: history.SystemPrompt, sink: sink}
	prompts = cloneMessages(prompts)
	run.result.Messages = append(cloneMessages(history.Messages), prompts...)
	run.emit(Event{Type: EventAgentStart})
	defer func() {
		err = withCancellation(ctx, err)
		run.result.StopReason = reasonFor(err, run.result.StopReason)
		run.emit(Event{Type: EventAgentEnd, Err: err, StopReason: run.result.StopReason, Usage: run.result.Usage, Result: run.result})
		// A synchronous observer can cancel during agent_end as well.
		err = withCancellation(ctx, err)
		run.result.StopReason = reasonFor(err, run.result.StopReason)
		result = cloneResult(run.result)
	}()
	if !resume {
		if len(prompts) == 0 {
			return Result{}, fmt.Errorf("%w: Run requires user prompts", ErrInvalidContext)
		}
		for _, prompt := range prompts {
			if prompt.Role != RoleUser {
				return Result{}, fmt.Errorf("%w: prompts must have user role", ErrInvalidContext)
			}
		}
	}
	if err := validateHistory(run.result.Messages); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidContext, err)
	}
	if len(run.result.Messages) == 0 || run.result.Messages[len(run.result.Messages)-1].Role == RoleAssistant {
		return Result{}, fmt.Errorf("%w: expected user or tool-result tail", ErrInvalidContext)
	}
	if !resume {
		for _, prompt := range prompts {
			run.emit(Event{Type: EventMessageStart, Message: prompt})
			run.emit(Event{Type: EventMessageEnd, Message: prompt})
		}
	}
	for {
		if err := run.check(); err != nil {
			return Result{}, err
		}
		if run.result.Turns >= a.config.MaxTurns {
			return Result{}, ErrMaxTurns
		}
		done, err := run.turn()
		if err != nil || done {
			return Result{}, err
		}
	}
}

func (r *execution) emit(event Event) {
	if r.sink == nil {
		return
	}
	event.Turn = r.result.Turns
	event.Message = cloneMessage(event.Message)
	event.ToolCall = cloneCall(event.ToolCall)
	event.Result = cloneResult(event.Result)
	r.emitMu.Lock()
	defer r.emitMu.Unlock()
	r.sink(event)
}

func withCancellation(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil && !errors.Is(err, canceled) {
		return errors.Join(canceled, err)
	}
	return err
}

func reasonFor(err error, fallback StopReason) StopReason {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return StopCanceled
	case errors.Is(err, ErrMaxTokens):
		return StopMaxTokens
	case errors.Is(err, ErrMaxTurns):
		return StopMaxTurns
	case err != nil:
		return StopError
	default:
		return fallback
	}
}

func (r *execution) check() error {
	var err error
	if r.result.Usage.InputTokens+r.result.Usage.OutputTokens >= r.agent.config.MaxTokens {
		err = ErrMaxTokens
	}
	return withCancellation(r.ctx, err)
}

func (r *execution) turn() (done bool, err error) {
	started := time.Now()
	defer func() {
		LogOperation(r.ctx, "minipi", "turn_end", started, withCancellation(r.ctx, err), slog.LevelDebug, slog.Int("turn", r.result.Turns))
	}()
	r.result.Turns++
	LogOperation(r.ctx, "minipi", "turn_start", started, nil, slog.LevelDebug, slog.Int("turn", r.result.Turns))
	r.emit(Event{Type: EventTurnStart})
	defer func() {
		err = withCancellation(r.ctx, err)
		r.result.StopReason = reasonFor(err, r.result.StopReason)
		r.emit(Event{Type: EventTurnEnd, Err: err, StopReason: r.result.StopReason, Usage: r.result.Usage})
	}()
	if err := r.check(); err != nil {
		return false, err
	}
	response, err := r.generate()
	r.result.StopReason = response.StopReason
	calls := response.Message.ToolCalls
	if err != nil {
		r.skip(calls, err.Error())
		return false, err
	}
	if err := r.check(); err != nil {
		r.skip(calls, err.Error())
		return false, err
	}
	if len(calls) == 0 {
		return true, nil
	}
	if response.StopReason == StopLength {
		r.skip(calls, "model response truncated; tool not executed")
		return false, nil
	}
	return r.executeTools(calls)
}

func (r *execution) request() Request {
	request := Request{
		SystemPrompt: r.system,
		Messages:     cloneMessages(r.result.Messages),
		MaxTokens:    r.agent.config.MaxTokens - r.result.Usage.InputTokens - r.result.Usage.OutputTokens,
	}
	for _, tool := range r.agent.config.Tools {
		request.Tools = append(request.Tools, cloneDefinition(tool.Definition))
	}
	return request
}

func (r *execution) generate() (out Response, finalErr error) {
	started := time.Now()
	defer func() {
		LogOperation(r.ctx, "minipi", "model_end", started, finalErr, slog.LevelDebug, slog.Int("turn", r.result.Turns), slog.Int("tool_calls", len(out.Message.ToolCalls)))
	}()
	LogOperation(r.ctx, "minipi", "model_start", started, nil, slog.LevelDebug)
	latest := Message{Role: RoleAssistant}
	r.emit(Event{Type: EventMessageStart, Message: latest})
	var mu sync.Mutex
	var closed atomic.Bool
	var updateErr error
	updated := false
	update := func(message Message) {
		if closed.Load() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if closed.Load() {
			return
		}
		if message.Role != RoleAssistant || messageShape(message) != nil {
			updateErr = fmt.Errorf("%w: invalid assistant update", ErrInvalidResponse)
			return
		}
		latest = cloneMessage(message)
		updated = true
		r.emit(Event{Type: EventMessageUpdate, Message: latest})
	}
	var response Response
	err := r.check() // message_start observers may have canceled the run.
	if err == nil {
		response, err = r.agent.config.Model.Generate(r.ctx, r.request(), update)
	}
	closed.Store(true)
	mu.Lock() // Wait for any already admitted update without orphan goroutines.
	if err != nil && response.Message.Role == "" && response.Message.Content == "" && len(response.Message.ToolCalls) == 0 {
		response.Message = latest
	}
	response.Message = cloneMessage(response.Message)
	mu.Unlock()
	usageErr := addUsage(&r.result.Usage, response.Usage)
	var responseErr error
	if err == nil {
		responseErr = validateResponse(response, r.result.Messages)
	}
	err = errors.Join(err, updateErr, usageErr, responseErr, r.check())
	// Even failed final responses are kept; partial snapshots are used only
	// when the adapter failed without returning a final message.
	if response.Message.Role != "" || response.Message.Content != "" || len(response.Message.ToolCalls) > 0 || updated {
		r.result.Messages = append(r.result.Messages, cloneMessage(response.Message))
	}
	r.emit(Event{Type: EventMessageEnd, Message: response.Message, Usage: response.Usage, StopReason: reasonFor(err, response.StopReason), Err: err})
	return response, withCancellation(r.ctx, err)
}
