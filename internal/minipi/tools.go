package minipi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (r *execution) executeTools(calls []ToolCall) (bool, error) {
	for i, call := range calls {
		if err := r.check(); err != nil {
			r.skip(calls[i:], err.Error())
			return false, err
		}
		result, toolErr := r.executeTool(call)
		fatal := withCancellation(r.ctx, nil)
		if errors.Is(toolErr, context.Canceled) || errors.Is(toolErr, context.DeadlineExceeded) {
			fatal = errors.Join(fatal, toolErr)
		} else if fatal != nil {
			fatal = errors.Join(fatal, toolErr)
		}
		if fatal != nil {
			r.skip(calls[i+1:], fatal.Error())
			return false, fatal
		}
		if result.Terminate {
			r.result.StopReason = StopTerminate
			r.skip(calls[i+1:], "agent terminated by special tool")
			// No next model turn can consume this failure; keep it observable.
			return true, toolErr
		}
	}
	return false, nil
}

func (r *execution) executeTool(call ToolCall) (result ToolResult, err error) {
	started := time.Now()
	stage := "tool_lookup"
	defer func() {
		logErr := err
		if logErr == nil && result.IsError {
			logErr = errors.New("tool failure")
		}
		LogOperation(r.ctx, "minipi", stage, started, logErr, slog.LevelDebug,
			slog.Int("turn", r.result.Turns), slog.Bool("terminate", result.Terminate))
	}()
	LogOperation(r.ctx, "minipi", "tool_start", started, nil, slog.LevelDebug, slog.Int("turn", r.result.Turns))
	r.emit(Event{Type: EventToolExecutionStart, ToolCall: call})
	defer func() {
		err = withCancellation(r.ctx, err)
		if err != nil {
			result.IsError = true
			if result.Content != "" {
				result.Content += "\n"
			}
			result.Content += err.Error()
		}
		r.emit(Event{Type: EventToolExecutionEnd, ToolCall: call, ToolResult: result, Err: err, StopReason: reasonFor(err, toolReason(result))})
		r.appendToolResult(call, result)
		err = withCancellation(r.ctx, err)
	}()
	if err := r.check(); err != nil {
		return ToolResult{}, err
	}
	tool, exists := r.agent.tools[call.Name]
	if !exists {
		return ToolResult{}, fmt.Errorf("unknown tool %q", call.Name)
	}
	stage = "tool_validate"
	if !jsonObject(call.Arguments) {
		return ToolResult{}, fmt.Errorf("tool %q arguments must be a complete JSON object", call.Name)
	}
	if err := tool.Validate(cloneCall(call).Arguments); err != nil {
		return ToolResult{}, fmt.Errorf("tool %q validation: %w", call.Name, err)
	}
	// Validators may be slow or cancel ctx. Never execute after cancellation.
	if err := r.check(); err != nil {
		return ToolResult{}, err
	}
	var mu sync.Mutex
	var closed atomic.Bool
	update := func(snapshot ToolResult) {
		if closed.Load() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if !closed.Load() {
			r.emit(Event{Type: EventToolExecutionUpdate, ToolCall: call, ToolResult: snapshot})
		}
	}
	stage = "tool_execute"
	result, err = tool.Execute(r.ctx, cloneCall(call), update)
	closed.Store(true)
	mu.Lock()
	mu.Unlock()
	return result, err
}

func toolReason(result ToolResult) StopReason {
	if result.IsError {
		return StopError
	}
	if result.Terminate {
		return StopTerminate
	}
	return ""
}

func (r *execution) appendToolResult(call ToolCall, result ToolResult) {
	message := Message{Role: RoleTool, ToolCallID: call.ID, Content: result.Content, IsError: result.IsError}
	r.emit(Event{Type: EventMessageStart, Message: message})
	r.result.Messages = append(r.result.Messages, message)
	r.emit(Event{Type: EventMessageEnd, Message: message, StopReason: toolReason(result)})
}

// skip emits error results, not execution events: no tool was attempted. Invalid
// empty/duplicate IDs cannot be repaired without rewriting the model response;
// retain that response but synthesize at most one result per nonempty ID.
func (r *execution) skip(calls []ToolCall, reason string) {
	started := time.Now()
	count := 0
	defer func() {
		if count > 0 {
			LogOperation(r.ctx, "minipi", "tools_skipped", started, nil, slog.LevelDebug, slog.Int("count", count))
		}
	}()
	seen := make(map[string]bool)
	for _, call := range calls {
		if strings.TrimSpace(call.ID) == "" || seen[call.ID] {
			continue
		}
		seen[call.ID] = true
		count++
		r.appendToolResult(call, ToolResult{Content: "skipped: " + reason, IsError: true})
	}
}
