package minipi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	. "ojj/internal/minipi"
)

// This wrapper must not hide a genuine failure behind an Is cancellation match.
type logCancellationMask struct{ error }

func (logCancellationMask) Is(target error) bool { return target == context.Canceled }
func (e logCancellationMask) Unwrap() error      { return e.error }

func TestSafeOperationLogs(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	secret := errors.New("private-error-marker")
	ctx := WithInputID(context.Background(), "0123456789abcdef0123456789abcdef")
	for _, tc := range []struct {
		err                error
		level              slog.Level
		failure, wantLevel string
	}{
		{nil, slog.LevelInfo, "none", "INFO"},
		{fmt.Errorf("private-wrapper: %w", context.Canceled), slog.LevelWarn, "canceled", "DEBUG"},
		{context.DeadlineExceeded, slog.LevelInfo, "timeout", "ERROR"},
		{errors.Join(context.Canceled, secret), slog.LevelInfo, "failure", "ERROR"},
		{logCancellationMask{secret}, slog.LevelInfo, "failure", "ERROR"},
		{errors.Join(context.Canceled, secret), slog.LevelWarn, "failure", "WARN"},
	} {
		buf.Reset()
		LogOperation(ctx, "minipi", "model_end", time.Now(), tc.err, tc.level)
		var record map[string]any
		if json.Unmarshal(buf.Bytes(), &record) != nil {
			t.Fatal("invalid log record")
		}
		if record["failure"] != tc.failure || record["level"] != tc.wantLevel {
			t.Fatal("incorrect failure classification")
		}
		if record["input_id"] != "0123456789abcdef0123456789abcdef" || record["duration"] == nil {
			t.Fatal("missing correlation or duration")
		}
		if strings.Contains(buf.String(), "private-") {
			t.Fatal("error text leaked")
		}
	}
	buf.Reset()
	LogOperation(WithInputID(context.Background(), "private-id"), "minipi", "model_end", time.Now(), nil, slog.LevelDebug)
	if strings.Contains(buf.String(), "input_id") {
		t.Fatal("invalid identifier logged")
	}
}

func TestAgentLogsOmitPayloads(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	tool := basicTool("private-tool")
	tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
		return ToolResult{Content: "private-result"}, errors.New("private-error")
	}
	calls := 0
	a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
		calls++
		if calls == 1 {
			return toolResponse(ToolCall{ID: "private-call", Name: "private-tool", Arguments: json.RawMessage(`{"value":"private-argument"}`)}), nil
		}
		return answer("private-answer"), nil
	})})
	_, err := a.Run(context.Background(), Context{SystemPrompt: "private-system"}, []Message{{Role: RoleUser, Content: "private-prompt"}}, nil)
	if err != nil {
		t.Fatal("ordinary tool error changed agent outcome")
	}
	if strings.Contains(buf.String(), "private-") {
		t.Fatal("sensitive agent data logged")
	}
	if !strings.Contains(buf.String(), `"stage":"tool_execute"`) || !strings.Contains(buf.String(), `"stage":"agent_end"`) {
		t.Fatal("missing agent diagnostics")
	}
}

type modelFunc func(context.Context, Request, func(Message)) (Response, error)

func (f modelFunc) Generate(ctx context.Context, req Request, update func(Message)) (Response, error) {
	return f(ctx, req, update)
}

func newAgent(t *testing.T, cfg Config) *Agent {
	t.Helper()
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func equal(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func outcome(t *testing.T, r Result, err, wantErr error, reason StopReason, turns int) {
	t.Helper()
	if (wantErr == nil && err != nil) || (wantErr != nil && !errors.Is(err, wantErr)) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	equal(t, r.StopReason, reason)
	equal(t, r.Turns, turns)
}

func prompt() []Message { return []Message{{Role: RoleUser, Content: "question"}} }
func call(id, name string) ToolCall {
	return ToolCall{ID: id, Name: name, Arguments: json.RawMessage(`{"x":1}`)}
}
func answer(text string) Response {
	return Response{Message: Message{Role: RoleAssistant, Content: text}, StopReason: StopEnd}
}
func toolResponse(calls ...ToolCall) Response {
	return Response{Message: Message{Role: RoleAssistant, ToolCalls: calls}, StopReason: StopToolUse}
}
func basicTool(name string) Tool {
	return Tool{
		Definition: ToolDefinition{Name: name, Description: "test tool", Parameters: json.RawMessage(`{"type":"object"}`)},
		Validate:   func(json.RawMessage) error { return nil },
		Execute: func(_ context.Context, c ToolCall, _ func(ToolResult)) (ToolResult, error) {
			return ToolResult{Content: "result:" + c.ID}, nil
		},
	}
}

func TestMultiTurnSerialToolsAndLifecycle(t *testing.T) {
	var events []Event
	var order []string
	var lateModels []func(Message)
	var lateTools []func(ToolResult)
	tool := basicTool("work")
	tool.Execute = func(_ context.Context, c ToolCall, update func(ToolResult)) (ToolResult, error) {
		order = append(order, "tool:"+c.ID)
		update(ToolResult{Content: "progress", Terminate: true})
		lateTools = append(lateTools, update)
		return ToolResult{Content: "result:" + c.ID}, nil
	}
	step := 0
	a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, req Request, update func(Message)) (Response, error) {
		step++
		order = append(order, fmt.Sprintf("model:%d", step))
		equal(t, req.SystemPrompt, "system")
		equal(t, req.MaxTokens, int64(100000-(step-1)*5))
		equal(t, req.Tools, []ToolDefinition{tool.Definition})
		for _, late := range lateModels {
			late(Message{Role: RoleAssistant, Content: "late model"})
		}
		for _, late := range lateTools {
			late(ToolResult{Content: "late tool", Terminate: true})
		}
		lateModels = append(lateModels, update)
		update(Message{Role: RoleAssistant, Content: "partial"})
		var r Response
		switch step {
		case 1:
			equal(t, req.Messages, prompt())
			r = toolResponse(call("a", "work"), call("b", "work"))
		case 2:
			equal(t, req.Messages, []Message{prompt()[0], toolResponse(call("a", "work"), call("b", "work")).Message,
				{Role: RoleTool, ToolCallID: "a", Content: "result:a"}, {Role: RoleTool, ToolCallID: "b", Content: "result:b"}})
			r = toolResponse(call("c", "work"))
		case 3:
			equal(t, len(req.Messages), 6)
			equal(t, req.Messages[5], Message{Role: RoleTool, ToolCallID: "c", Content: "result:c"})
			r = answer("done")
		default:
			t.Fatal("unexpected extra model call")
		}
		r.Usage = Usage{InputTokens: 2, OutputTokens: 3}
		return r, nil
	})})
	r, err := a.Run(context.Background(), Context{SystemPrompt: "system"}, prompt(), func(e Event) {
		if !a.Running() {
			t.Error("Running is false inside observer")
		}
		events = append(events, e)
	})
	outcome(t, r, err, nil, StopEnd, 3)
	equal(t, r.Usage, Usage{InputTokens: 6, OutputTokens: 9})
	equal(t, len(r.Messages), 7)
	equal(t, r.Messages[6], answer("done").Message)
	equal(t, order, []string{"model:1", "tool:a", "tool:b", "model:2", "tool:c", "model:3"})
	equal(t, a.Running(), false)
	want := []EventType{EventAgentStart, EventMessageStart, EventMessageEnd}
	for _, count := range []int{2, 1, 0} {
		want = append(want, EventTurnStart, EventMessageStart, EventMessageUpdate, EventMessageEnd)
		for range count {
			want = append(want, EventToolExecutionStart, EventToolExecutionUpdate, EventToolExecutionEnd, EventMessageStart, EventMessageEnd)
		}
		want = append(want, EventTurnEnd)
	}
	want = append(want, EventAgentEnd)
	var got []EventType
	turn := 0
	for _, e := range events {
		got = append(got, e.Type)
		if e.Type == EventTurnStart {
			turn++
		}
		equal(t, e.Turn, turn)
		if e.Type == EventMessageUpdate {
			equal(t, e.Message.Content, "partial")
		}
		if e.Type == EventToolExecutionUpdate {
			equal(t, e.ToolResult.Content, "progress")
		}
		if e.Type == EventToolExecutionEnd {
			equal(t, e.ToolResult, ToolResult{Content: "result:" + e.ToolCall.ID})
		}
	}
	equal(t, got, want)
	equal(t, events[len(events)-1].Result, r)
	for _, late := range lateModels {
		late(Message{Role: RoleUser, Content: "late invalid"})
	}
	for _, late := range lateTools {
		late(ToolResult{Content: "late"})
	}
	equal(t, len(events), len(want))
}

func TestOrdinaryToolFailuresAreFedBack(t *testing.T) {
	validationErr := errors.New("validator rejected")
	executionErr := errors.New("executor failed")
	var validated, executed []string
	tool := basicTool("work")
	tool.Validate = func(raw json.RawMessage) error {
		validated = append(validated, string(raw))
		if strings.Contains(string(raw), "reject") {
			return validationErr
		}
		return nil
	}
	tool.Execute = func(_ context.Context, c ToolCall, _ func(ToolResult)) (ToolResult, error) {
		executed = append(executed, c.ID)
		if c.ID == "execute" {
			return ToolResult{Content: "partial result"}, executionErr
		}
		return ToolResult{Content: "reported failure", IsError: true}, nil
	}
	calls := []ToolCall{call("unknown", "missing")}
	for i, raw := range []string{"", "{", "[]", "null", "1", `"text"`, "true", "{} {}"} {
		calls = append(calls, ToolCall{ID: fmt.Sprintf("json%d", i), Name: "work", Arguments: json.RawMessage(raw)})
	}
	calls = append(calls, ToolCall{ID: "validate", Name: "work", Arguments: json.RawMessage(`{"reject":true}`)}, call("execute", "work"), call("reported", "work"))
	step := 0
	var endErrors []error
	a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, req Request, _ func(Message)) (Response, error) {
		step++
		if step == 1 {
			return toolResponse(calls...), nil
		}
		equal(t, step, 2)
		equal(t, len(req.Messages), len(calls)+2)
		for i, c := range calls {
			m := req.Messages[i+2]
			equal(t, m.Role, RoleTool)
			equal(t, m.ToolCallID, c.ID)
			equal(t, m.IsError, true)
			if m.Content == "" {
				t.Errorf("empty error result for %s", c.ID)
			}
		}
		for index, fragment := range map[int]string{2: "unknown tool", len(calls) - 1: validationErr.Error(), len(calls): "partial result\n" + executionErr.Error(), len(calls) + 1: "reported failure"} {
			if !strings.Contains(req.Messages[index].Content, fragment) {
				t.Errorf("result %d = %q, want %q", index, req.Messages[index].Content, fragment)
			}
		}
		return answer("recovered"), nil
	})})
	r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
		if e.Type == EventToolExecutionEnd {
			endErrors = append(endErrors, e.Err)
		}
	})
	outcome(t, r, err, nil, StopEnd, 2)
	equal(t, validated, []string{`{"reject":true}`, `{"x":1}`, `{"x":1}`})
	equal(t, executed, []string{"execute", "reported"})
	equal(t, len(endErrors), len(calls))
	if !errors.Is(endErrors[len(calls)-3], validationErr) || !errors.Is(endErrors[len(calls)-2], executionErr) {
		t.Fatal("tool end events lost original errors")
	}
	equal(t, endErrors[len(calls)-1], nil)
}

func TestSpecialToolAndTruncation(t *testing.T) {
	for _, mode := range []string{"terminate", "length_calls", "length_empty"} {
		t.Run(mode, func(t *testing.T) {
			var executed, starts []string
			tool := basicTool("work")
			tool.Execute = func(_ context.Context, c ToolCall, _ func(ToolResult)) (ToolResult, error) {
				executed = append(executed, c.ID)
				return ToolResult{Content: "finished", Terminate: true}, nil
			}
			step := 0
			a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, req Request, _ func(Message)) (Response, error) {
				step++
				if step == 2 {
					equal(t, mode, "length_calls")
					for _, m := range req.Messages[2:] {
						equal(t, m.IsError, true)
						if !strings.Contains(m.Content, "truncated") {
							t.Error("missing truncation explanation")
						}
					}
					return answer("recovered"), nil
				}
				equal(t, step, 1)
				r := toolResponse(call("a", "work"), call("b", "work"), call("c", "work"))
				if mode != "terminate" {
					r.StopReason = StopLength
					r.Message.ToolCalls[0].Arguments = json.RawMessage(`{"unfinished":`)
				}
				if mode == "length_empty" {
					r.Message.ToolCalls = nil
				}
				return r, nil
			})})
			r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
				if e.Type == EventToolExecutionStart {
					starts = append(starts, e.ToolCall.ID)
				}
			})
			switch mode {
			case "terminate":
				outcome(t, r, err, nil, StopTerminate, 1)
				equal(t, executed, []string{"a"})
				equal(t, starts, []string{"a"})
				equal(t, len(r.Messages), 5)
				equal(t, r.Messages[2], Message{Role: RoleTool, ToolCallID: "a", Content: "finished"})
				for i, id := range []string{"b", "c"} {
					equal(t, r.Messages[i+3].ToolCallID, id)
					equal(t, r.Messages[i+3].IsError, true)
					if !strings.Contains(r.Messages[i+3].Content, "terminated") {
						t.Error("missing termination explanation")
					}
				}
			case "length_calls":
				outcome(t, r, err, nil, StopEnd, 2)
				equal(t, len(r.Messages), 6)
				equal(t, len(executed), 0)
				equal(t, len(starts), 0)
			case "length_empty":
				outcome(t, r, err, nil, StopLength, 1)
				equal(t, len(executed), 0)
			}
		})
	}
}

func TestTerminationPreservesErrors(t *testing.T) {
	for _, stage := range []string{"error", "tool_cancel", "skip_cancel"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			boom := errors.New("terminal tool failed")
			models, executions := 0, 0
			tool := basicTool("work")
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				executions++
				if stage == "tool_cancel" {
					cancel()
				}
				return ToolResult{Content: "partial", Terminate: true}, boom
			}
			a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
				models++
				return toolResponse(call("a", "work"), call("b", "work"), call("c", "work")), nil
			})})
			var ends []Event
			var toolEnd Event
			var messageEnd Event
			r, err := a.Run(ctx, Context{}, prompt(), func(e Event) {
				if stage == "skip_cancel" && e.Type == EventMessageStart && e.Message.ToolCallID == "b" {
					cancel()
				}
				switch e.Type {
				case EventTurnEnd, EventAgentEnd:
					ends = append(ends, e)
				case EventToolExecutionEnd:
					toolEnd = e
				case EventMessageEnd:
					if e.Message.ToolCallID == "a" {
						messageEnd = e
					}
				}
			})
			wantReason := StopError
			if stage != "error" {
				wantReason = StopCanceled
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
			}
			outcome(t, r, err, boom, wantReason, 1)
			equal(t, models, 1)
			equal(t, executions, 1)
			equal(t, len(r.Messages), 5)
			for i, id := range []string{"a", "b", "c"} {
				equal(t, r.Messages[i+2].ToolCallID, id)
				equal(t, r.Messages[i+2].IsError, true)
			}
			if !strings.Contains(r.Messages[2].Content, boom.Error()) {
				t.Fatal("lost failure in history")
			}
			equal(t, len(ends), 2)
			for _, e := range ends {
				if !errors.Is(e.Err, boom) || stage != "error" && !errors.Is(e.Err, context.Canceled) {
					t.Fatalf("end event lost error: %#v", e)
				}
				equal(t, e.StopReason, wantReason)
			}
			if !errors.Is(toolEnd.Err, boom) {
				t.Fatal("tool end lost error")
			}
			equal(t, messageEnd.StopReason, StopError)
			toolReason := StopError
			if stage == "tool_cancel" {
				toolReason = StopCanceled
			}
			equal(t, toolEnd.StopReason, toolReason)
		})
	}
}

func TestHistoryValidation(t *testing.T) {
	paired := []Message{toolResponse(call("old", "work")).Message, {Role: RoleTool, ToolCallID: "old", Content: "ok"}}
	invalid := map[string][]Message{
		"unknown_role":        {{Role: "system"}},
		"user_calls":          {{Role: RoleUser, ToolCalls: []ToolCall{call("a", "work")}}},
		"user_result_id":      {{Role: RoleUser, ToolCallID: "a"}},
		"user_error":          {{Role: RoleUser, IsError: true}},
		"assistant_error":     {{Role: RoleAssistant, IsError: true}},
		"assistant_result_id": {{Role: RoleAssistant, ToolCallID: "a"}},
		"tool_blank_id":       {{Role: RoleTool, ToolCallID: " "}},
		"tool_calls":          {{Role: RoleTool, ToolCallID: "a", ToolCalls: []ToolCall{call("a", "work")}}},
		"orphan_result":       {{Role: RoleTool, ToolCallID: "a"}},
		"pending":             {toolResponse(call("a", "work")).Message},
		"interrupted":         {toolResponse(call("a", "work")).Message, prompt()[0]},
		"blank_call_id":       {toolResponse(call(" ", "work")).Message},
		"blank_tool_name":     {toolResponse(call("a", " ")).Message},
		"duplicate_batch":     {toolResponse(call("a", "work"), call("a", "work")).Message},
		"duplicate_history":   {paired[0], paired[1], paired[0], paired[1]},
		"duplicate_result":    {paired[0], paired[1], paired[1]},
		"wrong_result":        {paired[0], {Role: RoleTool, ToolCallID: "wrong"}},
	}
	for name, messages := range invalid {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%v", name, resume), func(t *testing.T) {
				a := newAgent(t, Config{Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
					t.Fatal("invalid history reached model")
					return Response{}, nil
				})})
				var r Result
				var err error
				if resume {
					r, err = a.Continue(context.Background(), Context{Messages: messages}, nil)
				} else {
					r, err = a.Run(context.Background(), Context{Messages: messages}, prompt(), nil)
				}
				outcome(t, r, err, ErrInvalidContext, StopError, 0)
				equal(t, a.Running(), false)
			})
		}
	}
	for _, tc := range []struct {
		name          string
		history       []Message
		prompts       []Message
		resume, valid bool
	}{
		{"run_empty_prompts", nil, nil, false, false},
		{"run_assistant_prompt", nil, []Message{answer("no").Message}, false, false},
		{"continue_empty", nil, nil, true, false},
		{"continue_assistant", []Message{answer("no").Message}, nil, true, false},
		{"continue_user", prompt(), nil, true, true},
		{"continue_results", paired, nil, true, true},
		{"run_assistant_tail", []Message{answer("old").Message}, prompt(), false, true},
		{"run_multiple_prompts", paired, append(prompt(), prompt()...), false, true},
		{"paired_bad_json", []Message{toolResponse(ToolCall{ID: "bad", Name: "missing", Arguments: json.RawMessage(`{`)}).Message, {Role: RoleTool, ToolCallID: "bad", IsError: true}}, nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			a := newAgent(t, Config{Model: modelFunc(func(_ context.Context, req Request, _ func(Message)) (Response, error) {
				calls++
				equal(t, req.Messages, append(append([]Message(nil), tc.history...), tc.prompts...))
				return answer("ok"), nil
			})})
			var r Result
			var err error
			if tc.resume {
				r, err = a.Continue(context.Background(), Context{Messages: tc.history}, nil)
			} else {
				r, err = a.Run(context.Background(), Context{Messages: tc.history}, tc.prompts, nil)
			}
			if tc.valid {
				outcome(t, r, err, nil, StopEnd, 1)
				equal(t, calls, 1)
			} else {
				outcome(t, r, err, ErrInvalidContext, StopError, 0)
				equal(t, calls, 0)
			}
		})
	}
}

func TestInvalidModelResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response Response
		history  []Message
		update   *Message
	}{
		{"wrong_role", Response{Message: prompt()[0], StopReason: StopEnd}, nil, nil},
		{"assistant_error", Response{Message: Message{Role: RoleAssistant, IsError: true}, StopReason: StopEnd}, nil, nil},
		{"unknown_stop", Response{Message: answer("bad").Message, StopReason: StopTerminate}, nil, nil},
		{"empty_stop", Response{Message: answer("bad").Message}, nil, nil},
		{"end_with_calls", Response{Message: toolResponse(call("a", "work")).Message, StopReason: StopEnd}, nil, nil},
		{"tool_use_empty", toolResponse(), nil, nil},
		{"blank_id", toolResponse(call(" ", "work")), nil, nil},
		{"blank_name", toolResponse(call("a", " ")), nil, nil},
		{"duplicate_batch", toolResponse(call("a", "work"), call("a", "work")), nil, nil},
		{"duplicate_history", toolResponse(call("old", "work")), []Message{toolResponse(call("old", "work")).Message, {Role: RoleTool, ToolCallID: "old"}}, nil},
		{"invalid_update_role", answer("ok"), nil, &Message{Role: RoleUser}},
		{"invalid_update_shape", answer("ok"), nil, &Message{Role: RoleAssistant, ToolCallID: "bad"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tool := basicTool("work")
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				t.Fatal("invalid response executed tool")
				return ToolResult{}, nil
			}
			a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, _ Request, update func(Message)) (Response, error) {
				if tc.update != nil {
					update(*tc.update)
				}
				return tc.response, nil
			})})
			r, err := a.Run(context.Background(), Context{Messages: tc.history}, prompt(), nil)
			outcome(t, r, err, ErrInvalidResponse, StopError, 1)
			equal(t, r.Messages[len(tc.history)+1], tc.response.Message)
			seen := map[string]bool{}
			for _, m := range r.Messages[len(tc.history)+2:] {
				if seen[m.ToolCallID] || strings.TrimSpace(m.ToolCallID) == "" {
					t.Fatal("invalid synthesized result IDs")
				}
				seen[m.ToolCallID] = true
				equal(t, m.IsError, true)
			}
		})
	}
}

func TestModelFailureRetainsPartialSnapshot(t *testing.T) {
	boom := errors.New("model transport failed")
	partial := toolResponse(ToolCall{ID: "partial", Name: "work", Arguments: json.RawMessage(`{"x":`)}).Message
	var ended Message
	a := newAgent(t, Config{Model: modelFunc(func(_ context.Context, _ Request, update func(Message)) (Response, error) {
		update(partial)
		partial.ToolCalls[0].Arguments[0] = '!'
		return Response{}, boom
	})})
	r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
		if e.Type == EventMessageEnd && e.Message.Role == RoleAssistant {
			ended = e.Message
		}
	})
	outcome(t, r, err, boom, StopError, 1)
	equal(t, string(r.Messages[1].ToolCalls[0].Arguments), `{"x":`)
	equal(t, ended, r.Messages[1])
	equal(t, r.Messages[2].ToolCallID, "partial")
	equal(t, r.Messages[2].IsError, true)
}

func TestModelFailureRetainsFinalResponse(t *testing.T) {
	for _, withCalls := range []bool{false, true} {
		t.Run(fmt.Sprint(withCalls), func(t *testing.T) {
			boom := errors.New("model failed after final response")
			final := answer("authoritative final")
			if withCalls {
				final = toolResponse(call("final", "work"))
			}
			final.Usage = Usage{InputTokens: 7, OutputTokens: 3}
			tool := basicTool("work")
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				t.Fatal("failed model response must not execute tools")
				return ToolResult{}, nil
			}
			models := 0
			a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, _ Request, update func(Message)) (Response, error) {
				models++
				update(toolResponse(call("partial", "work")).Message)
				return final, boom
			})})
			var ended Message
			r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
				if e.Type == EventMessageEnd && e.Message.Role == RoleAssistant {
					ended = e.Message
					if !errors.Is(e.Err, boom) {
						t.Fatal("message end lost model error")
					}
				}
			})
			outcome(t, r, err, boom, StopError, 1)
			equal(t, models, 1)
			equal(t, r.Usage, final.Usage)
			equal(t, r.Messages[1], final.Message)
			equal(t, ended, final.Message)
			if withCalls {
				equal(t, len(r.Messages), 3)
				equal(t, r.Messages[2].ToolCallID, "final")
				equal(t, r.Messages[2].IsError, true)
			} else {
				equal(t, len(r.Messages), 2)
			}
		})
	}
}

func TestTurnAndTokenBudgets(t *testing.T) {
	for _, limit := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("turns_%d", limit), func(t *testing.T) {
			want := limit
			if want == 0 {
				want = 32
			}
			models, tools := 0, 0
			tool := basicTool("work")
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				tools++
				return ToolResult{}, nil
			}
			a := newAgent(t, Config{MaxTurns: limit, Tools: []Tool{tool}, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
				models++
				return toolResponse(call(fmt.Sprint(models), "work")), nil
			})})
			r, err := a.Run(context.Background(), Context{}, prompt(), nil)
			outcome(t, r, err, ErrMaxTurns, StopMaxTurns, want)
			equal(t, models, want)
			equal(t, tools, want)
			equal(t, len(r.Messages), 1+2*want)
		})
	}
	for _, tc := range []struct {
		name    string
		limit   int64
		usages  []Usage
		wantErr error
		reason  StopReason
		tools   int
	}{
		{"below", 10, []Usage{{InputTokens: 4, OutputTokens: 5}}, nil, StopEnd, 0},
		{"exact", 10, []Usage{{InputTokens: 4, OutputTokens: 6}}, ErrMaxTokens, StopMaxTokens, 0},
		{"over", 10, []Usage{{InputTokens: 4, OutputTokens: 7}}, ErrMaxTokens, StopMaxTokens, 0},
		{"default", 0, []Usage{{InputTokens: 100000}}, ErrMaxTokens, StopMaxTokens, 0},
		{"remaining", 10, []Usage{{InputTokens: 2, OutputTokens: 3}, {InputTokens: 4}}, nil, StopEnd, 1},
		{"negative_input", 10, []Usage{{InputTokens: -1}}, ErrInvalidResponse, StopError, 0},
		{"negative_output", 10, []Usage{{OutputTokens: -1}}, ErrInvalidResponse, StopError, 0},
		{"single_overflow", math.MaxInt64, []Usage{{InputTokens: math.MaxInt64, OutputTokens: 1}}, ErrInvalidResponse, StopError, 0},
		{"accumulated_overflow", math.MaxInt64, []Usage{{InputTokens: math.MaxInt64 - 2}, {OutputTokens: 3}}, ErrInvalidResponse, StopError, 1},
		{"max_int_exact", math.MaxInt64, []Usage{{InputTokens: math.MaxInt64 - 1, OutputTokens: 1}}, ErrMaxTokens, StopMaxTokens, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			step, executed := 0, 0
			var accepted Usage
			tool := basicTool("work")
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				executed++
				return ToolResult{}, nil
			}
			a := newAgent(t, Config{MaxTokens: tc.limit, Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, req Request, _ func(Message)) (Response, error) {
				limit := tc.limit
				if limit == 0 {
					limit = 100000
				}
				equal(t, req.MaxTokens, limit-accepted.InputTokens-accepted.OutputTokens)
				if step >= len(tc.usages) {
					t.Fatal("budget allowed extra request")
				}
				u := tc.usages[step]
				step++
				r := answer("done")
				if step < len(tc.usages) || tc.wantErr != nil {
					r = toolResponse(call(fmt.Sprint(step), "work"))
				}
				r.Usage = u
				if !(step == len(tc.usages) && tc.wantErr == ErrInvalidResponse) {
					accepted.InputTokens += u.InputTokens
					accepted.OutputTokens += u.OutputTokens
				}
				return r, nil
			})})
			r, err := a.Run(context.Background(), Context{}, prompt(), nil)
			outcome(t, r, err, tc.wantErr, tc.reason, len(tc.usages))
			equal(t, r.Usage, accepted)
			equal(t, executed, tc.tools)
			if tc.wantErr != nil {
				equal(t, r.Messages[len(r.Messages)-1].IsError, true)
			}
		})
	}
}

func TestTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		timeout, parent time.Duration
		tool            bool
		want            time.Duration
	}{
		{"default_model", 0, 0, false, 2 * time.Minute},
		{"configured_model", 3 * time.Second, 0, false, 3 * time.Second},
		{"parent_deadline", time.Minute, time.Second, false, time.Second},
		{"tool_deadline", 5 * time.Second, 0, true, 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				if tc.parent != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.parent)
					defer cancel()
				}
				start := time.Now()
				tool := basicTool("work")
				tool.Execute = func(ctx context.Context, _ ToolCall, _ func(ToolResult)) (ToolResult, error) {
					<-ctx.Done()
					return ToolResult{}, ctx.Err()
				}
				a := newAgent(t, Config{Timeout: tc.timeout, Tools: []Tool{tool}, Model: modelFunc(func(ctx context.Context, _ Request, _ func(Message)) (Response, error) {
					deadline, ok := ctx.Deadline()
					equal(t, ok, true)
					equal(t, deadline.Sub(start), tc.want)
					if tc.tool {
						return toolResponse(call("a", "work"), call("b", "work")), nil
					}
					<-ctx.Done()
					return Response{}, ctx.Err()
				})})
				r, err := a.Run(ctx, Context{}, prompt(), nil)
				outcome(t, r, err, context.DeadlineExceeded, StopCanceled, 1)
				equal(t, time.Since(start), tc.want)
				equal(t, a.Running(), false)
				if tc.tool {
					equal(t, len(r.Messages), 4)
					equal(t, r.Messages[3].IsError, true)
				}
			})
		})
	}
}

func TestCancellationPreservesRealErrors(t *testing.T) {
	boom := errors.New("real failure")
	for _, stage := range []string{"before", "model", "validator", "tool", "tool_joined", "tool_canceled_without_parent", "model_budget"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			models, executions := 0, 0
			tool := basicTool("work")
			tool.Validate = func(json.RawMessage) error {
				if stage == "validator" {
					cancel()
					return boom
				}
				return nil
			}
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				executions++
				if stage == "tool_canceled_without_parent" {
					return ToolResult{}, context.Canceled
				}
				cancel()
				if stage == "tool_joined" {
					return ToolResult{}, errors.Join(context.Canceled, boom)
				}
				return ToolResult{Content: "partial"}, boom
			}
			a := newAgent(t, Config{MaxTokens: 10, Tools: []Tool{tool}, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
				models++
				r := toolResponse(call("a", "work"), call("b", "work"))
				if stage == "model" || stage == "model_budget" {
					cancel()
					if stage == "model_budget" {
						r.Usage.InputTokens = 10
					}
					return r, boom
				}
				return r, nil
			})})
			if stage == "before" {
				cancel()
			}
			r, err := a.Run(ctx, Context{}, prompt(), nil)
			turns := 1
			if stage == "before" {
				turns = 0
			}
			outcome(t, r, err, context.Canceled, StopCanceled, turns)
			equal(t, models, turns)
			if stage != "before" && stage != "tool_canceled_without_parent" && !errors.Is(err, boom) {
				t.Fatalf("lost real error: %v", err)
			}
			if stage == "model_budget" && !errors.Is(err, ErrMaxTokens) {
				t.Fatalf("lost budget error: %v", err)
			}
			wantExec := 0
			if strings.HasPrefix(stage, "tool") {
				wantExec = 1
			}
			equal(t, executions, wantExec)
			if turns == 1 {
				equal(t, len(r.Messages), 4)
				for i, id := range []string{"a", "b"} {
					equal(t, r.Messages[i+2].ToolCallID, id)
					equal(t, r.Messages[i+2].IsError, true)
				}
			}
		})
	}
}

func TestObserverCancellationBoundaries(t *testing.T) {
	for _, stage := range []string{"agent_start", "turn_start", "assistant_start", "assistant_end", "tool_start", "validator", "tool_end", "tool_message_end", "turn_end", "agent_end"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			models, executions := 0, 0
			tool := basicTool("work")
			tool.Validate = func(json.RawMessage) error {
				if stage == "validator" {
					cancel()
				}
				return nil
			}
			tool.Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
				executions++
				return ToolResult{Content: "ok"}, nil
			}
			a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
				models++
				return toolResponse(call("a", "work"), call("b", "work")), nil
			})})
			r, err := a.Run(ctx, Context{}, prompt(), func(e Event) {
				match := string(e.Type) == stage
				match = match || stage == "assistant_start" && e.Type == EventMessageStart && e.Message.Role == RoleAssistant
				match = match || stage == "assistant_end" && e.Type == EventMessageEnd && e.Message.Role == RoleAssistant
				match = match || stage == "tool_start" && e.Type == EventToolExecutionStart
				match = match || stage == "tool_end" && e.Type == EventToolExecutionEnd
				match = match || stage == "tool_message_end" && e.Type == EventMessageEnd && e.Message.Role == RoleTool
				if stage == "agent_end" && e.Type == EventTurnEnd {
					cancel()
				}
				if match {
					cancel()
				}
			})
			turns := 1
			if stage == "agent_start" {
				turns = 0
			}
			outcome(t, r, err, context.Canceled, StopCanceled, turns)
			wantModels := 1
			if stage == "agent_start" || stage == "turn_start" || stage == "assistant_start" {
				wantModels = 0
			}
			equal(t, models, wantModels)
			wantExec := 0
			if stage == "tool_end" || stage == "tool_message_end" {
				wantExec = 1
			}
			if stage == "turn_end" || stage == "agent_end" {
				wantExec = 2
			}
			equal(t, executions, wantExec)
		})
	}
	t.Run("cancel_only_at_agent_end", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		a := newAgent(t, Config{Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) { return answer("ok"), nil })})
		r, err := a.Run(ctx, Context{}, prompt(), func(e Event) {
			if e.Type == EventAgentEnd {
				cancel()
			}
		})
		outcome(t, r, err, context.Canceled, StopCanceled, 1)
	})
}

func TestConcurrentRunRejectedAndRunningObservable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var r Result
		var runErr error
		a := newAgent(t, Config{Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
			close(entered)
			<-release
			return answer("done"), nil
		})})
		go func() {
			defer close(done)
			r, runErr = a.Run(context.Background(), Context{}, prompt(), func(e Event) {
				if !a.Running() {
					t.Error("observer saw idle agent")
				}
				if e.Type == EventAgentStart {
					_, err := a.Continue(context.Background(), Context{Messages: prompt()}, nil)
					if !errors.Is(err, ErrRunning) {
						t.Errorf("reentrant Continue = %v", err)
					}
				}
			})
		}()
		<-entered
		equal(t, a.Running(), true)
		for _, resume := range []bool{false, true} {
			var rejected Result
			var err error
			if resume {
				rejected, err = a.Continue(context.Background(), Context{Messages: prompt()}, nil)
			} else {
				rejected, err = a.Run(context.Background(), Context{}, prompt(), nil)
			}
			outcome(t, rejected, err, ErrRunning, StopError, 0)
			equal(t, rejected.Messages, prompt())
		}
		close(release)
		<-done
		outcome(t, r, runErr, nil, StopEnd, 1)
		equal(t, a.Running(), false)
	})
}

func TestSnapshotIsolation(t *testing.T) {
	mutate := func(messages []Message) {
		for i := range messages {
			messages[i].Content = "mutated"
			for j := range messages[i].ToolCalls {
				messages[i].ToolCalls[j].Name = "mutated"
				if len(messages[i].ToolCalls[j].Arguments) > 0 {
					messages[i].ToolCalls[j].Arguments[0] = '!'
				}
			}
		}
	}
	history := Context{SystemPrompt: "system", Messages: []Message{toolResponse(call("old", "work")).Message, {Role: RoleTool, ToolCallID: "old", Content: "old result"}}}
	prompts := prompt()
	original := []Message{toolResponse(call("old", "work")).Message, {Role: RoleTool, ToolCallID: "old", Content: "old result"}, prompt()[0]}
	tool := basicTool("work")
	tool.Validate = func(raw json.RawMessage) error { raw[0] = '!'; return nil }
	tool.Execute = func(_ context.Context, c ToolCall, update func(ToolResult)) (ToolResult, error) {
		equal(t, c, call("new", "work"))
		c.Arguments[0] = '!'
		c.Name = "mutated"
		update(ToolResult{Content: "progress"})
		return ToolResult{Content: "new result"}, nil
	}
	final := toolResponse(call("new", "work"))
	var retained Request
	var firstUpdate Message
	step := 0
	cfg := Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, req Request, update func(Message)) (Response, error) {
		step++
		equal(t, req.SystemPrompt, "system")
		equal(t, req.Tools[0], basicTool("work").Definition)
		if step == 1 {
			equal(t, req.Messages, original)
			retained = req
			mutate(req.Messages)
			req.Tools[0].Name = "changed"
			req.Tools[0].Parameters[0] = '!'
			m := toolResponse(call("stream", "work")).Message
			m.Content = "partial"
			update(m)
			mutate([]Message{m})
			return final, nil
		}
		equal(t, step, 2)
		mutate(retained.Messages)
		mutate([]Message{final.Message})
		equal(t, req.Messages[:3], original)
		equal(t, req.Messages[3], toolResponse(call("new", "work")).Message)
		equal(t, req.Messages[4], Message{Role: RoleTool, ToolCallID: "new", Content: "new result"})
		return answer("done"), nil
	})}
	a := newAgent(t, cfg)
	cfg.Tools[0].Definition.Name = "changed"
	cfg.Tools[0].Definition.Parameters[0] = '!'
	cfg.Tools[0].Execute = func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error) {
		t.Fatal("config was not copied")
		return ToolResult{}, nil
	}
	var endResult Result
	r, err := a.Run(context.Background(), history, prompts, func(e Event) {
		if e.Type == EventAgentStart {
			mutate(history.Messages)
			mutate(prompts)
			history.SystemPrompt = "changed"
		}
		if e.Type == EventMessageUpdate {
			firstUpdate = e.Message
			return
		}
		if e.Type == EventAgentEnd {
			endResult = e.Result
		}
		mutate([]Message{e.Message})
		if len(e.ToolCall.Arguments) > 0 {
			e.ToolCall.Arguments[0] = '!'
		}
		if e.Type != EventAgentEnd {
			mutate(e.Result.Messages)
		}
	})
	outcome(t, r, err, nil, StopEnd, 2)
	equal(t, r.Messages[:3], original)
	equal(t, r.Messages[3], toolResponse(call("new", "work")).Message)
	equal(t, firstUpdate.ToolCalls[0], call("stream", "work"))
	equal(t, firstUpdate.Content, "partial")
	equal(t, endResult, r)
	mutate(endResult.Messages)
	equal(t, r.Messages[:3], original)
	mutate(r.Messages)
	equal(t, original[0].ToolCalls[0], call("old", "work"))
}

func TestConcurrentUpdatesAndLateUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers = 24
		var modelLate func(Message)
		var toolLate func(ToolResult)
		var observerActive atomic.Int32
		var events []Event
		parallel := func(f func(int)) {
			var wg sync.WaitGroup
			gate := make(chan struct{})
			for i := range workers {
				wg.Add(1)
				go func() { defer wg.Done(); <-gate; f(i) }()
			}
			close(gate)
			wg.Wait()
		}
		tool := basicTool("work")
		tool.Execute = func(_ context.Context, _ ToolCall, update func(ToolResult)) (ToolResult, error) {
			toolLate = update
			parallel(func(i int) {
				update(ToolResult{Content: fmt.Sprint(i), Terminate: true})
				modelLate(Message{Role: RoleUser})
			})
			return ToolResult{Content: "final tool"}, nil
		}
		step := 0
		a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, _ Request, update func(Message)) (Response, error) {
			step++
			if step == 2 {
				parallel(func(i int) { toolLate(ToolResult{Content: "late"}); modelLate(Message{Role: RoleUser}) })
				return answer("done"), nil
			}
			modelLate = update
			parallel(func(i int) {
				m := toolResponse(call(fmt.Sprint(i), "work")).Message
				update(m)
				m.ToolCalls[0].Arguments[0] = '!'
			})
			return toolResponse(call("a", "work")), nil
		})})
		r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
			if observerActive.Add(1) != 1 {
				t.Error("concurrent observer delivery")
			}
			defer observerActive.Add(-1)
			if !a.Running() {
				t.Error("observer saw idle")
			}
			events = append(events, e)
		})
		outcome(t, r, err, nil, StopEnd, 2)
		counts := map[EventType]int{}
		for _, e := range events {
			counts[e.Type]++
			if e.Type == EventMessageUpdate {
				equal(t, string(e.Message.ToolCalls[0].Arguments), `{"x":1}`)
			}
		}
		equal(t, counts[EventMessageUpdate], workers)
		equal(t, counts[EventToolExecutionUpdate], workers)
		equal(t, r.Messages[2].Content, "final tool")
		before := len(events)
		parallel(func(int) { modelLate(Message{Role: RoleUser}); toolLate(ToolResult{Terminate: true}) })
		equal(t, len(events), before)
	})
}

func TestUpdatesRacingWithCallClosure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var wg sync.WaitGroup
		launch := func(update func()) {
			gate := make(chan struct{})
			for range 64 {
				wg.Add(1)
				go func() { defer wg.Done(); <-gate; update() }()
			}
			close(gate)
		}
		tool := basicTool("work")
		tool.Execute = func(_ context.Context, _ ToolCall, update func(ToolResult)) (ToolResult, error) {
			launch(func() { update(ToolResult{Content: "racing tool"}) })
			return ToolResult{Content: "final tool"}, nil
		}
		step := 0
		a := newAgent(t, Config{Tools: []Tool{tool}, Model: modelFunc(func(_ context.Context, _ Request, update func(Message)) (Response, error) {
			step++
			if step == 1 {
				launch(func() { update(Message{Role: RoleAssistant, Content: "racing model"}) })
				return toolResponse(call("a", "work")), nil
			}
			return answer("done"), nil
		})})
		modelEnded, toolEnded, agentEnded := false, false, false
		r, err := a.Run(context.Background(), Context{}, prompt(), func(e Event) {
			if agentEnded {
				t.Error("event after agent end")
			}
			switch e.Type {
			case EventMessageEnd:
				if e.Message.Role == RoleAssistant {
					modelEnded = true
				}
			case EventToolExecutionEnd:
				toolEnded = true
			case EventMessageUpdate:
				if modelEnded {
					t.Error("model update after originating call ended")
				}
			case EventToolExecutionUpdate:
				if toolEnded {
					t.Error("tool update after originating call ended")
				}
			case EventAgentEnd:
				agentEnded = true
			}
		})
		wg.Wait()
		outcome(t, r, err, nil, StopEnd, 2)
		equal(t, r.Messages[1], toolResponse(call("a", "work")).Message)
		equal(t, r.Messages[2].Content, "final tool")
		equal(t, r.Messages[3], answer("done").Message)
	})
}

func TestNilContextAndAgentReuse(t *testing.T) {
	calls := 0
	a := newAgent(t, Config{MaxTurns: 1, Model: modelFunc(func(context.Context, Request, func(Message)) (Response, error) {
		calls++
		return answer("ok"), nil
	})})
	r, err := a.Run(nil, Context{}, prompt(), nil)
	outcome(t, r, err, ErrInvalidContext, StopError, 0)
	equal(t, a.Running(), false)
	r, err = a.Continue(nil, Context{Messages: prompt()}, nil)
	outcome(t, r, err, ErrInvalidContext, StopError, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = a.Run(ctx, Context{}, prompt(), nil)
	outcome(t, r, err, context.Canceled, StopCanceled, 0)
	for range 2 {
		r, err = a.Run(context.Background(), Context{}, prompt(), nil)
		outcome(t, r, err, nil, StopEnd, 1)
		equal(t, len(r.Messages), 2)
		equal(t, a.Running(), false)
	}
	equal(t, calls, 2)
}

func TestConfigValidation(t *testing.T) {
	validModel := modelFunc(func(context.Context, Request, func(Message)) (Response, error) { return answer("ok"), nil })
	var nilFunction modelFunc
	var nilPointer *modelFunc
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"nil_model", func(c *Config) { c.Model = nil }},
		{"typed_nil_function", func(c *Config) { c.Model = nilFunction }},
		{"typed_nil_pointer", func(c *Config) { c.Model = nilPointer }},
		{"negative_turns", func(c *Config) { c.MaxTurns = -1 }},
		{"negative_tokens", func(c *Config) { c.MaxTokens = -1 }},
		{"negative_timeout", func(c *Config) { c.Timeout = -1 }},
		{"blank_name", func(c *Config) { c.Tools[0].Definition.Name = " \t" }},
		{"nil_validator", func(c *Config) { c.Tools[0].Validate = nil }},
		{"nil_executor", func(c *Config) { c.Tools[0].Execute = nil }},
		{"duplicate_tool", func(c *Config) { c.Tools = append(c.Tools, c.Tools[0]) }},
		{"array_schema", func(c *Config) { c.Tools[0].Definition.Parameters = json.RawMessage(`[]`) }},
		{"malformed_schema", func(c *Config) { c.Tools[0].Definition.Parameters = json.RawMessage(`{`) }},
		{"nil_schema", func(c *Config) { c.Tools[0].Definition.Parameters = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Model: validModel, Tools: []Tool{basicTool("work")}}
			tc.change(&cfg)
			a, err := New(cfg)
			if a != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New = %v, %v", a, err)
			}
		})
	}
}
