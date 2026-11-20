package brain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"ojj/internal/minipi"
	"ojj/internal/modelapi"
)

type logTransport func(*http.Request) (*http.Response, error)

func (f logTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDecisionLogsSafeCorrelation(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	for _, failed := range []bool{false, true} {
		client, err := modelapi.New(modelapi.Config{APIKey: "private-key", Model: "private-model", BaseURL: "https://private-host.test", HTTPClient: &http.Client{Transport: logTransport(func(*http.Request) (*http.Response, error) {
			if failed {
				return nil, errors.New("private-transport-error")
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"type":"message","role":"assistant","content":[{"type":"text","text":"private-answer"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3}}`))}, nil
		})}})
		if err != nil {
			t.Fatal("client construction failed")
		}
		cfg := testConfig(t, client)
		cfg.ID, cfg.SystemPrompt = "private-region", "private-system"
		cfg.InputContext = func(_ context.Context, id string) (context.Context, error) {
			// Even a detached context with an external correlation must be safe.
			return minipi.WithInputID(context.Background(), id), nil
		}
		r := openRegion(t, cfg)
		buf.Reset()
		if _, err := r.claim(context.Background()); !errors.Is(err, ErrNotFound) || buf.Len() != 0 {
			t.Fatal("empty claim must be silent")
		}
		const externalID = "abcdefabcdefabcdefabcdefabcdefab"
		receive(t, r, externalID, "private-prompt")
		in, err := r.claim(context.Background())
		if err != nil {
			t.Fatal("claim failed")
		}
		if err := r.process(context.Background(), in); err != nil {
			t.Fatal("model failure must not stop the queue after persistence")
		}
		stored, err := r.Get(context.Background(), externalID)
		wantStatus := StatusSucceeded
		if failed {
			wantStatus = StatusFailed
		}
		if err != nil || stored.Status != wantStatus {
			t.Fatal("changed persisted outcome")
		}
		if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), externalID) {
			t.Fatal("sensitive decision data logged")
		}
		seen := make(map[string]bool)
		persistedStages := make(map[string]bool)
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) != nil {
				t.Fatal("invalid decision log")
			}
			stage, _ := record["stage"].(string)
			if stage == "evidence_persist" || stage == "finish" {
				if record["input_id"] != fmt.Sprintf("%032x", in.Sequence) {
					t.Fatal("lost persistence correlation")
				}
				persistedStages[stage] = true
			}
			if stage == "process_end" || stage == "agent_end" || stage == "request_end" || stage == "transport" {
				if record["input_id"] != fmt.Sprintf("%032x", in.Sequence) {
					t.Fatal("lost cross-layer correlation")
				}
				seen[record["component"].(string)] = true
				if failed && (record["level"] != "ERROR" || record["failure"] != "failure") {
					t.Fatal("model failure downgraded")
				}
			}
			if stage == "process_end" && record["persisted"] != true {
				t.Fatal("missing persisted outcome")
			}
		}
		if len(persistedStages) != 2 {
			t.Fatal("missing persistence logs")
		}
		if len(seen) != 3 {
			t.Fatal("missing cross-layer completion logs")
		}
		if err := r.Close(); err != nil {
			t.Fatal("close failed")
		}
	}
}

func TestModelSkippedLogsSafeCorrelation(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	calls := 0
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		calls++
		return answer("private-answer"), nil
	}))
	cfg.InputContext = func(_ context.Context, id string) (context.Context, error) {
		return WithModelSkipped(minipi.WithInputID(context.Background(), id), true), nil
	}
	r := openRegion(t, cfg)
	const externalID = "abcdefabcdefabcdefabcdefabcdefab"
	receive(t, r, externalID, "private-prompt")
	in, err := r.claim(context.Background())
	if err != nil {
		t.Fatal("claim failed")
	}
	buf.Reset()
	if err := r.process(context.Background(), in); err != nil || calls != 0 {
		t.Fatal("skipped input ran model or failed")
	}
	if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), externalID) {
		t.Fatal("sensitive skip data logged")
	}
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			t.Fatal("invalid skip log")
		}
		stage, _ := record["stage"].(string)
		if record["component"] != "brain" || record["input_id"] != fmt.Sprintf("%032x", in.Sequence) {
			t.Fatal("skip lost safe host correlation")
		}
		switch stage {
		case "process_start", "model_skipped", "finish", "process_end":
			seen[stage] = true
		default:
			t.Fatal("skip unexpectedly performed history, model or tool work")
		}
		if stage == "model_skipped" && record["level"] != "INFO" {
			t.Fatal("skip log level changed")
		}
		if stage == "process_end" && (record["persisted"] != true || record["status"] != "succeeded" || record["turns"] != float64(0)) {
			t.Fatal("skip not logged as clean persisted success")
		}
	}
	if len(seen) != 4 {
		t.Fatal("missing skip lifecycle logs")
	}
}

func TestDecisionFinalizationFailureLog(t *testing.T) {
	r := openRegion(t, testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return answer("private-answer"), nil
	})))
	receive(t, r, "private-external-id", "private-prompt")
	in, err := r.claim(context.Background())
	if err != nil {
		t.Fatal("claim failed")
	}
	if _, err := r.db.Exec(`CREATE TRIGGER reject_log_finish BEFORE UPDATE ON inputs WHEN NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'private-finish-error'); END`); err != nil {
		t.Fatal("fixture failed")
	}
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	if err := r.process(context.Background(), in); err == nil {
		t.Fatal("lost finalization failure")
	}
	if strings.Contains(buf.String(), "private-") {
		t.Fatal("sensitive failure data logged")
	}
	seen := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			t.Fatal("invalid failure log")
		}
		if record["stage"] == "finish" || record["stage"] == "process_end" {
			if record["input_id"] != fmt.Sprintf("%032x", in.Sequence) {
				t.Fatal("lost finalization failure correlation")
			}
			seen++
			if record["persisted"] != false || record["level"] != "ERROR" {
				t.Fatal("failed finalization reported success")
			}
		}
	}
	if seen != 2 {
		t.Fatal("missing finalization failure logs")
	}
}

func toolCalls(ids ...string) minipi.Response {
	r := minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant}, StopReason: minipi.StopToolUse, Usage: minipi.Usage{InputTokens: 2, OutputTokens: 3}}
	for _, id := range ids {
		r.Message.ToolCalls = append(r.Message.ToolCalls, minipi.ToolCall{ID: id, Name: "test", Arguments: json.RawMessage(`{"x":1}`)})
	}
	return r
}

func fakeTool(execute func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error)) minipi.Tool {
	return minipi.Tool{Definition: minipi.ToolDefinition{Name: "test", Parameters: json.RawMessage(`{"type":"object"}`)}, Validate: func(json.RawMessage) error { return nil }, Execute: execute}
}

func TestInputContextUsesDurableIDForModelAndTool(t *testing.T) {
	type trustedKey struct{}
	ids := []string{"external-z", "external-a"}
	var restored, modelIDs, toolIDs []string
	turns := make(map[string]int)
	cfg := testConfig(t, modelFunc(func(ctx context.Context, _ minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		id, _ := ctx.Value(trustedKey{}).(string)
		modelIDs = append(modelIDs, id)
		turns[id]++
		if turns[id] == 1 {
			return toolCalls("call-" + id), nil
		}
		return answer("done"), nil
	}))
	cfg.InputContext = func(ctx context.Context, id string) (context.Context, error) {
		if ctx.Value(trustedKey{}) != nil {
			return nil, errors.New("previous input context leaked")
		}
		restored = append(restored, id)
		return context.WithValue(ctx, trustedKey{}, id), nil
	}
	cfg.Agent.Tools = []minipi.Tool{fakeTool(func(ctx context.Context, call minipi.ToolCall, _ func(minipi.ToolResult)) (minipi.ToolResult, error) {
		id, _ := ctx.Value(trustedKey{}).(string)
		toolIDs = append(toolIDs, id)
		if call.ID != "call-"+id {
			return minipi.ToolResult{}, errors.New("tool context mismatch")
		}
		return minipi.ToolResult{Content: "done"}, nil
	})}
	r := openRegion(t, cfg)
	for _, id := range ids {
		receive(t, r, id, "same untrusted text")
	}
	if len(restored) != 0 {
		t.Fatal("context restored during receipt rather than processing")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openRegion(t, cfg)
	cancel, done := startRun(t, r)
	for _, id := range ids {
		waitStatus(t, r, id, StatusSucceeded)
	}
	cancel()
	if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, ids) || !reflect.DeepEqual(toolIDs, ids) ||
		!reflect.DeepEqual(modelIDs, []string{ids[0], ids[0], ids[1], ids[1]}) {
		t.Fatalf("restored=%v models=%v tools=%v", restored, modelIDs, toolIDs)
	}
}

func TestInputContextFailureStopsQueue(t *testing.T) {
	failure := errors.New("snapshot storage failed")
	for _, tc := range []struct {
		name string
		hook func(context.Context, string) (context.Context, error)
		want error
	}{
		{"error", func(context.Context, string) (context.Context, error) { return nil, failure }, failure},
		{"context_and_error", func(ctx context.Context, _ string) (context.Context, error) { return ctx, failure }, failure},
		{"nil", func(context.Context, string) (context.Context, error) { return nil, nil }, ErrInvalid},
		{"panic", func(context.Context, string) (context.Context, error) { panic("secret-panic-payload") }, ErrPanic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var models, tools, restores atomic.Int32
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				models.Add(1)
				return toolCalls("one"), nil
			}))
			cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
				tools.Add(1)
				return minipi.ToolResult{}, nil
			})}
			cfg.InputContext = func(ctx context.Context, id string) (context.Context, error) {
				restores.Add(1)
				return tc.hook(ctx, id)
			}
			r := openRegion(t, cfg)
			receive(t, r, "external-a", "question")
			receive(t, r, "external-b", "future")
			_, done := startRun(t, r)
			if err := awaitRun(t, done); !errors.Is(err, tc.want) {
				t.Fatalf("Run error: %v", err)
			}
			in := waitStatus(t, r, "external-a", StatusFailed)
			if in.CompletedAt.IsZero() || !strings.Contains(in.LastError, tc.want.Error()) || strings.Contains(in.LastError, "secret-panic-payload") {
				t.Fatalf("failure evidence: %+v", in)
			}
			waitStatus(t, r, "external-b", StatusPending)
			if models.Load() != 0 || tools.Load() != 0 || restores.Load() != 1 || len(messages(t, r, "external-a")) != 1 {
				t.Fatal("recovery failure allowed subsequent work")
			}
		})
	}
}

func TestPersistenceBarriersAndStatistics(t *testing.T) {
	var r *Region
	var calls atomic.Int32
	tool := fakeTool(func(ctx context.Context, call minipi.ToolCall, update func(minipi.ToolResult)) (minipi.ToolResult, error) {
		logs, err := r.ListMessages(ctx, "a", 0, 100)
		if err != nil {
			return minipi.ToolResult{}, err
		}
		if len(logs) != 2 || logs[1].Message.Role != "assistant" {
			return minipi.ToolResult{}, errors.New("assistant not committed before tool")
		}
		update(minipi.ToolResult{Content: "not-final"})
		return minipi.ToolResult{Content: "tool output"}, nil
	})
	cfg := testConfig(t, modelFunc(func(ctx context.Context, req minipi.Request, update func(minipi.Message)) (minipi.Response, error) {
		if calls.Add(1) == 1 {
			update(minipi.Message{Role: minipi.RoleAssistant, Content: "partial"})
			return toolCalls("call-a"), nil
		}
		logs, err := r.ListMessages(ctx, "a", 0, 100)
		if err != nil {
			return minipi.Response{}, err
		}
		if len(logs) != 3 || logs[2].Message.Role != "toolResult" || logs[2].Message.Content[0].Type != "tool_result" || logs[2].Message.Content[0].Text != "tool output" {
			return minipi.Response{}, errors.New("tool result not committed before model")
		}
		if len(req.Messages) != 3 || req.Messages[2].Role != minipi.RoleTool {
			return minipi.Response{}, errors.New("wrong model history")
		}
		out := answer("final")
		out.Usage = minipi.Usage{InputTokens: 5, OutputTokens: 7}
		return out, nil
	}))
	cfg.Agent.Tools = []minipi.Tool{tool}
	r = openRegion(t, cfg)
	receive(t, r, "a", "question")
	cancel, done := startRun(t, r)
	in := waitStatus(t, r, "a", StatusSucceeded)
	cancel()
	if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if in.Turns != 2 || in.Usage.InputTokens != 7 || in.Usage.OutputTokens != 10 || in.StopReason != minipi.StopEnd {
		t.Fatalf("statistics: %+v", in)
	}
	logs := messages(t, r, "a")
	if len(logs) != 4 || logs[1].Turn != 1 || logs[3].Turn != 2 {
		t.Fatalf("logs: %+v", logs)
	}
	for _, log := range logs {
		for _, b := range log.Message.Content {
			if b.Text == "partial" || b.Text == "not-final" {
				t.Fatal("persisted streaming update")
			}
		}
	}
}

func TestStorageFailureStopsBeforeSideEffects(t *testing.T) {
	for _, role := range []string{"assistant", "toolResult"} {
		t.Run(role, func(t *testing.T) {
			var models, tools atomic.Int32
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				models.Add(1)
				return toolCalls("one", "two"), nil
			}))
			cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
				tools.Add(1)
				return minipi.ToolResult{Content: "effect done"}, nil
			})}
			r := openRegion(t, cfg)
			receive(t, r, "a", "question")
			receive(t, r, "b", "future")
			query := fmt.Sprintf(`CREATE TRIGGER reject_message BEFORE INSERT ON messages WHEN json_extract(NEW.message,'$.role')='%s' BEGIN SELECT RAISE(ABORT,'injected storage failure'); END`, role)
			if _, err := r.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			_, done := startRun(t, r)
			err := awaitRun(t, done)
			if err == nil || !strings.Contains(err.Error(), "injected storage failure") {
				t.Fatal(err)
			}
			in := waitStatus(t, r, "a", StatusFailed)
			if !strings.Contains(in.LastError, "injected storage failure") {
				t.Fatal(in.LastError)
			}
			want := int32(0)
			logs := 1
			if role == "toolResult" {
				want = 1
				logs = 2
			}
			if tools.Load() != want || models.Load() != 1 {
				t.Fatalf("models=%d tools=%d", models.Load(), tools.Load())
			}
			if len(messages(t, r, "a")) != logs {
				t.Fatal("unexpected evidence")
			}
			pending, err := r.Get(context.Background(), "b")
			if err != nil || pending.Status != StatusPending {
				t.Fatalf("queue continued: %+v %v", pending, err)
			}
		})
	}
}

func TestHistoryIsolationAndFailedEvidence(t *testing.T) {
	var requests [][]string
	cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, update func(minipi.Message)) (minipi.Response, error) {
		var contents []string
		for _, m := range req.Messages {
			contents = append(contents, m.Content)
		}
		requests = append(requests, contents)
		switch req.Messages[len(req.Messages)-1].Content {
		case "first":
			return answer("first answer"), nil
		case "bad":
			update(minipi.Message{Role: minipi.RoleAssistant, Content: "stream only"})
			return minipi.Response{}, errors.New("model unavailable")
		case "cut":
			out := answer("truncated")
			out.StopReason = minipi.StopLength
			return out, nil
		default:
			return answer("last answer"), nil
		}
	}))
	r := openRegion(t, cfg)
	for _, id := range []string{"first", "bad", "cut", "last"} {
		receive(t, r, id, id)
	}
	cancel, done := startRun(t, r)
	waitStatus(t, r, "last", StatusSucceeded)
	cancel()
	awaitRun(t, done)
	want := [][]string{{"first"}, {"first", "first answer", "bad"}, {"first", "first answer", "cut"}, {"first", "first answer", "last"}}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("history: %#v", requests)
	}
	waitStatus(t, r, "bad", StatusFailed)
	waitStatus(t, r, "cut", StatusFailed)
	for _, log := range messages(t, r, "bad") {
		for _, b := range log.Message.Content {
			if b.Text == "stream only" {
				t.Fatal("update promoted to final")
			}
		}
	}
	if len(messages(t, r, "cut")) != 2 {
		t.Fatal("lost truncated final evidence")
	}
}

func TestMalformedArgumentsAndInvalidResponseEvidence(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"x":`), []byte(`[]`), {0xff, '{'}} {
		t.Run(fmt.Sprintf("%x", raw), func(t *testing.T) {
			var models, tools atomic.Int32
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				if models.Add(1) == 1 {
					out := toolCalls("raw")
					out.Message.ToolCalls[0].Arguments = raw
					return out, nil
				}
				return answer("handled"), nil
			}))
			cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
				tools.Add(1)
				return minipi.ToolResult{}, nil
			})}
			r := openRegion(t, cfg)
			receive(t, r, "a", "question")
			cancel, done := startRun(t, r)
			waitStatus(t, r, "a", StatusSucceeded)
			cancel()
			awaitRun(t, done)
			logs := messages(t, r, "a")
			b := logs[1].Message.Content[1]
			if !reflect.DeepEqual(b.ArgumentsRaw, raw) || b.Diagnostic == "" || !logs[2].Message.IsError || tools.Load() != 0 {
				t.Fatalf("lost invalid evidence: %+v", logs)
			}
		})
	}
	t.Run("invalid-role", func(t *testing.T) {
		cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
			out := answer("evidence")
			out.Message.Role = "invalid"
			return out, nil
		}))
		r := openRegion(t, cfg)
		receive(t, r, "a", "question")
		cancel, done := startRun(t, r)
		waitStatus(t, r, "a", StatusFailed)
		cancel()
		awaitRun(t, done)
		if logs := messages(t, r, "a"); len(logs) != 2 || logs[1].Message.Role != "invalid" || logs[1].Error == "" {
			t.Fatalf("lost invalid response: %+v", logs)
		}
	})
}

func TestExternalPanicsFailSafelyAndContinue(t *testing.T) {
	for _, where := range []string{"model", "validator", "tool"} {
		t.Run(where, func(t *testing.T) {
			var models, tools atomic.Int32
			cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
				models.Add(1)
				if req.Messages[len(req.Messages)-1].Content == "next" {
					return answer("ok"), nil
				}
				if where == "model" {
					panic("model panic")
				}
				return toolCalls("one", "two"), nil
			}))
			tool := fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
				tools.Add(1)
				panic("tool panic")
			})
			if where == "validator" {
				tool.Validate = func(json.RawMessage) error { panic("validator panic") }
			}
			cfg.Agent.Tools = []minipi.Tool{tool}
			r := openRegion(t, cfg)
			receive(t, r, "a", "bad")
			receive(t, r, "b", "next")
			cancel, done := startRun(t, r)
			waitStatus(t, r, "b", StatusSucceeded)
			cancel()
			awaitRun(t, done)
			in := waitStatus(t, r, "a", StatusFailed)
			if !strings.Contains(in.LastError, ErrPanic.Error()) || models.Load() != 2 {
				t.Fatalf("panic lost: %+v calls=%d", in, models.Load())
			}
			want := int32(0)
			if where == "tool" {
				want = 1
			}
			if tools.Load() != want {
				t.Fatalf("unexpected tools=%d", tools.Load())
			}
		})
	}
}

func TestTruncatedToolResponseStopsBeforeNextTurn(t *testing.T) {
	var calls, tools atomic.Int32
	cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		calls.Add(1)
		if req.Messages[len(req.Messages)-1].Content == "next" {
			if len(req.Messages) != 1 {
				return minipi.Response{}, errors.New("truncated history included")
			}
			return answer("ok"), nil
		}
		out := toolCalls("cut")
		out.StopReason = minipi.StopLength
		return out, nil
	}))
	cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
		tools.Add(1)
		return minipi.ToolResult{}, nil
	})}
	r := openRegion(t, cfg)
	receive(t, r, "a", "cut")
	receive(t, r, "b", "next")
	cancel, done := startRun(t, r)
	waitStatus(t, r, "b", StatusSucceeded)
	cancel()
	awaitRun(t, done)
	waitStatus(t, r, "a", StatusFailed)
	if calls.Load() != 2 || tools.Load() != 0 {
		t.Fatalf("models=%d tools=%d", calls.Load(), tools.Load())
	}
}

func TestHistoryMessageLimit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		limit, tasks, want int
	}{
		{"default", 0, 501, 999},
		{"exact", 5, 3, 5},
		{"partial_task", 4, 3, 3},
		{"current_only", 1, 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				return answer("unused"), nil
			}))
			cfg.MaxHistoryMessages = tc.limit
			r := openRegion(t, cfg)
			// Seed many complete tasks cheaply, including excluded failed evidence.
			tx, err := r.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for i := 0; i <= tc.tasks; i++ {
				id, status := fmt.Sprint(i), "succeeded"
				if i == tc.tasks {
					status = "failed"
				}
				if _, err = tx.Exec(`INSERT INTO inputs(id,text,status,received_at) VALUES(?,?,?,0)`, id, id, status); err != nil {
					t.Fatal(err)
				}
				for turn, role := range []minipi.Role{minipi.RoleUser, minipi.RoleAssistant} {
					raw, err := json.Marshal(encodeMessage(minipi.Message{Role: role, Content: id}))
					if err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Exec(`INSERT INTO messages(input_id,message,turn,input_tokens,output_tokens,stop_reason,error,created_at) VALUES(?,?,?,0,0,'','',0)`, id, raw, turn); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			receive(t, r, "current", "current")
			receive(t, r, "future", "future")
			in, err := r.Get(ctx, "current")
			if err != nil {
				t.Fatal(err)
			}
			history, err := r.history(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if len(history.Messages) != tc.want {
				t.Fatalf("got %d messages, want %d", len(history.Messages), tc.want)
			}
			start := tc.tasks - (tc.want-1)/2
			for i, m := range history.Messages[:tc.want-1] {
				wantRole := minipi.RoleUser
				if i%2 == 1 {
					wantRole = minipi.RoleAssistant
				}
				if m.Content != fmt.Sprint(start+i/2) || m.Role != wantRole {
					t.Fatalf("unexpected message %d: %+v", i, m)
				}
			}
			if history.Messages[tc.want-1].Content != "current" {
				t.Fatal("current input missing")
			}
			if len(messages(t, r, "0")) != 2 {
				t.Fatal("older evidence removed")
			}
		})
	}
}

func TestHistoryLimitSelectsWholeTaskBeforeFilteringTools(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return answer("unused"), nil
	}))
	r := openRegion(t, cfg)
	receive(t, r, "tool-task", "question")
	for _, m := range []minipi.Message{
		{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "call", Name: "tool", Arguments: json.RawMessage(`{}`)}}},
		{Role: minipi.RoleTool, ToolCallID: "call", Content: "result"},
		{Role: minipi.RoleAssistant, Content: "done"},
	} {
		if err := r.appendEvent(ctx, "tool-task", minipi.Event{Message: m, Turn: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.db.Exec(`UPDATE inputs SET status='succeeded' WHERE id='tool-task'`); err != nil {
		t.Fatal(err)
	}
	receive(t, r, "current", "current")
	in, err := r.Get(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{5, 4, 3, 2, 1} {
		r.cfg.MaxHistoryMessages = limit
		history, err := r.history(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if limit == 5 {
			want = 3
		}
		if len(history.Messages) != want {
			t.Fatalf("limit %d: got %d", limit, len(history.Messages))
		}
		expected := []minipi.Message{{Role: minipi.RoleUser, Content: "current"}}
		if limit == 5 {
			expected = append([]minipi.Message{
				{Role: minipi.RoleUser, Content: "question"},
				{Role: minipi.RoleAssistant, Content: "done"},
			}, expected...)
		}
		if !reflect.DeepEqual(history.Messages, expected) {
			t.Fatalf("limit %d: unexpected filtered history: %+v", limit, history.Messages)
		}
	}
	if len(messages(t, r, "tool-task")) != 4 {
		t.Fatal("tool evidence removed")
	}
	cfg.MaxHistoryMessages = -1
	if _, err := cfg.normalized(); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative limit accepted")
	}
}

func TestContextCapacityFailsWithoutTruncation(t *testing.T) {
	var calls atomic.Int32
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		calls.Add(1)
		return answer(strings.Repeat("x", 200)), nil
	}))
	cfg.MaxContextBytes = 180
	r := openRegion(t, cfg)
	receive(t, r, "a", "first")
	receive(t, r, "b", "second")
	cancel, done := startRun(t, r)
	in := waitStatus(t, r, "b", StatusFailed)
	cancel()
	awaitRun(t, done)
	if calls.Load() != 1 || !strings.Contains(in.LastError, ErrContextCapacity.Error()) {
		t.Fatalf("capacity: %+v calls=%d", in, calls.Load())
	}
	if len(messages(t, r, "a")) != 2 {
		t.Fatal("history was truncated")
	}
}
