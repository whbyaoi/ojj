package agenttools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replypolicy"
)

type senderFunc func(context.Context, qq.TextMessage) (qq.SentMessage, error)

func (f senderFunc) SendText(ctx context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
	return f(ctx, msg)
}

func TestQQLogsOmitSensitiveData(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	for _, failure := range []error{nil, context.Canceled, errors.Join(context.Canceled, errors.New("private-send-error"))} {
		buf.Reset()
		tool, err := NewQQSend(senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
			return qq.SentMessage{ID: "private-sent-id", Timestamp: "private-time"}, failure
		}))
		if err != nil {
			t.Fatal("tool construction failed")
		}
		result, err := tool.Execute(context.Background(), minipi.ToolCall{ID: "private-call", Name: "private-tool", Arguments: json.RawMessage(`{"target_type":"private","target_id":"private-platform-id","content":"private-body","msg_id":"private-reply-id"}`)}, nil)
		if (err != nil) != (failure != nil) || result.IsError != (failure != nil) {
			t.Fatal("changed send outcome")
		}
		if strings.Contains(buf.String(), "private-") {
			t.Fatal("sensitive send data logged")
		}
		var record map[string]any
		if json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record) != nil {
			t.Fatal("invalid send log")
		}
		want := "INFO"
		if failure == context.Canceled {
			want = "DEBUG"
		} else if failure != nil {
			want = "ERROR"
		}
		if record["level"] != want || record["tool"] != "qq_send_text" || record["component"] != "agenttools" {
			t.Fatal("incorrect send classification")
		}
	}
}

func TestQQLogStages(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	for _, tc := range []struct {
		name, id, stage, level string
		failure                error
	}{
		{"skipped", "", "send_skipped", "INFO", replypolicy.ErrSkipped},
		{"wrapped-skip", "", "send_skipped", "INFO", fmt.Errorf("private-error: %w", replypolicy.ErrSkipped)},
		{"ack", "private-ack", "send_ack", "INFO", nil},
		{"ack-storage", "private-ack", "send_ack", "ERROR", replypolicy.ErrStorage},
		{"ack-canceled", "private-ack", "send_ack", "DEBUG", context.Canceled},
		{"unknown", "", "send", "ERROR", errors.New("private-transport-error")},
		{"missing-ack", "", "send", "ERROR", nil},
		{"canceled-skip", "", "send", "ERROR", errors.Join(replypolicy.ErrSkipped, context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf.Reset()
			calls := 0
			tool, err := NewQQSend(senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				calls++
				return qq.SentMessage{ID: tc.id}, tc.failure
			}))
			if err != nil {
				t.Fatal("tool construction failed")
			}
			const inputID = "0123456789abcdef0123456789abcdef"
			ctx := minipi.WithInputID(context.Background(), inputID)
			result, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`{"target_type":"private","target_id":"private-target","content":"private-body","msg_id":"private-message"}`)}, nil)
			if calls != 1 {
				t.Fatal("diagnostics changed send count")
			}
			wantError := tc.level != "INFO"
			if (err != nil) != wantError || result.IsError != wantError {
				t.Fatal("diagnostics changed result semantics")
			}
			if tc.failure == replypolicy.ErrStorage && err != tc.failure {
				t.Fatal("ACK lost precise storage sentinel")
			}
			var record map[string]any
			if json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record) != nil {
				t.Fatal("invalid diagnostic record")
			}
			if record["stage"] != tc.stage || record["level"] != tc.level || record["input_id"] != inputID {
				t.Fatalf("incorrect diagnostic classification: %v", record)
			}
			if strings.Contains(buf.String(), "private-") {
				t.Fatal("sensitive send data logged")
			}
		})
	}
}

func TestQQTool(t *testing.T) {
	var got []qq.TextMessage
	tool, err := NewQQSend(senderFunc(func(_ context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
		got = append(got, msg)
		return qq.SentMessage{ID: "sent", Timestamp: "timestamp"}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(tool.Definition.Parameters) || tool.Definition.Name != "qq_send_text" {
		t.Fatalf("invalid definition: %+v", tool.Definition)
	}
	var schema struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			MinLength   int    `json:"minLength"`
			Description string `json:"description"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(tool.Definition.Parameters, &schema); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(schema.Required, ","), "msg_id") || schema.Properties["msg_id"].MinLength != 1 {
		t.Fatal("current input msg_id must be required and nonempty")
	}
	for _, text := range []string{"宿主当前输入", "每条原消息最多回复一条", "模型理由不可信", "不重试"} {
		if !strings.Contains(tool.Definition.Description, text) {
			t.Fatalf("missing host policy guidance: %s", text)
		}
	}
	if !strings.Contains(schema.Properties["msg_id"].Description, "宿主当前输入") || !strings.Contains(schema.Properties["msg_seq"].Description, "不得换序号补发") {
		t.Fatal("parameter guidance permits unbound sends or retries")
	}
	// The generic parser remains compatible; the host sender enforces authorization.
	for _, msg := range []qq.TextMessage{
		{TargetType: "private", TargetID: "user_1", Content: "你好"},
		{TargetType: "group", TargetID: "group-2", Content: "群消息", MsgID: "original"},
		{TargetType: "group", TargetID: "group-2", Content: "第二条", MsgID: "original", MsgSeq: 2},
	} {
		raw, _ := json.Marshal(msg)
		if err := tool.Validate(raw); err != nil {
			t.Fatal(err)
		}
		result, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: raw}, nil)
		if err != nil || result.IsError || result.Terminate {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		var sent qq.SentMessage
		if err := json.Unmarshal([]byte(result.Content), &sent); err != nil || sent.ID != "sent" || sent.Timestamp != "timestamp" {
			t.Fatalf("invalid result: %+v %v", sent, err)
		}
		if msg.MsgID != "" && msg.MsgSeq == 0 {
			msg.MsgSeq = 1
		}
		if got[len(got)-1] != msg {
			t.Fatalf("sent=%+v want=%+v", got[len(got)-1], msg)
		}
	}
	if len(got) != 3 {
		t.Fatalf("unexpected send count: %d", len(got))
	}
	for _, raw := range []string{
		`{}`, `null`, `{"target_type":"channel","target_id":"id","content":"text"}`,
		`{"target_type":"private","target_id":"../escape","content":"text"}`,
		`{"target_type":"private","target_id":"id","content":" "}`,
		`{"target_type":"private","target_id":"id","content":"text","msg_seq":1}`,
		`{"target_type":"private","target_id":"id","content":"text","msg_seq":-1}`,
		`{"target_type":"private","target_id":"id","content":"text","msg_seq":4294967296}`,
		`{"target_type":"private","target_id":"id","content":"text","msg_id":null}`,
		`{"target_type":"private","target_id":"id","content":"text","CONTENT":"override"}`,
		`{"target_type":"private","target_id":"id","target_id":"other","content":"text"}`,
	} {
		if err := tool.Validate(json.RawMessage(raw)); err == nil {
			t.Errorf("validation accepted %q", raw)
		}
		if _, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(raw)}, nil); err == nil {
			t.Errorf("execute accepted %q", raw)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`{"target_type":"private","target_id":"id","content":"text"}`)}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancel: %v", err)
	}
	if len(got) != 3 {
		t.Fatal("invalid/canceled calls caused network side effects")
	}
}

func TestQQToolSkipped(t *testing.T) {
	const skipped = "QQ 回复已跳过，未发送；不排队、不重试，也不要换目标或序号补发。"
	for _, mode := range []string{"skip", "wrapped", "cancel", "wrapped-cancel", "deadline", "cancel-ack", "pre-cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			tool, err := NewQQSend(senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				calls++
				switch mode {
				case "wrapped":
					return qq.SentMessage{}, fmt.Errorf("wrapped: %w", replypolicy.ErrSkipped)
				case "cancel":
					cancel()
				case "wrapped-cancel":
					return qq.SentMessage{}, errors.Join(replypolicy.ErrSkipped, context.Canceled)
				case "deadline":
					return qq.SentMessage{}, errors.Join(replypolicy.ErrSkipped, context.DeadlineExceeded)
				case "cancel-ack":
					cancel()
					return qq.SentMessage{ID: "sent"}, nil
				}
				return qq.SentMessage{}, replypolicy.ErrSkipped
			}))
			if err != nil {
				t.Fatal(err)
			}
			args := json.RawMessage(`{"target_type":"private","target_id":"id","content":"text","msg_id":"original"}`)
			if mode == "pre-cancel" {
				cancel()
				args = json.RawMessage(`{}`)
			}
			result, err := tool.Execute(ctx, minipi.ToolCall{Arguments: args}, nil)
			if mode == "skip" || mode == "wrapped" {
				if err != nil || result.IsError || result.Terminate || result.Content != skipped {
					t.Fatalf("skip result=%+v err=%v", result, err)
				}
			} else {
				want := context.Canceled
				if mode == "deadline" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) || result.Content == skipped || result.Terminate || (mode != "pre-cancel" && !result.IsError) {
					t.Fatalf("cancellation result=%+v err=%v", result, err)
				}
			}
			wantCalls := 1
			if mode == "pre-cancel" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("calls=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestQQToolErrors(t *testing.T) {
	for _, sender := range []QQSender{nil, (*qq.Client)(nil), senderFunc(nil)} {
		if _, err := NewQQSend(sender); err == nil {
			t.Fatal("accepted nil sender")
		}
	}
	failure := errors.New("send failure")
	for _, mode := range []string{"error", "cancel", "empty"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			tool, err := NewQQSend(senderFunc(func(gotCtx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
				calls++
				if gotCtx != ctx {
					t.Fatal("context not forwarded")
				}
				if mode == "cancel" {
					cancel()
				}
				if mode == "empty" {
					return qq.SentMessage{}, nil
				}
				return qq.SentMessage{}, failure
			}))
			if err != nil {
				t.Fatal(err)
			}
			result, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`{"target_type":"private","target_id":"id","content":"text"}`)}, nil)
			if err == nil || !result.IsError || result.Terminate || calls != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
			if mode != "empty" && !errors.Is(err, failure) {
				t.Fatalf("lost original error: %v", err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if mode == "empty" && !errors.Is(err, qq.ErrProtocol) {
				t.Fatalf("lost protocol error: %v", err)
			}
		})
	}
}
