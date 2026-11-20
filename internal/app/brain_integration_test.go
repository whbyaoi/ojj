package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/brain"
	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/messagebuilder"
)

func TestBrainAdmissionFailureStopsApplication(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint("late=", late), func(t *testing.T) {
			brainTestEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			store, err := event.Open(ctx, event.Config{Path: filepath.Join(dir, "events.db"), PollInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			builder, err := messagebuilder.Open(ctx, messagebuilder.Config{Path: filepath.Join(dir, "messages.db"), QuietPeriod: time.Millisecond, PollInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
			cfg, _, err := mainBrainConfig(filepath.Join(dir, "events.db"))
			if err != nil {
				t.Fatal(err)
			}
			cfg.MaxStoredBytes = 1
			region, err := brain.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer region.Close()
			producer, err := store.BindProducer("test")
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{}, 1)
			var input messagebuilder.Message
			handler := func(call context.Context, msg messagebuilder.Message) error {
				input = msg
				select {
				case entered <- struct{}{}:
				default:
				}
				if late {
					<-call.Done()
					// Let the lifecycle monitor exit before returning the real failure.
					time.Sleep(20 * time.Millisecond)
					return errors.Join(call.Err(), brain.ErrCapacity)
				}
				return region.Handle(call, msg)
			}
			err = runWithMessages(ctx, store, builder, handler, func(child context.Context) error {
				if _, err := producer.Publish(child, event.Input{EventType: "test.message", OccurredAt: time.Now(), Content: "payload"}); err != nil {
					return err
				}
				if late {
					select {
					case <-entered:
						return nil
					case <-child.Done():
						return child.Err()
					}
				}
				<-child.Done()
				return child.Err()
			})
			if !errors.Is(err, brain.ErrCapacity) {
				t.Fatal("lost admission failure", err)
			}
			persisted, err := builder.Get(ctx, input.ID)
			if err != nil || persisted.Status != messagebuilder.StatusRetry {
				t.Fatal("rejected input acknowledged", err, persisted.Status)
			}
			if _, err := region.Get(ctx, input.ID); !errors.Is(err, brain.ErrNotFound) {
				t.Fatal("rejected input stored in brain", err)
			}
		})
	}
}

type checkedQQSender func(context.Context, qq.TextMessage) (qq.SentMessage, error)

func (f checkedQQSender) SendText(ctx context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
	return f(ctx, msg)
}

func TestMainBrainEndToEnd(t *testing.T) {
	for _, protocol := range []string{"", "openai", "anthropic"} {
		t.Run("protocol="+protocol, func(t *testing.T) { testMainBrainEndToEnd(t, protocol) })
	}
}

func testMainBrainEndToEnd(t *testing.T, protocol string) {
	brainTestEnv(t)
	t.Setenv("OJJ_MODEL_PROTOCOL", protocol)
	if protocol == "anthropic" {
		t.Setenv("ANTHROPIC_API_KEY", "fake-test-key")
		t.Setenv("ANTHROPIC_MODEL", "test-model")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	store, err := event.Open(ctx, event.Config{Path: filepath.Join(dir, "events.db"), PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	builder, err := messagebuilder.Open(ctx, messagebuilder.Config{Path: filepath.Join(dir, "messages.db"), QuietPeriod: time.Millisecond, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if protocol != "anthropic" {
			var body struct {
				Messages []struct {
					Role       string  `json:"role"`
					Content    *string `json:"content"`
					ToolCallID string  `json:"tool_call_id"`
					ToolCalls  []struct {
						ID string `json:"id"`
					} `json:"tool_calls"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer fake-test-key" || r.Header.Get("x-api-key") != "" {
				t.Error("incorrect OpenAI endpoint or credential")
			}
			if len(body.Messages) == 0 || body.Messages[0].Role != "system" || body.Messages[0].Content == nil || *body.Messages[0].Content != testSystemPrompt {
				t.Error("missing host rules or portrait in OpenAI request")
			}
			w.Header().Set("Content-Type", "application/json")
			switch requests.Add(1) {
			case 1:
				if len(body.Messages) != 2 || body.Messages[1].Role != "user" {
					t.Error("missing system/user messages")
				}
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"send-1","type":"function","function":{"name":"qq_send_text","arguments":"{\"target_type\":\"group\",\"target_id\":\"user_1\",\"content\":\"hello\",\"msg_id\":\"original\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`)
			case 2:
				if len(body.Messages) != 4 || body.Messages[2].Role != "assistant" || len(body.Messages[2].ToolCalls) != 1 || body.Messages[2].ToolCalls[0].ID != "send-1" || body.Messages[3].Role != "tool" || body.Messages[3].ToolCallID != "send-1" || body.Messages[3].Content == nil || !strings.Contains(*body.Messages[3].Content, "sent-1") {
					t.Error("missing OpenAI tool result")
				}
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":2}}`)
			default:
				t.Error("unexpected OpenAI model replay")
				w.WriteHeader(500)
			}
			return
		}
		var body struct {
			System   string `json:"system"`
			Messages []struct {
				Role    string `json:"role"`
				Content []struct {
					Type      string `json:"type"`
					ToolUseID string `json:"tool_use_id"`
					Content   string `json:"content"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if body.System != testSystemPrompt {
			t.Error("missing host rules or portrait in Anthropic request")
		}
		if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "fake-test-key" {
			t.Error("incorrect model endpoint or credential")
		}
		for _, msg := range body.Messages {
			if msg.Role != "user" && msg.Role != "assistant" {
				t.Errorf("invalid wire role %s", msg.Role)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		switch requests.Add(1) {
		case 1:
			fmt.Fprint(w, `{"type":"message","role":"assistant","content":[{"type":"tool_use","id":"send-1","name":"qq_send_text","input":{"target_type":"group","target_id":"user_1","content":"hello","msg_id":"original"}}],"stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":5}}`)
		case 2:
			if len(body.Messages) != 3 || body.Messages[2].Role != "user" || len(body.Messages[2].Content) != 1 || body.Messages[2].Content[0].Type != "tool_result" || body.Messages[2].Content[0].ToolUseID != "send-1" || !strings.Contains(body.Messages[2].Content[0].Content, "sent-1") {
				t.Error("missing standard user/tool_result block")
			}
			fmt.Fprint(w, `{"type":"message","role":"assistant","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":20,"output_tokens":2}}`)
		default:
			t.Error("unexpected model replay")
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	if protocol == "anthropic" {
		t.Setenv("ANTHROPIC_BASE_URL", server.URL)
	} else {
		t.Setenv("OPENAI_BASE_URL", server.URL)
	}
	t.Setenv("OJJ_QQ_SEND_ALLOWLIST", "group:user_1")
	cfg, allowed, err := mainBrainConfig(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.PollInterval = time.Millisecond
	var region *brain.Region
	var sends atomic.Int32
	checked := checkedQQSender(func(call context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
		messages, err := builder.List(call, messagebuilder.MessageQuery{})
		if err != nil || len(messages) != 1 {
			return qq.SentMessage{}, errors.New("missing builder input before send")
		}
		log, err := region.ListMessages(call, messages[0].ID, 0, 100)
		if err != nil || len(log) != 2 || log[1].Message.Role != "assistant" || len(log[1].Message.Content) != 2 || log[1].Message.Content[1].ID != "send-1" {
			return qq.SentMessage{}, errors.New("tool executed before durable assistant call")
		}
		if msg.TargetType != "group" || msg.TargetID != "user_1" || msg.MsgID != "original" || msg.MsgSeq != 1 || msg.Content != "hello" {
			return qq.SentMessage{}, errors.New("unexpected QQ group message")
		}
		sends.Add(1)
		return qq.SentMessage{ID: "sent-1"}, nil
	})
	pc, err := replyPolicyConfig(filepath.Join(dir, "events.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := attachReplyPolicy(ctx, &cfg, builder, pc, checked, allowed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { policy.Close() }()
	region, err = brain.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { region.Close() }()
	producer, err := store.BindProducer("qq.websocket")
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("verified")
	var input messagebuilder.Message
	err = runWithMessages(ctx, store, builder, region.Handle, region.Run, func(child context.Context) error {
		converted, err := qq.Convert("test", "GROUP_AT_MESSAGE_CREATE", []byte(fmt.Sprintf(`{"id":"original","group_openid":"user_1","author":{"member_openid":"member_1"},"content":"请回复我","timestamp":%q}`, time.Now().UTC().Format(time.RFC3339Nano))))
		if err != nil {
			return err
		}
		if _, err := producer.Publish(child, converted); err != nil {
			return err
		}
		for child.Err() == nil {
			messages, err := builder.List(child, messagebuilder.MessageQuery{Status: messagebuilder.StatusSucceeded})
			if err != nil {
				return err
			}
			if len(messages) == 1 {
				input = messages[0]
				state, err := region.Get(child, input.ID)
				if err != nil {
					return err
				}
				if state.Status == brain.StatusSucceeded {
					return stop
				}
				if state.Status == brain.StatusFailed {
					return fmt.Errorf("brain failed: %s", state.LastError)
				}
			}
			time.Sleep(time.Millisecond)
		}
		return child.Err()
	})
	if !errors.Is(err, stop) || requests.Load() != 2 || sends.Load() != 1 {
		t.Fatalf("pipeline: %v, model=%d sends=%d", err, requests.Load(), sends.Load())
	}
	log, err := region.ListMessages(ctx, input.ID, 0, 100)
	if err != nil || len(log) != 4 {
		t.Fatalf("incomplete log: %v %v", log, err)
	}
	for i, role := range []string{"user", "assistant", "toolResult", "assistant"} {
		if log[i].Message.Role != role {
			t.Fatalf("log %d role %s", i, log[i].Message.Role)
		}
	}
	if log[0].Message.Content[0].Text != input.Text || log[2].Message.ToolCallID != "send-1" || log[3].Message.Content[0].Text != "done" {
		t.Fatal("message content not preserved")
	}
	if err := region.Close(); err != nil {
		t.Fatal(err)
	}
	if err := policy.Close(); err != nil {
		t.Fatal(err)
	}
	// Rebuild the configuration instead of appending another tool on restart.
	cfg, allowed, err = mainBrainConfig(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.PollInterval = time.Millisecond
	policy, err = attachReplyPolicy(ctx, &cfg, builder, pc, checked, allowed)
	if err != nil {
		t.Fatal(err)
	}
	region, err = brain.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := region.Handle(ctx, input); err != nil {
		t.Fatal(err)
	}
	replayCtx, replayCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer replayCancel()
	if err := region.Run(replayCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	log, err = region.ListMessages(ctx, input.ID, 0, 100)
	if err != nil || len(log) != 4 || requests.Load() != 2 || sends.Load() != 1 {
		t.Fatal("duplicate input replayed after reopen", err)
	}
}
