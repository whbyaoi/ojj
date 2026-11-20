package app

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"ojj/internal/brain"
	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
)

type deliveryModelFunc func(context.Context, minipi.Request) (minipi.Response, error)

func (f deliveryModelFunc) Generate(ctx context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
	return f(ctx, req)
}

// runPrecheckProduct waits for durable completion and joins the worker before
// callers inspect model/sender counters or reopen the temporary stores.
func runPrecheckProduct(t *testing.T, f *replyIntegrationFixture, r *brain.Region, product messagebuilder.Message) brain.Input {
	t.Helper()
	if err := r.Handle(f.ctx, product); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(brain.WithModelSkipped(f.ctx, true))
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	for ctx.Err() == nil {
		in, err := r.Get(ctx, product.ID)
		if err != nil {
			t.Fatal(err)
		}
		if in.Status == brain.StatusSucceeded {
			return in
		}
		if in.Status == brain.StatusFailed || in.Status == brain.StatusInterrupted {
			t.Fatalf("unexpected terminal status: %s", in.Status)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(ctx.Err())
	return brain.Input{}
}

func TestReplyPrecheckProductionWindows(t *testing.T) {
	for _, name := range []string{"total_blocked", "active_blocked", "direct_bypasses_active", "earlier_direct_send", "other_group", "private", "heartbeat", "mixed_heartbeat", "mixed_other_task"} {
		t.Run(name, func(t *testing.T) {
			limit := 1
			if name == "active_blocked" || name == "direct_bypasses_active" || name == "earlier_direct_send" {
				limit = 12
			}
			f := newReplyIntegrationFixture(t, limit, nil)
			seed := f.group("GROUP_MESSAGE_CREATE", "group_1", "seed", "普通群聊")
			f.sent(f.bind(f.ctx, seed), "group_1", "seed")
			type source struct {
				producer string
				input    event.Input
			}
			chat := func(kind, group, id string) source {
				raw, err := json.Marshal(map[string]any{
					"id": id, "group_openid": group,
					"author":  map[string]string{"member_openid": "member_1", "user_openid": "private_user"},
					"content": "待处理消息", "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
				})
				if err != nil {
					t.Fatal(err)
				}
				in, err := qq.Convert("test", kind, raw)
				if err != nil {
					t.Fatal(err)
				}
				return source{"qq.websocket", in}
			}
			sources := []source{chat("GROUP_MESSAGE_CREATE", "group_1", "blocked")}
			task := source{"system.heartbeat", event.Input{EventType: "system.heartbeat", SchemaVersion: 1, OccurredAt: time.Now(), Content: `{"version":1,"reason":"idle"}`}}
			switch name {
			case "total_blocked":
				sources = append(sources, chat("GROUP_AT_MESSAGE_CREATE", "group_1", "blocked-direct"))
			case "direct_bypasses_active":
				sources = append(sources, chat("GROUP_AT_MESSAGE_CREATE", "group_1", "direct"))
			case "earlier_direct_send":
				sources = []source{chat("GROUP_AT_MESSAGE_CREATE", "group_1", "early-direct"), chat("GROUP_MESSAGE_CREATE", "group_1", "later-active")}
			case "other_group":
				sources = append(sources, chat("GROUP_MESSAGE_CREATE", "group_2", "other"))
			case "private":
				sources = append(sources, chat("C2C_MESSAGE_CREATE", "", "private"))
			case "heartbeat":
				sources = []source{task}
			case "mixed_heartbeat":
				sources = append(sources, task)
			case "mixed_other_task":
				task.producer, task.input.EventType = "system.task", "system.task"
				sources = append(sources, task)
			}
			// Capacity seals synchronously, avoiding timing-dependent mixed windows.
			f.bc.QuietPeriod, f.bc.MaxWindow = time.Hour, time.Hour
			f.bc.MaxWindowEvents = len(sources)
			f.reopen()
			var ids []string
			for _, src := range sources {
				producer, err := f.store.BindProducer(src.producer)
				if err != nil {
					t.Fatal(err)
				}
				published, err := producer.Publish(f.ctx, src.input)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.builder.Handle(f.ctx, published.Event); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, published.Event.ID)
			}
			products, err := f.builder.List(f.ctx, messagebuilder.MessageQuery{AfterSequence: f.seq})
			if err != nil || len(products) != 1 || !reflect.DeepEqual(products[0].EventIDs, ids) {
				t.Fatal("mixed window was not preserved", err)
			}
			f.seq = products[0].Sequence
			if name == "earlier_direct_send" {
				verifyEarlierWindowDelivery(t, f, products[0])
			} else {
				verifyPrecheckWindow(t, f, products[0], name == "total_blocked" || name == "active_blocked")
			}
		})
	}
}

func verifyEarlierWindowDelivery(t *testing.T, f *replyIntegrationFixture, product messagebuilder.Message) {
	t.Helper()
	phase := 0
	f.cfg.PollInterval = time.Millisecond
	f.cfg.Agent.Model = deliveryModelFunc(func(ctx context.Context, req minipi.Request) (minipi.Response, error) {
		defer func() { phase++ }()
		want := replydelivery.NotSent
		if phase >= 2 {
			want = replydelivery.Sent
		}
		if replydelivery.Outcome(ctx) != want {
			t.Error("window delivery tracking mismatch")
		}
		if phase < 3 {
			// The later active message is blocked. The earlier direct message
			// remains selectable; repeating it cannot consume a second send.
			id := "early-direct"
			if phase == 0 {
				id = "later-active"
			}
			raw, err := json.Marshal(qq.TextMessage{TargetType: "group", TargetID: "group_1", MsgID: id, Content: "hello", MsgSeq: uint32(phase + 1)})
			if err != nil {
				return minipi.Response{}, err
			}
			return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: []string{"later-active", "early-direct", "early-direct-repeat"}[phase], Name: "qq_send_text", Arguments: raw}}}, StopReason: minipi.StopToolUse}, nil
		}
		return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, Content: "done"}, StopReason: minipi.StopEnd}, nil
	})
	r, err := brain.Open(f.ctx, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	runPrecheckProduct(t, f, r, product)
	if phase != 4 || len(f.calls) != 2 || f.calls[1].MsgID != "early-direct" || f.calls[1].TargetID != "group_1" {
		t.Fatal("earlier window message was not delivered exactly once")
	}
}

func verifyPrecheckWindow(t *testing.T, f *replyIntegrationFixture, product messagebuilder.Message, skipped bool) {
	t.Helper()
	calls := 0
	model := deliveryModelFunc(func(ctx context.Context, req minipi.Request) (minipi.Response, error) {
		calls++
		if replydelivery.Outcome(ctx) != replydelivery.NotSent {
			t.Error("missing fresh delivery tracker")
		}
		if skipped && (len(req.Messages) != 2 || req.Messages[0].Role != minipi.RoleUser || req.Messages[1].Role != minipi.RoleUser) {
			t.Error("skipped input missing from later history")
		}
		return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, Content: "done"}, StopReason: minipi.StopEnd}, nil
	})
	f.cfg.Agent.Model, f.cfg.PollInterval = model, time.Millisecond
	r, err := brain.Open(f.ctx, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r.Close() }()
	in := runPrecheckProduct(t, f, r, product)
	wantCalls := 1
	if skipped {
		wantCalls = 0
		if in.Turns != 0 || in.Usage != (brain.Usage{}) || in.StopReason != "" || in.LastError != "" || in.CompletedAt.IsZero() {
			t.Fatal("skip did not persist clean zero-work success")
		}
	}
	if calls != wantCalls || len(f.calls) != 1 {
		t.Fatalf("model/sender calls=%d/%d", calls, len(f.calls))
	}
	if !skipped {
		return
	}
	original, err := r.ListMessages(f.ctx, product.ID, 0, 100)
	if err != nil || len(original) != 1 {
		t.Fatal("skipped input should retain only its user record", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	f.bc.MaxWindowEvents = 1
	f.reopen()
	f.cfg.Agent.Model, f.cfg.PollInterval = model, time.Millisecond
	r, err = brain.Open(f.ctx, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	next := f.group("GROUP_AT_MESSAGE_CREATE", "group_2", "next", "下一条消息")
	runPrecheckProduct(t, f, r, next)
	after, err := r.ListMessages(f.ctx, product.ID, 0, 100)
	if err != nil || !reflect.DeepEqual(original, after) || calls != 1 || len(f.calls) != 1 {
		t.Fatal("restart/next input changed skipped audit or retried sending", err)
	}
}

func TestReplyDeliveryHistoryEndToEnd(t *testing.T) {
	for _, name := range []string{"no_tool", "skip", "sent", "ordinary_sent", "error", "empty_ack", "ack_error"} {
		t.Run(name, func(t *testing.T) {
			want := replydelivery.NotSent
			switch name {
			case "sent", "ordinary_sent", "ack_error":
				want = replydelivery.Sent
			case "error", "empty_ack":
				want = replydelivery.Unknown
			}
			f := newReplyIntegrationFixture(t, 12, checkedQQSender(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				switch name {
				case "error":
					return qq.SentMessage{}, errors.New("uncertain transport")
				case "empty_ack":
					return qq.SentMessage{}, nil
				case "ack_error":
					return qq.SentMessage{ID: "ack"}, errors.New("after ack")
				default:
					return qq.SentMessage{ID: "ack"}, nil
				}
			}))
			kind := "GROUP_AT_MESSAGE_CREATE"
			if name == "ordinary_sent" {
				kind = "GROUP_MESSAGE_CREATE"
			}
			first := f.group(kind, "group_1", "first", "今天有个新发现")
			messageID := "first"
			if name == "skip" {
				// A model cannot authorize a different original message.
				messageID = "not-current-input"
			}
			raw, err := json.Marshal(qq.TextMessage{TargetType: "group", TargetID: "group_1", MsgID: messageID, Content: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			phase := 0
			f.cfg.PollInterval = time.Millisecond
			f.cfg.Agent.Model = deliveryModelFunc(func(ctx context.Context, req minipi.Request) (minipi.Response, error) {
				defer func() { phase++ }()
				if phase == 0 {
					if replydelivery.Outcome(ctx) != replydelivery.NotSent {
						t.Error("production input lacks a fresh host tracker")
					}
					if name != "no_tool" {
						return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, Content: "claimed sent", ToolCalls: []minipi.ToolCall{{ID: "send", Name: "qq_send_text", Arguments: raw}}}, StopReason: minipi.StopToolUse}, nil
					}
				} else if phase == 1 && name != "no_tool" {
					if len(req.Messages) != 3 || req.Messages[1].Content != "claimed sent" || req.Messages[2].ToolCallID != "send" {
						t.Error("current tool round changed")
					}
					if replydelivery.Outcome(ctx) != want {
						t.Errorf("production delivery=%q, want %q", replydelivery.Outcome(ctx), want)
					}
				} else {
					if replydelivery.Outcome(ctx) != replydelivery.NotSent {
						t.Error("new input inherited old delivery")
					}
					count := 4
					if want == replydelivery.NotSent {
						count = 2
					}
					if len(req.Messages) != count {
						t.Errorf("history length=%d, want %d", len(req.Messages), count)
					} else if want != replydelivery.NotSent {
						if req.Messages[1].Content != "claimed sent" || req.Messages[2].Content != "claimed not sent" {
							t.Error("sent or uncertain assistant prose was removed")
						}
					}
					for _, m := range req.Messages {
						if m.Role == minipi.RoleTool || len(m.ToolCalls) != 0 || m.ToolCallID != "" {
							t.Error("historical tool call or result reached model")
						}
						if want == replydelivery.NotSent && m.Role == minipi.RoleAssistant {
							t.Error("not-sent assistant prose reached model")
						}
					}
				}
				return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, Content: "claimed not sent"}, StopReason: minipi.StopEnd}, nil
			})
			r, err := brain.Open(f.ctx, f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { r.Close() }()
			run := func(product messagebuilder.Message) {
				t.Helper()
				if err := r.Handle(f.ctx, product); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				done := make(chan error, 1)
				go func() { done <- r.Run(ctx) }()
				defer func() {
					cancel()
					if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				}()
				for ctx.Err() == nil {
					in, err := r.Get(ctx, product.ID)
					if err != nil {
						t.Fatal(err)
					}
					if in.Status == brain.StatusSucceeded {
						return
					}
					if in.Status == brain.StatusFailed || in.Status == brain.StatusInterrupted {
						t.Fatalf("unexpected terminal status: %s", in.Status)
					}
					time.Sleep(time.Millisecond)
				}
				t.Fatal(ctx.Err())
			}
			run(first)
			original, err := r.ListMessages(f.ctx, first.ID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			r, err = brain.Open(f.ctx, f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			run(f.group("GROUP_AT_MESSAGE_CREATE", "group_2", "next", "下一条"))
			after, err := r.ListMessages(f.ctx, first.ID, 0, 100)
			if err != nil || !reflect.DeepEqual(original, after) {
				t.Fatal("audit changed after history filtering", err)
			}
			calls := 1
			if name == "no_tool" || name == "skip" {
				calls = 0
			}
			if len(f.calls) != calls {
				t.Errorf("transport calls=%d, want %d", len(f.calls), calls)
			}
		})
	}
}
