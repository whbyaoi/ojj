package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"ojj/internal/brain"
	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
	"ojj/internal/replypolicy"
)

type replyIntegrationFixture struct {
	t       *testing.T
	ctx     context.Context
	path    string
	store   *event.Store
	builder *messagebuilder.Builder
	bc      messagebuilder.Config
	pc      replypolicy.Config
	cfg     brain.Config
	policy  *replypolicy.Policy
	tool    minipi.Tool
	sender  checkedQQSender
	calls   []qq.TextMessage
	seq     int64
}

func newReplyIntegrationFixture(t *testing.T, totalLimit int, outcome checkedQQSender) *replyIntegrationFixture {
	t.Helper()
	brainTestEnv(t)
	t.Setenv("OJJ_QQ_SEND_ALLOWLIST", "group:group_1,group:group_2")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	f := &replyIntegrationFixture{t: t, ctx: ctx, path: filepath.Join(t.TempDir(), "events.db")}
	var err error
	f.store, err = event.Open(ctx, event.Config{Path: f.path, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.bc = messagebuilder.Config{Path: f.path + ".messages.db", QuietPeriod: time.Millisecond, PollInterval: time.Millisecond}
	f.builder, err = messagebuilder.Open(ctx, f.bc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.builder != nil {
			if err := f.builder.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	f.pc, err = replyPolicyConfig(f.path, "test")
	if err != nil {
		t.Fatal(err)
	}
	f.pc.TotalLimit = totalLimit
	f.sender = checkedQQSender(func(ctx context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
		f.calls = append(f.calls, msg)
		if outcome != nil {
			return outcome(ctx, msg)
		}
		return qq.SentMessage{ID: "ack-" + msg.MsgID}, nil
	})
	t.Cleanup(func() {
		if f.policy != nil {
			if err := f.policy.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	f.attach()
	return f
}

func (f *replyIntegrationFixture) attach() {
	f.t.Helper()
	// Always rebuild: reusing cfg would append a duplicate qq_send_text tool.
	cfg, allowed, err := mainBrainConfig(f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	f.policy, err = attachReplyPolicy(f.ctx, &cfg, f.builder, f.pc, f.sender, allowed)
	if err != nil {
		f.t.Fatal(err)
	}
	f.cfg = cfg
	count := 0
	for _, tool := range cfg.Agent.Tools {
		if tool.Definition.Name == "qq_send_text" {
			f.tool = tool
			count++
		}
	}
	if count != 1 || cfg.InputContext == nil {
		f.t.Fatalf("incomplete production assembly: send tools=%d", count)
	}
}

func (f *replyIntegrationFixture) reopen() {
	f.t.Helper()
	if err := f.policy.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.policy = nil
	if err := f.builder.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.builder = nil
	var err error
	f.builder, err = messagebuilder.Open(f.ctx, f.bc)
	if err != nil {
		f.t.Fatal(err)
	}
	f.attach()
}

func (f *replyIntegrationFixture) seal(producerID string, in event.Input) messagebuilder.Message {
	f.t.Helper()
	producer, err := f.store.BindProducer(producerID)
	if err != nil {
		f.t.Fatal(err)
	}
	stop := errors.New("durable window sealed")
	var product messagebuilder.Message
	err = runWithMessages(f.ctx, f.store, f.builder, nil, func(child context.Context) error {
		published, err := producer.Publish(child, in)
		if err != nil {
			return err
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			messages, err := f.builder.List(child, messagebuilder.MessageQuery{AfterSequence: f.seq})
			if err != nil {
				return err
			}
			if len(messages) != 0 {
				product = messages[0]
				if len(messages) != 1 || len(product.EventIDs) != 1 || product.EventIDs[0] != published.Event.ID || product.ID == published.Event.ID {
					return errors.New("incorrect durable product or event snapshot")
				}
				return stop
			}
			select {
			case <-child.Done():
				return child.Err()
			case <-ticker.C:
			}
		}
	})
	if !errors.Is(err, stop) {
		f.t.Fatal("seal production window:", err)
	}
	f.seq = product.Sequence
	return product
}

func (f *replyIntegrationFixture) group(kind, group, id, text string) messagebuilder.Message {
	f.t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id": id, "group_openid": group, "author": map[string]string{"member_openid": "member_1"},
		"content": text, "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		f.t.Fatal(err)
	}
	in, err := qq.Convert("test", kind, raw)
	if err != nil {
		f.t.Fatal(err)
	}
	return f.seal("qq.websocket", in)
}

func (f *replyIntegrationFixture) bind(parent context.Context, product messagebuilder.Message) context.Context {
	f.t.Helper()
	ctx, err := f.cfg.InputContext(parent, product.ID)
	if err != nil {
		f.t.Fatal("restore production input context:", err)
	}
	return ctx
}

func (f *replyIntegrationFixture) execute(ctx context.Context, group, id string, seq uint32) (minipi.ToolResult, error) {
	f.t.Helper()
	raw, err := json.Marshal(qq.TextMessage{TargetType: "group", TargetID: group, Content: "hello", MsgID: id, MsgSeq: seq})
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.tool.Validate(raw); err != nil {
		f.t.Fatal(err)
	}
	return f.tool.Execute(ctx, minipi.ToolCall{ID: "send-" + id, Name: f.tool.Definition.Name, Arguments: raw}, nil)
}

func (f *replyIntegrationFixture) skipped(ctx context.Context, group, id string, seq uint32) {
	f.t.Helper()
	before := len(f.calls)
	result, err := f.execute(ctx, group, id, seq)
	if err != nil || result.IsError || result.Terminate || result.Content != "QQ 回复已跳过，未发送；不排队、不重试，也不要换目标或序号补发。" || len(f.calls) != before {
		f.t.Fatalf("skip must be a non-error without sending: result=%+v err=%v calls=%d/%d", result, err, len(f.calls), before)
	}
}

func (f *replyIntegrationFixture) sent(ctx context.Context, group, id string) {
	f.t.Helper()
	before := len(f.calls)
	result, err := f.execute(ctx, group, id, 0)
	var ack qq.SentMessage
	decodeErr := json.Unmarshal([]byte(result.Content), &ack)
	if err != nil || result.IsError || result.Terminate || decodeErr != nil || ack.ID != "ack-"+id || len(f.calls) != before+1 {
		f.t.Fatalf("expected one acknowledged send: result=%+v err=%v calls=%d/%d", result, err, len(f.calls), before)
	}
	want := qq.TextMessage{TargetType: "group", TargetID: group, Content: "hello", MsgID: id, MsgSeq: 1}
	if f.calls[before] != want {
		f.t.Fatalf("sender received %+v, want %+v", f.calls[before], want)
	}
}

func TestReplyIntegrationInputContext(t *testing.T) {
	f := newReplyIntegrationFixture(t, 12, nil)
	first := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "first", "请回复我")
	for _, id := range []string{"", "unknown-product", first.EventIDs[0]} {
		ctx, err := f.cfg.InputContext(f.ctx, id)
		if !errors.Is(err, replypolicy.ErrStorage) || ctx != nil {
			t.Fatalf("non-product ID %q accepted: ctx=%v err=%v", id, ctx, err)
		}
	}
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.cfg.InputContext(cancelled, first.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	before, err := f.builder.Events(f.ctx, first.ID)
	if err != nil || len(before) != 1 || before[0].ProducerID != "qq.websocket" {
		t.Fatalf("missing trusted snapshot: %+v %v", before, err)
	}
	f.reopen()
	after, err := f.builder.Events(f.ctx, first.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("permanent snapshot changed after reopen: %+v %v", after, err)
	}
	bound := f.bind(f.ctx, first)
	f.skipped(f.ctx, "group_1", "first", 0)
	f.skipped(bound, "group_2", "first", 0)
	f.skipped(bound, "group_1", "not-in-window", 0)

	heartbeat := f.seal("system.heartbeat", event.Input{EventType: "system.heartbeat", OccurredAt: time.Now(), SchemaVersion: 1, Content: `{"version":1,"reason":"idle"}`})
	// A new heartbeat window must mask even a valid parent input binding.
	f.skipped(f.bind(bound, heartbeat), "group_1", "first", 0)
	current := f.group("GROUP_AT_MESSAGE_CREATE", "group_2", "current", "请回复我")
	currentCtx := f.bind(bound, current)
	// Both groups are allowlisted: these denials must come from input scoping.
	f.skipped(currentCtx, "group_1", "first", 0)
	f.skipped(currentCtx, "group_2", "first", 0)
	f.skipped(currentCtx, "group_1", "current", 0)
	f.sent(currentCtx, "group_2", "current")
	// The restored first snapshot really authorizes its own message; earlier
	// denials did not silently consume it, and the heartbeat did not inherit it.
	f.sent(f.bind(f.ctx, first), "group_1", "first")
}

func TestReplyIntegrationOrdinaryCandidates(t *testing.T) {
	for _, text := range []string{"请继续", "这个思路还能再展开一点吗", "谢谢", "今天有个新发现", "怎么弄？"} {
		t.Run(text, func(t *testing.T) {
			f := newReplyIntegrationFixture(t, 12, nil)
			if f.pc.Proactive {
				t.Fatal("fixture must keep the legacy proactive flag disabled")
			}
			// No prior direct reply or session, and no required help/continuation phrase.
			first := f.group("GROUP_MESSAGE_CREATE", "group_1", "ordinary", text)
			bound := f.bind(f.ctx, first)
			f.sent(bound, "group_1", "ordinary")
			f.skipped(bound, "group_1", "ordinary", 0)
			// Ordinary replies still consume active quota; a direct reply neither
			// needs that quota nor grants the next ordinary message a bypass.
			direct := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "direct", "请回复我")
			f.sent(f.bind(f.ctx, direct), "group_1", "direct")
			follow := f.group("GROUP_MESSAGE_CREATE", "group_1", "follow", "请继续")
			f.skipped(f.bind(f.ctx, follow), "group_1", "follow", 0)
			f.reopen()
			f.skipped(f.bind(f.ctx, follow), "group_1", "follow", 0)
			if len(f.calls) != 2 {
				t.Fatalf("candidate sends=%d, want 2", len(f.calls))
			}
		})
	}
}

func TestReplyIntegrationBudgetAndRestart(t *testing.T) {
	f := newReplyIntegrationFixture(t, 2, nil)
	var first messagebuilder.Message
	var oldBinding context.Context
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("direct-%d", i)
		product := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", id, "请回复我")
		bound := f.bind(f.ctx, product)
		f.sent(bound, "group_1", id)
		f.skipped(bound, "group_1", id, 2)
		if i == 1 {
			first, oldBinding = product, bound
			// Verify durable deduplication while budget is still available.
			f.reopen()
			f.skipped(oldBinding, "group_1", id, 3)
			f.skipped(f.bind(f.ctx, first), "group_1", id, 3)
		}
	}
	overBudget := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "over-budget", "请回复我")
	f.skipped(f.bind(f.ctx, overBudget), "group_1", "over-budget", 0)
	f.reopen()
	f.skipped(f.bind(f.ctx, overBudget), "group_1", "over-budget", 4)
	if len(f.calls) != 2 {
		t.Fatalf("budget/restart sends=%d, want 2", len(f.calls))
	}
}

func TestReplyIntegrationUnknownOutcome(t *testing.T) {
	for _, outcome := range []string{"sender-error", "empty-ack"} {
		t.Run(outcome, func(t *testing.T) {
			f := newReplyIntegrationFixture(t, 12, checkedQQSender(func(_ context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
				if msg.MsgID != "unknown" {
					return qq.SentMessage{ID: "ack-" + msg.MsgID}, nil
				}
				if outcome == "sender-error" {
					return qq.SentMessage{}, errors.New("connection lost after possible send")
				}
				return qq.SentMessage{}, nil
			}))
			product := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "unknown", "请回复我")
			bound := f.bind(f.ctx, product)
			result, err := f.execute(bound, "group_1", "unknown", 0)
			if !errors.Is(err, replypolicy.ErrSend) || !result.IsError || result.Terminate || len(f.calls) != 1 {
				t.Fatalf("unknown send must remain an error: result=%+v err=%v calls=%d", result, err, len(f.calls))
			}
			f.skipped(bound, "group_1", "unknown", 2)
			// Reopen before a newer event can invalidate this candidate, so the
			// denial proves durable deduplication rather than stale-input rejection.
			f.reopen()
			f.skipped(f.bind(f.ctx, product), "group_1", "unknown", 3)
			// A new ordinary input needs no acknowledged direct reply/session.
			// This is a fresh message, never a retry of the unknown send.
			follow := f.group("GROUP_MESSAGE_CREATE", "group_1", "no-ack-follow", "请继续")
			f.sent(f.bind(f.ctx, follow), "group_1", "no-ack-follow")
			f.reopen()
			f.skipped(f.bind(f.ctx, product), "group_1", "unknown", 3)
			follow = f.group("GROUP_MESSAGE_CREATE", "group_1", "restart-follow", "请继续")
			f.skipped(f.bind(f.ctx, follow), "group_1", "restart-follow", 0)
			if len(f.calls) != 2 || f.calls[0].MsgID != "unknown" || f.calls[1].MsgID != "no-ack-follow" {
				t.Fatalf("unknown send retried or active quota lost: calls=%d", len(f.calls))
			}
			// A fresh direct input still works: the denials are not a closed policy.
			fresh := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "fresh", "请回复我")
			f.sent(f.bind(f.ctx, fresh), "group_1", "fresh")
		})
	}
}
