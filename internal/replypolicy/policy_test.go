package replypolicy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/im/qq"
)

type senderFunc func(context.Context, qq.TextMessage) (qq.SentMessage, error)

func (f senderFunc) SendText(ctx context.Context, m qq.TextMessage) (qq.SentMessage, error) {
	return f(ctx, m)
}

type fixture struct {
	p     *Policy
	cfg   Config
	clock atomic.Int64
	calls atomic.Int64
	seq   int64
}

func newFixture(t *testing.T, change func(*Config)) *fixture {
	t.Helper()
	f := &fixture{cfg: DefaultConfig()}
	f.clock.Store(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano())
	f.cfg.Path = filepath.Join(t.TempDir(), "policy.db")
	f.cfg.AccountID = "account"
	f.cfg.BotUserID = "bot"
	f.cfg.Aliases = []string{"小助手"}
	if change != nil {
		change(&f.cfg)
	}
	f.p = f.open(t)
	return f
}

func (f *fixture) now() time.Time       { return time.Unix(0, f.clock.Load()) }
func (f *fixture) tick(d time.Duration) { f.clock.Add(int64(d)) }
func (f *fixture) open(t *testing.T) *Policy {
	t.Helper()
	p, err := Open(context.Background(), f.cfg, senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
		f.calls.Add(1)
		return qq.SentMessage{ID: "sent"}, nil
	}))
	if err != nil {
		t.Fatal("open failed")
	}
	p.now = f.now
	t.Cleanup(func() {
		if p.Close() != nil {
			t.Error("close failed")
		}
	})
	return p
}

func (f *fixture) event(group, user, text string, direct bool) event.Event {
	f.seq++
	kind, ct, target := "GROUP_MESSAGE_CREATE", "group", group
	author := qq.User{MemberOpenID: user}
	if direct {
		kind = "GROUP_AT_MESSAGE_CREATE"
	}
	if group == "" {
		kind, ct, target = "C2C_MESSAGE_CREATE", "private", user
		author = qq.User{UserOpenID: user}
	}
	body, _ := json.Marshal(qq.Content{Version: 1, QQEvent: kind, Text: text, Author: author})
	return event.Event{ID: fmt.Sprint(f.seq), Sequence: f.seq, ProducerID: "qq.websocket", ReceivedAt: f.now(), Input: event.Input{
		EventType: "im.message", SchemaVersion: 1, OccurredAt: f.now(), Content: string(body),
		Source: &event.Source{Platform: "qq", AccountID: "account", ConversationType: ct, ConversationID: target, SenderID: user, MessageID: fmt.Sprint(f.seq)},
	}}
}

func editBody(e *event.Event, change func(*qq.Content)) {
	var c qq.Content
	_ = json.Unmarshal([]byte(e.Content), &c)
	change(&c)
	b, _ := json.Marshal(c)
	e.Content = string(b)
}
func bind(t *testing.T, p *Policy, events ...event.Event) context.Context {
	t.Helper()
	ctx, err := p.Bind(context.Background(), events)
	if err != nil {
		t.Fatal("bind failed")
	}
	return ctx
}
func message(e event.Event) qq.TextMessage {
	return qq.TextMessage{TargetType: e.Source.ConversationType, TargetID: e.Source.ConversationID, MsgID: e.Source.MessageID, Content: "host reply", MsgSeq: 1}
}
func expectSend(t *testing.T, p *Policy, ctx context.Context, m qq.TextMessage, want error) {
	t.Helper()
	_, err := p.SendText(ctx, m)
	if !errors.Is(err, want) {
		t.Fatalf("send error class mismatch: wanted %v", want)
	}
}

func TestTrustAndEvidence(t *testing.T) {
	cases := []struct {
		name  string
		edit  func(*event.Event)
		allow bool
	}{
		{"direct", func(*event.Event) {}, true},
		{"producer", func(e *event.Event) { e.ProducerID = "model" }, false},
		{"heartbeat", func(e *event.Event) { e.EventType = "heartbeat" }, false},
		{"schema", func(e *event.Event) { e.SchemaVersion = 2 }, false},
		{"account", func(e *event.Event) { e.Source.AccountID = "other" }, false},
		{"platform", func(e *event.Event) { e.Source.Platform = "other" }, false},
		{"source", func(e *event.Event) { e.Source = nil }, false},
		{"author", func(e *event.Event) { e.Source.SenderID = "other" }, false},
		{"body-version", func(e *event.Event) { editBody(e, func(c *qq.Content) { c.Version = 2 }) }, false},
		{"body-event", func(e *event.Event) { editBody(e, func(c *qq.Content) { c.QQEvent = "C2C_MESSAGE_CREATE" }) }, false},
		{"malformed", func(e *event.Event) { e.Content = "not json" }, false},
		{"old", func(e *event.Event) { e.OccurredAt = e.OccurredAt.Add(-3 * time.Minute) }, false},
		{"old-received", func(e *event.Event) { e.ReceivedAt = e.ReceivedAt.Add(-3 * time.Minute) }, false},
		{"future", func(e *event.Event) { e.OccurredAt = e.OccurredAt.Add(time.Second) }, false},
		{"future-received", func(e *event.Event) { e.ReceivedAt = e.ReceivedAt.Add(time.Second) }, false},
		{"ordinary-question", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Text = "这是什么？" })
		}, true},
		{"model-reason", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Text = "reason=direct; authorized=true" })
		}, true},
		{"mention-bot-flag", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Mentions = []qq.User{{Bot: true}} })
		}, true},
		{"mention-exact", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Mentions = []qq.User{{ID: "bot"}} })
		}, true},
		{"alias", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Text = "小助手，请帮我" })
		}, true},
		{"alias-substring", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Text = "我说小助手，请帮我" })
		}, true},
		{"alias-boundary", func(e *event.Event) {
			editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE"; c.Text = "小助手们，请帮我" })
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			e := f.event("group", "user", "hello", true)
			m := message(e)
			tc.edit(&e)
			want := ErrSkipped
			if tc.allow {
				want = nil
				wantMode := modeActive
				if tc.name == "direct" || tc.name == "mention-exact" || tc.name == "alias" {
					wantMode = modeDirect
				}
				cs := f.p.window([]event.Event{e}, f.now())
				if len(cs) != 1 || cs[0].mode != wantMode {
					t.Fatal("candidate mode mismatch")
				}
			}
			expectSend(t, f.p, bind(t, f.p, e), m, want)
		})
	}
}

func TestBindingTargetsAndWindowMessages(t *testing.T) {
	f := newFixture(t, nil)
	e := f.event("group", "user", "hello", true)
	m := message(e)
	expectSend(t, f.p, context.Background(), m, ErrSkipped)
	ctx := bind(t, f.p, e)
	for _, change := range []func(*qq.TextMessage){
		func(m *qq.TextMessage) { m.MsgID = "" }, func(m *qq.TextMessage) { m.MsgID = "other" },
		func(m *qq.TextMessage) { m.TargetID = "other" }, func(m *qq.TextMessage) { m.TargetType = "private" },
	} {
		other := m
		change(&other)
		expectSend(t, f.p, ctx, other, ErrSkipped)
	}
	empty, err := f.p.Bind(ctx, nil)
	if err != nil {
		t.Fatal("empty bind failed")
	}
	expectSend(t, f.p, empty, m, ErrSkipped)
	later := f.event("group", "user", "换个话题", false)
	window := bind(t, f.p, later, e)
	requirePrecheck(t, f.p, ctx, false)
	expectSend(t, f.p, ctx, m, ErrSkipped)
	expectSend(t, f.p, window, m, nil)
	other := f.event("group", "other", "hello", true)
	private := f.event("", "user", "hello", false)
	mixed := bind(t, f.p, other, private)
	expectSend(t, f.p, mixed, message(other), nil)
	expectSend(t, f.p, mixed, message(private), nil)
}

func TestWindowMessageModesAndACK(t *testing.T) {
	for _, directFirst := range []bool{true, false} {
		f := newFixture(t, func(c *Config) { c.ActiveLimit = 1 })
		seed := f.event("group", "seed", "ordinary", false)
		expectSend(t, f.p, bind(t, f.p, seed), message(seed), nil)
		first := f.event("group", "user", "first", directFirst)
		last := f.event("group", "user", "last", !directFirst)
		ctx := bind(t, f.p, last, first)
		requirePrecheck(t, f.p, ctx, true)
		direct, active := first, last
		if !directFirst {
			direct, active = last, first
		}
		expectSend(t, f.p, ctx, message(active), ErrSkipped)
		expectSend(t, f.p, ctx, message(direct), nil)
		expectSend(t, f.p, ctx, message(direct), ErrSkipped)
		requirePrecheck(t, f.p, ctx, false)
		var charged int
		if err := f.p.db.QueryRow(`SELECT SUM(active) FROM reservations`).Scan(&charged); err != nil || charged != 1 {
			t.Fatal("message classification changed active charging")
		}
		var expires int64
		actor := f.p.window([]event.Event{last}, f.now())[0].actor
		if err := f.p.db.QueryRow(`SELECT expires FROM actors WHERE actor=?`, actor).Scan(&expires); err != nil || (directFirst && expires != 0) || (!directFirst && expires == 0) {
			t.Fatal("ACK did not match the selected message's own token")
		}
	}
}

func TestWindowMultipleDirectAndConservativeRebind(t *testing.T) {
	f := newFixture(t, nil)
	first := f.event("group", "user", "first", true)
	last := f.event("group", "user", "last", true)
	ctx := bind(t, f.p, last, first, first)
	b := ctx.Value(bindingKey{}).(*binding)
	if len(b.candidates) != 2 {
		t.Fatal("exact duplicates did not collapse")
	}
	peer := f.open(t)
	replayed := bind(t, peer, first, last)
	expectSend(t, peer, replayed, message(first), ErrSkipped)
	// Rebinding the final token must not invalidate the original window.
	requirePrecheck(t, f.p, ctx, true)
	expectSend(t, f.p, ctx, message(last), nil)
	var before, after int64
	actor := f.p.window([]event.Event{last}, f.now())[0].actor
	if err := f.p.db.QueryRow(`SELECT expires FROM actors WHERE actor=?`, actor).Scan(&before); err != nil || before == 0 {
		t.Fatal("latest ACK did not open session")
	}
	f.tick(time.Second)
	expectSend(t, f.p, ctx, message(first), nil)
	if err := f.p.db.QueryRow(`SELECT expires FROM actors WHERE actor=?`, actor).Scan(&after); err != nil || after != before {
		t.Fatal("earlier ACK overwrote latest session")
	}
	for _, e := range []event.Event{first, last} {
		m := message(e)
		m.MsgSeq++
		expectSend(t, f.p, ctx, m, ErrSkipped)
		expectSend(t, peer, replayed, m, ErrSkipped)
	}
}

func TestWindowCandidateOwnTimeAndSilentFence(t *testing.T) {
	for _, name := range []string{"expires-after-bind", "silent-first", "silent-last"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			first := f.event("group", "user", "first", true)
			if name == "silent-first" {
				f.tick(maxEventAge + time.Second)
			} else {
				f.tick(maxEventAge - time.Second)
			}
			last := f.event("group", "user", "last", true)
			if name == "silent-last" {
				last.ReceivedAt = last.ReceivedAt.Add(time.Second)
			}
			ctx := bind(t, f.p, first, last)
			if name == "expires-after-bind" {
				f.tick(2 * time.Second)
			}
			expectSend(t, f.p, ctx, message(first), ErrSkipped)
			want := error(nil)
			if name == "silent-last" {
				want = ErrSkipped
			}
			expectSend(t, f.p, ctx, message(last), want)
		})
	}
}

func TestWindowConflictsMaskWithoutStateChanges(t *testing.T) {
	changes := map[string]func(*event.Event){
		"content": func(e *event.Event) { editBody(e, func(c *qq.Content) { c.Text = "different" }) },
		"author": func(e *event.Event) {
			e.Source.SenderID = "other"
			editBody(e, func(c *qq.Content) { c.Author.MemberOpenID = "other" })
		},
		"time":           func(e *event.Event) { e.OccurredAt = e.OccurredAt.Add(-time.Second) },
		"silent":         func(e *event.Event) { e.ReceivedAt = e.ReceivedAt.Add(time.Second) },
		"malformed":      func(e *event.Event) { e.Content = "not json" },
		"schema":         func(e *event.Event) { e.SchemaVersion++ },
		"classification": func(e *event.Event) { editBody(e, func(c *qq.Content) { c.QQEvent = "GROUP_MESSAGE_CREATE" }) },
	}
	for name, change := range changes {
		for _, sameEventID := range []bool{true, false} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/event=%t/reverse=%t", name, sameEventID, reverse), func(t *testing.T) {
					f := newFixture(t, nil)
					parentEvent := f.event("group", "user", "parent", true)
					parent := bind(t, f.p, parentEvent)
					fresh := f.event("group", "user", "fresh", true)
					conflict := fresh
					src := *fresh.Source
					conflict.Source = &src
					if !sameEventID {
						conflict.ID = "different-event"
					}
					change(&conflict)
					window := []event.Event{fresh, conflict}
					if reverse {
						window[0], window[1] = window[1], window[0]
					}
					f.tick(time.Nanosecond)
					masked, err := f.p.Bind(parent, window)
					if err != nil {
						t.Fatal("conflict interrupted input restoration")
					}
					b := masked.Value(bindingKey{}).(*binding)
					if len(b.candidates) != 0 || b.reason != "window_conflict" {
						t.Fatal("conflict retained partial authorization")
					}
					requirePrecheck(t, f.p, masked, false)
					expectSend(t, f.p, masked, message(parentEvent), ErrSkipped)
					expectSend(t, f.p, masked, message(fresh), ErrSkipped)
					var clock int64
					if err := f.p.db.QueryRow(`SELECT clock FROM policy_meta`).Scan(&clock); err != nil || clock != parentEvent.ReceivedAt.UnixNano() {
						t.Fatal("conflict changed durable clock")
					}
					requirePrecheck(t, f.p, parent, true)
					expectSend(t, f.p, parent, message(parentEvent), nil)
				})
			}
		}
	}
}

func TestCandidatesWithoutSession(t *testing.T) {
	for _, ending := range []string{"结束", "换个话题", "天气怎么样", "谢谢", "请问数据库如何设置连接超时？"} {
		t.Run(ending, func(t *testing.T) {
			f := newFixture(t, nil)
			e := f.event("group", "user", "hello", true)
			expectSend(t, f.p, bind(t, f.p, e), message(e), nil)
			f.tick(90 * time.Second)
			other := f.event("other-group", "other", "继续", false)
			expectSend(t, f.p, bind(t, f.p, other), message(other), nil)
			next := f.event("group", "user", "请继续", false)
			expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
			f.tick(90 * time.Second)
			next = f.event("group", "user", "再详细说说", false)
			expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
			stop := f.event("group", "user", ending, false)
			bind(t, f.p, stop)
			f.tick(90 * time.Second)
			next = f.event("group", "user", "继续", false)
			expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
		})
	}
	t.Run("unanswered-does-not-refresh", func(t *testing.T) {
		f := newFixture(t, nil)
		first := f.event("group", "user", "hello", true)
		expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
		f.tick(90 * time.Second)
		next := f.event("group", "user", "继续", false)
		bind(t, f.p, next)
		f.tick(30 * time.Second)
		next = f.event("group", "user", "继续", false)
		expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
		expectSend(t, f.p, bind(t, f.p, first), message(first), ErrSkipped)
	})
}

func requirePrecheck(t *testing.T, p *Policy, ctx context.Context, want bool) {
	t.Helper()
	ready, err := p.Precheck(ctx)
	if err != nil || ready != want {
		t.Fatalf("precheck ready=%t want=%t err=%v", ready, want, err)
	}
}

func TestPrecheckNonConsumingAndBudgets(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.TotalLimit = 2; c.ActiveLimit = 1 })
	first := f.event("group", "user", "ordinary", false)
	ctx := bind(t, f.p, first)
	var before, after int64
	if err := f.p.db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	f.tick(time.Second)
	for i := 0; i < 3; i++ {
		requirePrecheck(t, f.p, ctx, true)
	}
	if err := f.p.db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || after != before || f.calls.Load() != 0 {
		t.Fatal("precheck wrote state or called sender")
	}
	expectSend(t, f.p, ctx, message(first), nil)
	requirePrecheck(t, f.p, ctx, false)
	ordinary := f.event("group", "other", "ordinary", false)
	requirePrecheck(t, f.p, bind(t, f.p, ordinary), false)
	direct := f.event("group", "direct", "hello", true)
	ctx = bind(t, f.p, direct)
	requirePrecheck(t, f.p, ctx, true)
	expectSend(t, f.p, ctx, message(direct), nil)
	blocked := f.event("group", "blocked", "hello", true)
	requirePrecheck(t, f.p, bind(t, f.p, blocked), false)
	other := f.event("other-group", "other", "ordinary", false)
	requirePrecheck(t, f.p, bind(t, f.p, blocked, other), true)
	private := f.event("", "private", "hello", false)
	requirePrecheck(t, f.p, bind(t, f.p, blocked, private), true)
	if err := f.p.Close(); err != nil {
		t.Fatal(err)
	}
	f.p = f.open(t)
	requirePrecheck(t, f.p, bind(t, f.p, blocked), false)
	f.tick(f.cfg.TotalWindow)
	fresh := f.event("group", "fresh", "hello", true)
	requirePrecheck(t, f.p, bind(t, f.p, fresh), true)
	ordinary = f.event("group", "fresh", "ordinary", false)
	requirePrecheck(t, f.p, bind(t, f.p, ordinary), false)
	f.tick(f.cfg.ActiveWindow)
	ordinary = f.event("group", "fresh", "ordinary", false)
	requirePrecheck(t, f.p, bind(t, f.p, ordinary), true)
}

func TestPrecheckIntervalAndStateFences(t *testing.T) {
	f := newFixture(t, nil)
	first := f.event("group", "first", "ordinary", false)
	expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
	second := f.event("group", "second", "ordinary", false)
	ctx := bind(t, f.p, second)
	requirePrecheck(t, f.p, ctx, false)
	f.tick(f.cfg.ActiveInterval)
	second = f.event("group", "second", "ordinary", false)
	ctx = bind(t, f.p, second)
	requirePrecheck(t, f.p, ctx, true)
	f.tick(-time.Second)
	requirePrecheck(t, f.p, ctx, false)
	f.tick(time.Second)
	newer := f.event("group", "second", "newer", false)
	fresh := bind(t, f.p, newer)
	requirePrecheck(t, f.p, ctx, false)
	requirePrecheck(t, f.p, fresh, true)
	f.tick(24 * time.Hour)
	requirePrecheck(t, f.p, fresh, false)
	requirePrecheck(t, f.p, context.Background(), false)
	canceled, cancel := context.WithCancel(fresh)
	cancel()
	if ready, err := f.p.Precheck(canceled); ready || !errors.Is(err, context.Canceled) {
		t.Fatal("lost precheck cancellation")
	}
	f.cfg.TotalLimit++
	_ = f.open(t)
	requirePrecheck(t, f.p, fresh, false)
	if err := f.p.Close(); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.p.Precheck(fresh); ready || !errors.Is(err, ErrClosed) {
		t.Fatal("closed policy accepted precheck")
	}
}

func TestPrecheckStorageFailure(t *testing.T) {
	f := newFixture(t, nil)
	ctx := bind(t, f.p, f.event("group", "user", "hello", true))
	if _, err := f.p.db.Exec(`DROP TABLE reservations`); err != nil {
		t.Fatal(err)
	}
	if ready, err := f.p.Precheck(ctx); ready || !errors.Is(err, ErrStorage) {
		t.Fatal("storage failure became a normal skip")
	}
}

func TestBudgetsAndRestart(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.TotalLimit = 2 })
	question := "普通群聊消息"
	first := f.event("group", "user", question, false)
	ctx := bind(t, f.p, first)
	expectSend(t, f.p, ctx, message(first), nil)
	retry := message(first)
	retry.MsgSeq = 999
	expectSend(t, f.p, ctx, retry, ErrSkipped)
	second := f.event("group", "other", question, false)
	expectSend(t, f.p, bind(t, f.p, second), message(second), ErrSkipped)
	direct := f.event("group", "other", "hello", true)
	expectSend(t, f.p, bind(t, f.p, direct), message(direct), nil)
	direct = f.event("group", "third", "hello", true)
	expectSend(t, f.p, bind(t, f.p, direct), message(direct), ErrSkipped)
	private := f.event("", "user", "hello", false)
	expectSend(t, f.p, bind(t, f.p, private), message(private), nil)
	expectSend(t, f.p, bind(t, f.p, private), message(private), ErrSkipped)
	if f.p.Close() != nil {
		t.Fatal("close failed")
	}
	f.p = f.open(t)
	expectSend(t, f.p, bind(t, f.p, first), message(first), ErrSkipped)
	expectSend(t, f.p, bind(t, f.p, direct), message(direct), ErrSkipped)
	f.tick(90 * time.Second)
	next := f.event("group", "user", question, false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
	f.tick(90 * time.Second)
	next = f.event("group", "user", question, false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
	f.tick(90 * time.Second)
	next = f.event("group", "user", question, false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), ErrSkipped)
	f.tick(6 * time.Minute)
	next = f.event("group", "user", question, false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
	info, err := os.Stat(f.cfg.Path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("database permissions mismatch")
	}
}

func TestUnknownOutcomeAndSessionRestart(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.TotalLimit = 1 })
	f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
		return qq.SentMessage{}, errors.New("sensitive transport detail")
	})
	e := f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, e), message(e), ErrSend)
	expectSend(t, f.p, bind(t, f.p, e), message(e), ErrSkipped)
	if f.p.Close() != nil {
		t.Fatal("close failed")
	}
	f.p = f.open(t)
	next := f.event("group", "user", "继续", false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), ErrSkipped)
	next = f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, next), message(next), ErrSkipped)
	f.tick(time.Minute)
	next = f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
	if f.p.Close() != nil {
		t.Fatal("close failed")
	}
	f.p = f.open(t)
	f.tick(time.Minute)
	next = f.event("group", "user", "继续", false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
}

func TestEmptyACKDoesNotAuthorize(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprint(continued), func(t *testing.T) {
			f := newFixture(t, func(c *Config) { c.TotalLimit = 2 })
			first := f.event("group", "user", "hello", true)
			if continued {
				expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
				f.tick(90 * time.Second)
				first = f.event("group", "user", "继续", false)
			}
			f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				return qq.SentMessage{}, nil
			})
			ctx := bind(t, f.p, first)
			expectSend(t, f.p, ctx, message(first), ErrSend)
			retry := message(first)
			retry.MsgSeq++
			expectSend(t, f.p, ctx, retry, ErrSkipped)
			var count int
			if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != int(f.calls.Load()) {
				t.Fatal("unknown outcome did not retain reservation")
			}
			if f.p.Close() != nil {
				t.Fatal("close failed")
			}
			f.p = f.open(t)
			var expires int64
			if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || expires != 0 {
				t.Fatal("empty ACK authorized a session")
			}
			f.tick(90 * time.Second)
			next := f.event("group", "user", "继续", false)
			expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
		})
	}
}

func TestCandidatesAndGroupIsolation(t *testing.T) {
	for _, proactive := range []bool{false, true} {
		for _, variant := range []string{"ordinary", "mention", "reference", "missing-reference", "attachment"} {
			t.Run(fmt.Sprintf("%t/%s", proactive, variant), func(t *testing.T) {
				f := newFixture(t, func(c *Config) { c.Proactive = proactive; c.TotalLimit = 1 })
				first := f.event("group", "user", "hello", true)
				expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
				e := f.event("other", "user", "这个思路我刚刚也试过", false)
				editBody(&e, func(c *qq.Content) {
					switch variant {
					case "mention":
						c.Mentions = []qq.User{{ID: "someone"}}
					case "reference":
						c.ReferenceIndex = "ref"
						c.Elements = []qq.Element{{Index: "ref", Author: qq.User{ID: "someone"}}}
					case "missing-reference":
						c.ReferenceIndex = "missing"
					case "attachment":
						c.Text = ""
						c.Elements = []qq.Element{{Index: "rich"}}
					}
				})
				cs := f.p.window([]event.Event{e}, f.now())
				if len(cs) != 1 || cs[0].mode != modeActive {
					t.Fatal("ordinary mode mismatch")
				}
				expectSend(t, f.p, bind(t, f.p, e), message(e), nil)
				var active int
				if err := f.p.db.QueryRow(`SELECT active FROM reservations WHERE message=?`, cs[0].message).Scan(&active); err != nil || active != 1 {
					t.Fatal("ordinary candidate did not consume active budget")
				}
			})
		}
	}
}

func TestOrdinaryCandidates(t *testing.T) {
	for _, text := range []string{"啊啊啊啊啊啊啊啊啊啊啊？", "😀😀😀😀😀😀😀😀😀😀😀😀？", "怎么办？", "请问你好吗？", "请问这个配置应该如何调整？", "这个思路我刚刚也试过", "继续", "再详细说说", "谢谢", "结束", "换个话题", "😀"} {
		f := newFixture(t, nil)
		e := f.event("group", "user", text, false)
		cs := f.p.window([]event.Event{e}, f.now())
		if len(cs) != 1 || cs[0].mode != modeActive {
			t.Fatal("ordinary mode mismatch")
		}
		expectSend(t, f.p, bind(t, f.p, e), message(e), nil)
	}
}

func TestGroupFocus(t *testing.T) {
	for _, variant := range []string{"same-window", "restart", "other-instance", "reference", "other-group", "old-message", "bound-before-turn"} {
		t.Run(variant, func(t *testing.T) {
			f := newFixture(t, nil)
			old := f.event("group", "other", "先前话题", false)
			first := f.event("group", "user", "hello", true)
			expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
			group := "group"
			if variant == "other-group" {
				group = "elsewhere"
			}
			other := f.event(group, "other", "讨论明天的天气", false)
			if variant == "old-message" {
				other = old
			}
			var before context.Context
			if variant == "bound-before-turn" {
				next := f.event("group", "user", "继续", false)
				before = bind(t, f.p, next)
				other = f.event("group", "other", "讨论明天的天气", false)
				bind(t, f.p, other)
				expectSend(t, f.p, before, message(next), nil)
				return
			}
			next := f.event("group", "user", "继续", false)
			if variant == "reference" {
				editBody(&next, func(c *qq.Content) {
					c.ReferenceIndex = "ref"
					c.Elements = []qq.Element{{Index: "ref", Author: qq.User{ID: "bot"}}}
				})
			}
			var ctx context.Context
			if variant == "same-window" {
				ctx = bind(t, f.p, next, other)
			} else {
				p := f.p
				if variant == "other-instance" {
					p = f.open(t)
				}
				bind(t, p, other)
				if variant == "restart" {
					if f.p.Close() != nil {
						t.Fatal("close failed")
					}
					f.p = f.open(t)
				}
				ctx = bind(t, f.p, next)
			}
			expectSend(t, f.p, ctx, message(next), nil)
		})
	}
}

func TestReferenceEvidence(t *testing.T) {
	for _, variant := range []string{"exact", "bot-flag", "wrong-index", "nested", "no-session", "no-bot-id"} {
		t.Run(variant, func(t *testing.T) {
			f := newFixture(t, func(c *Config) {
				if variant == "no-bot-id" {
					c.BotUserID = ""
				}
			})
			if variant != "no-session" {
				e := f.event("group", "user", "hello", true)
				expectSend(t, f.p, bind(t, f.p, e), message(e), nil)
			}
			e := f.event("group", "user", "解释一下", false)
			editBody(&e, func(c *qq.Content) {
				c.ReferenceIndex = "ref"
				c.Elements = []qq.Element{{Index: "ref", Author: qq.User{ID: "bot"}}}
				switch variant {
				case "bot-flag":
					c.Elements[0].Author = qq.User{Bot: true}
				case "wrong-index":
					c.Elements[0].Index = "sent"
				case "nested":
					c.Elements = []qq.Element{{Elements: c.Elements}}
				}
			})
			wantMode := modeActive
			if variant == "exact" || variant == "no-session" {
				wantMode = modeDirect
			}
			cs := f.p.window([]event.Event{e}, f.now())
			if len(cs) != 1 || cs[0].mode != wantMode {
				t.Fatal("reference mode mismatch")
			}
			expectSend(t, f.p, bind(t, f.p, e), message(e), nil)
		})
	}
}
