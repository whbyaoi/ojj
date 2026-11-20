package replypolicy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/im/qq"
)

func TestConcurrentInstances(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(fmt.Sprint(same), func(t *testing.T) {
			f := newFixture(t, func(c *Config) { c.TotalLimit = 3 })
			second := f.open(t)
			var contexts []context.Context
			var messages []qq.TextMessage
			e := f.event("group", "user", "hello", true)
			for i := 0; i < 24; i++ {
				p := f.p
				if i%2 != 0 {
					p = second
				}
				if !same {
					e = f.event("group", fmt.Sprint("user", i), "hello", true)
				}
				contexts = append(contexts, bind(t, p, e))
				messages = append(messages, message(e))
			}
			// Every read-only precheck passes before any instance reserves a slot.
			for i, ctx := range contexts {
				p := f.p
				if i%2 != 0 {
					p = second
				}
				requirePrecheck(t, p, ctx, true)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var success atomic.Int64
			for i := range contexts {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					p := f.p
					if i%2 != 0 {
						p = second
					}
					_, err := p.SendText(contexts[i], messages[i])
					if err == nil {
						success.Add(1)
					} else if !errors.Is(err, ErrSkipped) {
						t.Error("unexpected concurrent error class")
					}
				}(i)
			}
			close(start)
			wg.Wait()
			want := int64(3)
			if same {
				want = 1
			}
			if success.Load() != want || f.calls.Load() != want {
				t.Fatal("atomic quota or idempotency mismatch")
			}
		})
	}
}

type ackResult struct {
	sent qq.SentMessage
	err  error
}

func expectACK(t *testing.T, result ackResult) {
	t.Helper()
	if result.err != nil || result.sent.ID != "sent" {
		t.Fatal("platform ACK was lost or reported as unsent")
	}
}

func expectACKReservation(t *testing.T, f *fixture, p *Policy, ctx context.Context, msg qq.TextMessage, calls int64) {
	t.Helper()
	msg.MsgSeq++
	expectSend(t, p, ctx, msg, ErrSkipped)
	var count int64
	if err := p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != calls {
		t.Fatal("ACK refunded reservation")
	}
	if f.calls.Load() != calls {
		t.Fatal("ACK or retry invoked sender more than once")
	}
}

func TestDelayedACKCannotUndoNewState(t *testing.T) {
	for _, followup := range []string{"结束", "继续", "new direct"} {
		t.Run(followup, func(t *testing.T) {
			f := newFixture(t, nil)
			second := f.open(t)
			entered, release := make(chan struct{}), make(chan struct{})
			f.p.sender = senderFunc(func(ctx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				close(entered)
				select {
				case <-release:
					return qq.SentMessage{ID: "sent"}, nil
				case <-ctx.Done():
					return qq.SentMessage{}, ctx.Err()
				}
			})
			first := f.event("group", "user", "hello", true)
			ctx := bind(t, f.p, first)
			done := make(chan ackResult, 1)
			go func() { sent, err := f.p.SendText(ctx, message(first)); done <- ackResult{sent, err} }()
			<-entered
			next := f.event("group", "user", followup, followup == "new direct")
			bind(t, second, next)
			close(release)
			expectACK(t, <-done)
			expectACKReservation(t, f, f.p, ctx, message(first), 1)
			// The old ACK must not authorize the newer, unanswered request.
			var expires int64
			if err := second.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || expires != 0 {
				t.Fatal("old ACK authorized newer request")
			}
			next = f.event("group", "user", "继续", false)
			expectSend(t, second, bind(t, second, next), message(next), nil)
		})
	}
}

func TestACKStateFailure(t *testing.T) {
	for _, continued := range []bool{false, true} {
		for _, failure := range []string{"storage", "canceled"} {
			t.Run(fmt.Sprintf("%t/%s", continued, failure), func(t *testing.T) {
				f := newFixture(t, nil)
				first := f.event("group", "user", "hello", true)
				calls := int64(1)
				if continued {
					expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
					f.tick(time.Minute)
					first = f.event("group", "user", "继续", true)
					calls++
				}
				bound := bind(t, f.p, first)
				ctx, cancel := context.WithCancel(bound)
				defer cancel()
				var before int64
				if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&before); err != nil {
					t.Fatal(err)
				}
				if failure == "storage" {
					_, err := f.p.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE OF expires ON actors
WHEN NEW.expires > OLD.expires
BEGIN SELECT RAISE(ABORT, 'sensitive storage detail'); END;`)
					if err != nil {
						t.Fatal(err)
					}
				}
				f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
					f.calls.Add(1)
					if failure == "canceled" {
						cancel()
					}
					return qq.SentMessage{ID: "sent"}, nil
				})
				sent, err := f.p.SendText(ctx, message(first))
				want := ErrStorage
				if failure == "canceled" {
					want = context.Canceled
				}
				// Exact sentinel equality also ensures database details stay redacted.
				if sent.ID != "sent" || err != want || errors.Is(err, ErrSkipped) {
					t.Fatal("ACK state failure lost result or error semantics")
				}
				var after int64
				if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&after); err != nil || after != before {
					t.Fatal("failed ACK state transaction changed session")
				}
				expectACKReservation(t, f, f.p, bound, message(first), calls)

			})
		}
	}
}

func TestCancellationAndClose(t *testing.T) {
	f := newFixture(t, nil)
	e := f.event("group", "user", "hello", true)
	ctx, cancel := context.WithCancel(bind(t, f.p, e))
	cancel()
	expectSend(t, f.p, ctx, message(e), context.Canceled)
	if _, err := f.p.Bind(ctx, []event.Event{e}); !errors.Is(err, context.Canceled) {
		t.Fatal("bind cancellation mismatch")
	}
	var count int
	if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != 0 {
		t.Fatal("pre-cancel reserved state")
	}
	entered := make(chan struct{})
	f.p.sender = senderFunc(func(ctx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
		close(entered)
		<-ctx.Done()
		return qq.SentMessage{}, errors.New("private failure")
	})
	ctx = bind(t, f.p, e)
	sent := make(chan error, 1)
	go func() { _, err := f.p.SendText(ctx, message(e)); sent <- err }()
	<-entered
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f.p.Close() != nil {
				t.Error("concurrent close failed")
			}
		}()
	}
	wg.Wait()
	if !errors.Is(<-sent, context.Canceled) {
		t.Fatal("inflight cancellation mismatch")
	}
	expectSend(t, f.p, ctx, message(e), ErrClosed)
	if _, err := f.p.Bind(context.Background(), nil); !errors.Is(err, ErrClosed) {
		t.Fatal("closed bind mismatch")
	}
	f.p = f.open(t)
	expectSend(t, f.p, bind(t, f.p, e), message(e), ErrSkipped)
}

func TestACKTimeBoundaries(t *testing.T) {
	for _, variant := range []string{"stale-direct", "expired-continuation", "rollback"} {
		t.Run(variant, func(t *testing.T) {
			f := newFixture(t, nil)
			direct := f.event("group", "user", "hello", true)
			if variant == "expired-continuation" {
				expectSend(t, f.p, bind(t, f.p, direct), message(direct), nil)
				f.tick(90 * time.Second)
				direct = f.event("group", "user", "继续", false)
			}
			ctx := bind(t, f.p, direct)
			entered, release := make(chan struct{}), make(chan struct{})
			f.p.sender = senderFunc(func(ctx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				close(entered)
				select {
				case <-release:
					return qq.SentMessage{ID: "sent"}, nil
				case <-ctx.Done():
					return qq.SentMessage{}, ctx.Err()
				}
			})
			done := make(chan ackResult, 1)
			go func() { sent, err := f.p.SendText(ctx, message(direct)); done <- ackResult{sent, err} }()
			<-entered
			switch variant {
			case "stale-direct":
				f.tick(maxEventAge + time.Nanosecond)
			case "expired-continuation":
				f.tick(30 * time.Second)
			case "rollback":
				f.tick(30 * time.Second)
				bind(t, f.p)
				f.tick(-time.Second)
			}
			close(release)
			expectACK(t, <-done)
			if variant == "rollback" {
				f.tick(time.Second)
			}
			calls := int64(1)
			if variant == "expired-continuation" {
				calls++
			}
			expectACKReservation(t, f, f.p, ctx, message(direct), calls)
			var expires int64
			if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || expires != 0 {
				t.Fatal("invalid ACK refreshed session")
			}
		})
	}
}

func TestWindowTransitionsAndClock(t *testing.T) {
	f := newFixture(t, nil)
	first := f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
	closeEvent := f.event("group", "user", "结束", false)
	next := f.event("group", "user", "继续", false)
	expectSend(t, f.p, bind(t, f.p, next, closeEvent), message(next), nil)
	next = f.event("group", "user", "hello", true)
	ctx := bind(t, f.p, next)
	f.tick(maxEventAge + time.Nanosecond)
	expectSend(t, f.p, ctx, message(next), ErrSkipped)
	bind(t, f.p)
	f.tick(-time.Second)
	empty := bind(t, f.p)
	if b, _ := empty.Value(bindingKey{}).(*binding); b == nil || len(b.candidates) != 0 {
		t.Fatal("clock rollback retained authorization")
	}
}

func TestBindSkippedMasksParent(t *testing.T) {
	for _, reason := range []string{"clock", "capacity", "storage"} {
		t.Run(reason, func(t *testing.T) {
			f := newFixture(t, nil)
			first := f.event("group", "user", "hello", true)
			parent := bind(t, f.p, first)
			next := f.event("group", "other", "hello", true)
			switch reason {
			case "clock":
				f.tick(-time.Nanosecond)
			case "capacity":
				// Only the fixture database is populated. The first candidate is
				// accepted before the second hits the cap and rolls back the window.
				_, err := f.p.db.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO actors(actor,token,received,occurred,sequence,mode,expires)
SELECT 'filler-'||x,'',0,0,0,0,0 FROM n`, maxActors-1)
				if err != nil {
					t.Fatal(err)
				}
			case "storage":
				_, err := f.p.db.Exec(`CREATE TRIGGER fail_insert BEFORE INSERT ON actors
WHEN NEW.actor NOT IN (SELECT actor FROM actors)
BEGIN SELECT RAISE(ABORT, 'test failure'); END;`)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, err := f.p.Bind(parent, []event.Event{first, next})
			if reason == "storage" {
				if !errors.Is(err, ErrStorage) || ctx != nil {
					t.Fatal("storage failure was swallowed")
				}
				return
			}
			if err != nil || ctx == nil {
				t.Fatal("policy skip stopped input context restoration")
			}
			b, _ := ctx.Value(bindingKey{}).(*binding)
			if b == nil || b.owner != f.p || len(b.candidates) != 0 {
				t.Fatal("skipped window leaked parent or partial authorization")
			}
			if reason == "clock" {
				f.tick(time.Nanosecond)
			}
			expectSend(t, f.p, ctx, message(first), ErrSkipped)
			expectSend(t, f.p, ctx, message(next), ErrSkipped)
			if f.calls.Load() != 0 {
				t.Fatal("skipped binding invoked sender")
			}
			// The inherited permission really was live, not expired or spent.
			expectSend(t, f.p, parent, message(first), nil)
		})
	}
}

func TestConfigurationAndIsolation(t *testing.T) {
	f := newFixture(t, nil)
	for _, field := range []string{"account", "bot", "empty-bot"} {
		cfg := f.cfg
		switch field {
		case "account":
			cfg.AccountID = "other"
		case "bot":
			cfg.BotUserID = "other"
		case "empty-bot":
			cfg.BotUserID = ""
		}
		if p, err := Open(context.Background(), cfg, f.p.sender); !errors.Is(err, ErrInvalid) {
			if p != nil {
				_ = p.Close()
			}
			t.Fatal("identity mismatch accepted")
		}
	}
	cfg := f.cfg
	cfg.Path = ":memory:"
	if _, err := Open(context.Background(), cfg, f.p.sender); !errors.Is(err, ErrInvalid) {
		t.Fatal("memory path accepted")
	}
	var typedNil senderFunc
	if _, err := Open(context.Background(), f.cfg, typedNil); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil sender accepted")
	}
	e := f.event("group", "user", "hello", true)
	other := f.open(t)
	expectSend(t, other, bind(t, f.p, e), message(e), ErrSkipped)
}

func TestLegacyMigration(t *testing.T) {
	for _, legacy := range []int{1, 2} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			testLegacyMigration(t, legacy)
		})
	}
}

func testLegacyMigration(t *testing.T, legacy int) {
	f := newFixture(t, func(c *Config) { c.TotalLimit = 1 })
	first := f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
	pending := f.event("group", "pending", "hello", true)
	bind(t, f.p, pending)
	// Reconstruct the exact legacy metadata, not just its version number.
	if legacy == 1 {
		if _, err := f.p.db.Exec(`DROP INDEX actor_target;
ALTER TABLE actors DROP COLUMN target;
ALTER TABLE actors DROP COLUMN short_ok;`); err != nil {
			t.Fatal("legacy actor fixture failed")
		}
	}
	if _, err := f.p.db.Exec(`ALTER TABLE policy_meta DROP COLUMN identity;
ALTER TABLE policy_meta DROP COLUMN generation;`); err != nil {
		t.Fatal("legacy metadata fixture failed")
	}
	original := f.cfg
	original.Path = ""
	if _, err := f.p.db.Exec(`UPDATE policy_meta SET version=?,config=?`, legacy, configDigest(original)); err != nil {
		t.Fatal("legacy digest fixture failed")
	}
	for _, field := range []string{"policy", "account", "bot"} {
		changed := f.cfg
		switch field {
		case "policy":
			changed.TotalLimit++
		case "account":
			changed.AccountID = "other"
		case "bot":
			changed.BotUserID = "other"
		}
		if p, err := Open(context.Background(), changed, f.p.sender); !errors.Is(err, ErrInvalid) {
			if p != nil {
				_ = p.Close()
			}
			t.Fatal("legacy identity guessed from changed config")
		}
	}
	if err := f.p.Close(); err != nil {
		t.Fatal("legacy close failed")
	}
	f.p = f.open(t)
	var version, reservations int
	var clock, expires int64
	if err := f.p.db.QueryRow(`SELECT version,clock FROM policy_meta WHERE id=1`).Scan(&version, &clock); err != nil || version != 3 || clock != f.now().UnixNano() {
		t.Fatal("migration lost clock or version")
	}
	if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&reservations); err != nil || reservations != 1 {
		t.Fatal("migration lost reservations")
	}
	if err := f.p.db.QueryRow(`SELECT COALESCE(MAX(expires),0) FROM actors`).Scan(&expires); err != nil || expires != 0 {
		t.Fatal("legacy session retained without group evidence")
	}
	// After identity is established, policy changes are safe on the same file.
	f.cfg.Aliases = []string{"新助手"}
	f.p = f.open(t)
	expectSend(t, f.p, bind(t, f.p, pending), message(pending), ErrSkipped)
	expectSend(t, f.p, bind(t, f.p, first), message(first), ErrSkipped)
	next := f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, next), message(next), ErrSkipped)
	f.tick(time.Minute)
	next = f.event("group", "user", "继续", false)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
	f.tick(time.Minute)
	next = f.event("group", "user", "hello", true)
	expectSend(t, f.p, bind(t, f.p, next), message(next), nil)
}

func TestGroupTurnDuringACK(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprint(continued), func(t *testing.T) {
			f := newFixture(t, nil)
			second := f.open(t)
			first := f.event("group", "user", "hello", true)
			if continued {
				expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
				f.tick(time.Minute)
				first = f.event("group", "user", "继续", false)
			}
			ctx := bind(t, f.p, first)
			entered, release := make(chan struct{}), make(chan struct{})
			f.p.sender = senderFunc(func(ctx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				close(entered)
				select {
				case <-release:
					return qq.SentMessage{ID: "sent"}, nil
				case <-ctx.Done():
					return qq.SentMessage{}, ctx.Err()
				}
			})
			done := make(chan ackResult, 1)
			go func() { sent, err := f.p.SendText(ctx, message(first)); done <- ackResult{sent, err} }()
			<-entered
			bind(t, second, f.event("group", "other", "天气如何", false))
			close(release)
			expectACK(t, <-done)
			calls := int64(1)
			if continued {
				calls++
			}
			expectACKReservation(t, f, f.p, ctx, message(first), calls)
			if continued {
				var expires int64
				actor := digest(digest(f.cfg.AccountID, "group", "group"), "user")
				if err := f.p.db.QueryRow(`SELECT expires FROM actors WHERE actor=?`, actor).Scan(&expires); err != nil || expires != 0 {
					t.Fatal("interrupted bare continuation refreshed session")
				}
			}
			next := f.event("group", "user", "继续", false)
			var want error
			if continued {
				want = ErrSkipped // Active interval remains binding despite group turns.
			}
			expectSend(t, second, bind(t, second, next), message(next), want)
		})
	}
}

func TestActiveConcurrentInstances(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.ActiveLimit = 1 })
	other := f.open(t)
	a := f.event("group", "one", "普通群聊消息", false)
	b := f.event("group", "two", "请問", false)
	editBody(&b, func(c *qq.Content) { c.Text = "普通群聊消息" })
	ca, cb := bind(t, f.p, a), bind(t, other, b)
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() { <-start; _, err := f.p.SendText(ca, message(a)); done <- err }()
	go func() { <-start; _, err := other.SendText(cb, message(b)); done <- err }()
	close(start)
	success := 0
	for i := 0; i < 2; i++ {
		err := <-done
		if err == nil {
			success++
		} else if !errors.Is(err, ErrSkipped) {
			t.Fatal("active concurrency error")
		}
	}
	if success != 1 {
		t.Fatal("active quota exceeded")
	}
}

func TestPolicyGenerationSwitch(t *testing.T) {
	changes := map[string]func(*Config){
		"candidate-semantics": func(c *Config) {},
		"group-candidates-v1": func(c *Config) {},
		"aliases":             func(c *Config) { c.Aliases = []string{"新助手"} },
		"proactive":           func(c *Config) { c.Proactive = true },
		"total-limit":         func(c *Config) { c.TotalLimit++ },
		"total-window":        func(c *Config) { c.TotalWindow += time.Minute },
		"active-limit":        func(c *Config) { c.ActiveLimit++ },
		"active-window":       func(c *Config) { c.ActiveWindow += time.Minute },
		"active-interval":     func(c *Config) { c.ActiveInterval += time.Second },
		"session-ttl":         func(c *Config) { c.SessionTTL += time.Minute },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			old, peer := f.p, f.open(t)
			first := f.event("group", "session", "hello", true)
			expectSend(t, old, bind(t, old, first), message(first), nil)
			pending := f.event("group", "pending", "hello", true)
			bound, peerBound := bind(t, old, pending), bind(t, peer, pending)
			if name == "candidate-semantics" || name == "group-candidates-v1" {
				original := f.cfg
				original.Path, original.AccountID, original.BotUserID = "", "", ""
				fingerprint := configDigest(original)
				if name == "group-candidates-v1" {
					fingerprint = digest("group-candidates-v1", fingerprint)
				}
				if _, err := old.db.Exec(`UPDATE policy_meta SET config=?`, fingerprint); err != nil {
					t.Fatal("old candidate fingerprint fixture failed")
				}
				for _, field := range []string{"account", "bot"} {
					changed := f.cfg
					if field == "account" {
						changed.AccountID = "other"
					} else {
						changed.BotUserID = "other"
					}
					p, err := Open(context.Background(), changed, old.sender)
					if p != nil {
						_ = p.Close()
					}
					if err != ErrInvalid {
						t.Fatal("semantic upgrade lost identity check")
					}
				}
			}
			change(&f.cfg)
			current := f.open(t)
			if current.generation != old.generation+1 {
				t.Fatal("policy change did not increment generation")
			}
			var count int
			var clock int64
			if err := current.db.QueryRow(`SELECT clock FROM policy_meta`).Scan(&clock); err != nil || clock != f.now().UnixNano() {
				t.Fatal("policy switch changed clock")
			}
			if err := current.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != 1 {
				t.Fatal("policy switch lost reservations")
			}
			if err := current.db.QueryRow(`SELECT COUNT(*) FROM actors WHERE mode<>0 OR expires<>0 OR short_ok<>0`).Scan(&count); err != nil || count != 0 {
				t.Fatal("policy switch retained authorization")
			}
			expectSend(t, old, bound, message(pending), ErrSkipped)
			expectSend(t, peer, peerBound, message(pending), ErrSkipped)
			fresh := f.event("group", "fresh", "hello", true)
			masked, err := old.Bind(bound, []event.Event{fresh})
			if err != nil {
				t.Fatal("stale bind blocked input restoration")
			}
			b, _ := masked.Value(bindingKey{}).(*binding)
			if b == nil || len(b.candidates) != 0 {
				t.Fatal("stale bind retained candidates")
			}
			expectSend(t, current, bound, message(pending), ErrSkipped)
			expectSend(t, current, bind(t, current, pending), message(pending), ErrSkipped)
			expectSend(t, current, bind(t, current, first), message(first), ErrSkipped)
			next := f.event("group", "session", "继续", false)
			expectSend(t, current, bind(t, current, next), message(next), nil)
			shared := f.open(t)
			if shared.generation != current.generation {
				t.Fatal("identical config changed generation")
			}
			ca, cb := bind(t, current, fresh), bind(t, shared, fresh)
			start, done := make(chan struct{}), make(chan error, 2)
			go func() { <-start; _, err := current.SendText(ca, message(fresh)); done <- err }()
			go func() { <-start; _, err := shared.SendText(cb, message(fresh)); done <- err }()
			close(start)
			success := 0
			for i := 0; i < 2; i++ {
				if err := <-done; err == nil {
					success++
				} else if !errors.Is(err, ErrSkipped) {
					t.Fatal("shared send failed")
				}
			}
			if success != 1 {
				t.Fatal("new generation lost atomic idempotency")
			}
		})
	}
}

func TestGenerationABAAndDelayedACK(t *testing.T) {
	for _, variant := range []string{"config", "aba", "candidate-semantics"} {
		t.Run(variant, func(t *testing.T) {
			aba := variant == "aba"
			f := newFixture(t, nil)
			original := f.cfg
			old := f.p
			pending := f.event("group", "pending", "hello", true)
			bound := bind(t, old, pending)
			first := f.event("group", "user", "hello", true)
			ctx := bind(t, old, first)
			entered, release := make(chan struct{}), make(chan struct{})
			old.sender = senderFunc(func(ctx context.Context, _ qq.TextMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				close(entered)
				select {
				case <-release:
					return qq.SentMessage{ID: "sent"}, nil
				case <-ctx.Done():
					return qq.SentMessage{}, ctx.Err()
				}
			})
			done := make(chan ackResult, 1)
			go func() { sent, err := old.SendText(ctx, message(first)); done <- ackResult{sent, err} }()
			<-entered
			if variant == "candidate-semantics" {
				legacy := original
				legacy.Path, legacy.AccountID, legacy.BotUserID = "", "", ""
				if _, err := old.db.Exec(`UPDATE policy_meta SET config=?`, configDigest(legacy)); err != nil {
					t.Fatal("old candidate fingerprint fixture failed")
				}
			} else {
				f.cfg.Aliases = []string{"新助手"}
			}
			middle := f.open(t)
			if middle.generation != old.generation+1 {
				t.Fatal("upgrade generation mismatch")
			}
			current := middle
			if aba {
				f.cfg = original
				current = f.open(t)
				if current.generation != old.generation+2 {
					t.Fatal("ABA generation mismatch")
				}
			}
			close(release)
			expectACK(t, <-done)
			expectACKReservation(t, f, old, ctx, message(first), 1)
			var authorized, reservations int
			if err := current.db.QueryRow(`SELECT COUNT(*) FROM actors WHERE mode<>0 OR expires<>0 OR short_ok<>0`).Scan(&authorized); err != nil || authorized != 0 {
				t.Fatal("old ACK restored authorization")
			}
			if err := current.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&reservations); err != nil || reservations != 1 {
				t.Fatal("old ACK refunded reservation")
			}
			expectSend(t, old, bound, message(pending), ErrSkipped)
			fresh := f.event("group", "new", "hello", true)
			expectSend(t, old, bind(t, old, fresh), message(fresh), ErrSkipped)
			if aba {
				expectSend(t, middle, bind(t, middle, fresh), message(fresh), ErrSkipped)
			}
			expectSend(t, current, bind(t, current, pending), message(pending), ErrSkipped)
			expectSend(t, current, bind(t, current, fresh), message(fresh), nil)
		})
	}
}

func TestSwitchPreservesBudgetsAndClock(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.TotalLimit = 3; c.ActiveLimit = 1 })
	first := f.event("group", "one", "普通群聊消息", false)
	expectSend(t, f.p, bind(t, f.p, first), message(first), nil)
	f.cfg.TotalLimit = 1
	current := f.open(t)
	next := f.event("group", "two", "hello", true)
	expectSend(t, current, bind(t, current, next), message(next), ErrSkipped)
	f.cfg.TotalLimit = 3
	f.cfg.ActiveInterval = time.Second
	current = f.open(t)
	expectSend(t, current, bind(t, current, first), message(first), ErrSkipped)
	f.tick(-time.Nanosecond)
	next = f.event("group", "rollback", "hello", true)
	expectSend(t, current, bind(t, current, next), message(next), ErrSkipped)
	f.tick(2 * time.Second)
	next = f.event("group", "three", "普通群聊消息", false)
	expectSend(t, current, bind(t, current, next), message(next), ErrSkipped)
	next = f.event("group", "four", "hello", true)
	expectSend(t, current, bind(t, current, next), message(next), nil)
}
