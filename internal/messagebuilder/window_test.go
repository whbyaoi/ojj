package messagebuilder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"ojj/internal/event"
)

func testBuilder(t *testing.T, cfg Config) *Builder {
	t.Helper()
	if cfg.Path == "" {
		cfg.Path = filepath.Join(t.TempDir(), "messages.db")
	}
	b, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

func testEvent(id string) event.Event {
	return event.Event{ID: id, ProducerID: "test", ReceivedAt: time.Unix(1, 0), Input: event.Input{
		EventType: "unknown", OccurredAt: time.Unix(2, 0), Content: "完整正文", SchemaVersion: 1,
	}}
}

func clockAt(at time.Time) func() time.Time { return func() time.Time { return at } }

func acceptAt(t *testing.T, b *Builder, e event.Event, at time.Time) {
	t.Helper()
	if err := b.handle(context.Background(), e, clockAt(at)); err != nil {
		t.Fatal(err)
	}
}

func settleAt(t *testing.T, b *Builder, at time.Time) {
	t.Helper()
	if err := b.settle(context.Background(), clockAt(at)); err != nil {
		t.Fatal(err)
	}
}

func listMessages(t *testing.T, b *Builder) []Message {
	t.Helper()
	ms, err := b.List(context.Background(), MessageQuery{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func TestWindowQuietMaximumAndEmpty(t *testing.T) {
	b := testBuilder(t, Config{})
	start := time.Now().UTC()
	settleAt(t, b, start)
	if len(listMessages(t, b)) != 0 {
		t.Fatal("empty message")
	}
	e := testEvent("one")
	acceptAt(t, b, e, start)
	acceptAt(t, b, e, start.Add(7900*time.Millisecond))
	settleAt(t, b, start.Add(8*time.Second-time.Nanosecond))
	if len(listMessages(t, b)) != 0 {
		t.Fatal("closed early")
	}
	settleAt(t, b, start.Add(8*time.Second))
	ms := listMessages(t, b)
	if len(ms) != 1 || ms[0].CloseReason != ReasonQuiet || !ms[0].OpenedAt.Equal(start) || !ms[0].ClosedAt.Equal(start.Add(8*time.Second)) {
		t.Fatalf("quiet: %+v", ms)
	}
	if ms[0].Text != formatEvents([]event.Event{e}, start, start.Add(8*time.Second)) {
		t.Fatal("snapshot text differs")
	}
	acceptAt(t, b, e, start.Add(20*time.Second))
	settleAt(t, b, start.Add(30*time.Second))
	if len(listMessages(t, b)) != 1 {
		t.Fatal("duplicate window")
	}
	e.Content = "conflict"
	if err := b.Handle(context.Background(), e); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	start = start.Add(time.Minute)
	for i := 0; i < 30; i++ {
		acceptAt(t, b, testEvent(fmt.Sprint(i)), start.Add(time.Duration(i)*time.Second))
	}
	settleAt(t, b, start.Add(30*time.Second-time.Nanosecond))
	if len(listMessages(t, b)) != 1 {
		t.Fatal("maximum closed early")
	}
	settleAt(t, b, start.Add(30*time.Second))
	ms = listMessages(t, b)
	if len(ms) != 2 || len(ms[1].EventIDs) != 30 || ms[1].CloseReason != ReasonMaxWindow || !ms[1].ClosedAt.Equal(start.Add(30*time.Second)) {
		t.Fatalf("max: %+v", ms)
	}
	settleAt(t, b, start.Add(time.Hour))
	if len(listMessages(t, b)) != 2 {
		t.Fatal("duplicate settle")
	}
}

func TestArrivalAtDeadlineAndSnapshot(t *testing.T) {
	b := testBuilder(t, Config{})
	start := time.Now()
	e := testEvent("first")
	e.Source = &event.Source{SenderID: "original"}
	acceptAt(t, b, e, start)
	e.Source.SenderID = "mutated"
	acceptAt(t, b, testEvent("second"), start.Add(8*time.Second))
	ms := listMessages(t, b)
	if len(ms) != 1 || !strings.Contains(ms[0].Text, "original") || strings.Contains(ms[0].Text, "mutated") {
		t.Fatal("snapshot not isolated")
	}
	settleAt(t, b, start.Add(16*time.Second))
	if ms = listMessages(t, b); len(ms) != 2 || len(ms[1].EventIDs) != 1 {
		t.Fatal("arrival merged into expired window")
	}
}

func TestRestartWindowPreservesPolicy(t *testing.T) {
	cfg := Config{Path: filepath.Join(t.TempDir(), "messages.db")}
	b := testBuilder(t, cfg)
	start := time.Now()
	acceptAt(t, b, testEvent("one"), start)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.QuietPeriod = time.Hour
	cfg.MaxWindow = 2 * time.Hour
	b = testBuilder(t, cfg)
	if len(listMessages(t, b)) != 0 {
		t.Fatal("close forced seal")
	}
	acceptAt(t, b, testEvent("two"), start.Add(time.Second))
	settleAt(t, b, start.Add(9*time.Second))
	ms := listMessages(t, b)
	if len(ms) != 1 || len(ms[0].EventIDs) != 2 || !ms[0].ClosedAt.Equal(start.Add(9*time.Second)) {
		t.Fatal("restart changed deadline policy")
	}
	acceptAt(t, b, testEvent("three"), start.Add(10*time.Second))
	settleAt(t, b, start.Add(20*time.Second))
	if len(listMessages(t, b)) != 1 {
		t.Fatal("new window ignored new config")
	}
}

func TestGlobalWindowRestartMaximumAndCapacity(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		t.Run(fmt.Sprint("capacity=", capacity), func(t *testing.T) {
			cfg := Config{Path: filepath.Join(t.TempDir(), "messages.db"), QuietPeriod: 3 * time.Second, MaxWindow: 5 * time.Second}
			if capacity {
				cfg.MaxWindowEvents = 3
			}
			b := testBuilder(t, cfg)
			start := time.Now().UTC()
			events := []event.Event{testEvent("first"), testEvent("second"), testEvent("third")}
			for i := range events {
				events[i].ProducerID = fmt.Sprint("producer-", i)
				events[i].Source = &event.Source{ConversationID: fmt.Sprint("conversation-", i)}
			}
			acceptAt(t, b, events[0], start)
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.QuietPeriod, cfg.MaxWindow = time.Hour, 2*time.Hour
			cfg.MaxWindowEvents = 100
			b = testBuilder(t, cfg)
			acceptAt(t, b, events[1], start.Add(2*time.Second))
			acceptAt(t, b, events[2], start.Add(4*time.Second))
			closed := start.Add(5 * time.Second)
			reason := ReasonMaxWindow
			if capacity {
				closed, reason = start.Add(4*time.Second), ReasonCapacity
			} else {
				settleAt(t, b, closed.Add(-time.Nanosecond))
				if len(listMessages(t, b)) != 0 {
					t.Fatal("maximum closed early")
				}
				settleAt(t, b, closed)
			}
			ms := listMessages(t, b)
			if len(ms) != 1 || len(ms[0].EventIDs) != 3 || ms[0].CloseReason != reason || !ms[0].ClosedAt.Equal(closed) {
				t.Fatal("global window lost restart policy")
			}
			if ms[0].Text != formatEvents(events, start, closed) {
				t.Fatal("cross-conversation snapshot or order changed")
			}
		})
	}
}

func TestCapacityNoTruncationAndRetention(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 2, MaxStoredEvents: 3})
	start := time.Now()
	for i := 0; i < 3; i++ {
		acceptAt(t, b, testEvent(fmt.Sprint(i)), start)
	}
	ms := listMessages(t, b)
	if len(ms) != 1 || ms[0].CloseReason != ReasonCapacity || len(ms[0].EventIDs) != 2 {
		t.Fatal("event capacity")
	}
	if err := b.Handle(context.Background(), testEvent("extra")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("retention cap: %v", err)
	}
	acceptAt(t, b, testEvent("0"), start)
	settleAt(t, b, start.Add(8*time.Second))
	if ms = listMessages(t, b); len(ms) != 2 || ms[1].EventIDs[0] != "2" {
		t.Fatal("lost event")
	}
	e := testEvent("bytes1")
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	second := testBuilder(t, Config{MaxWindowBytes: len(raw) + 1})
	acceptAt(t, second, e, start)
	acceptAt(t, second, testEvent("bytes2"), start)
	if ms = listMessages(t, second); len(ms) != 1 || ms[0].CloseReason != ReasonCapacity {
		t.Fatal("byte capacity")
	}
	e = testEvent("oversized")
	e.Content = strings.Repeat("x", 600)
	if err := second.Handle(context.Background(), e); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	settleAt(t, second, start.Add(8*time.Second))
	if len(listMessages(t, second)) != 2 {
		t.Fatal("oversized input disturbed active window")
	}
}

func TestConcurrentDedupAndSettle(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 1})
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Handle(context.Background(), testEvent("same")); err != nil {
				t.Error(err)
			}
			if err := b.settle(context.Background(), time.Now); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if ms := listMessages(t, b); len(ms) != 1 || len(ms[0].EventIDs) != 1 {
		t.Fatal("concurrent duplication")
	}
}

func TestOwnershipPermissionsAndCanceledOperations(t *testing.T) {
	b := testBuilder(t, Config{})
	other, err := Open(context.Background(), b.cfg)
	if other != nil {
		other.Close()
		t.Fatal("second owner")
	}
	if !errors.Is(err, ErrOwned) {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias.db")
	if err := os.Symlink(b.cfg.Path, alias); err != nil {
		t.Fatal(err)
	}
	if other, err = Open(context.Background(), Config{Path: alias}); !errors.Is(err, ErrOwned) {
		if other != nil {
			other.Close()
		}
		t.Fatal(err)
	}
	for _, path := range []string{b.cfg.Path, b.cfg.Path + ".lock", b.cfg.Path + "-wal", b.cfg.Path + "-shm"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("permissions %s: %v", path, info.Mode())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Handle(ctx, testEvent("canceled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(listMessages(t, b)) != 0 {
		t.Fatal("canceled persisted")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Handle(context.Background(), testEvent("closed")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	b = testBuilder(t, b.cfg)
	if _, err := b.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestSealRollbackIsAtomic(t *testing.T) {
	b := testBuilder(t, Config{})
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	settleAt(t, b, time.Now())
	if _, found, err := b.claim(context.Background()); err != nil || found || buf.Len() != 0 {
		t.Fatal("empty polling must be silent")
	}
	start := time.Now()
	acceptAt(t, b, testEvent("one"), start)
	_, err := b.db.Exec(`CREATE TRIGGER reject_binding BEFORE UPDATE OF message_sequence ON inbox BEGIN SELECT RAISE(ABORT,'test failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.settle(context.Background(), clockAt(start.Add(8*time.Second))); err == nil {
		t.Fatal("expected failure")
	}
	if len(listMessages(t, b)) != 0 {
		t.Fatal("partial message committed")
	}
	if strings.Contains(buf.String(), "window_sealed") || strings.Contains(buf.String(), "test failure") || !strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Fatal("rollback logged success or unsafe error")
	}
	buf.Reset()
	if _, err = b.db.Exec("DROP TRIGGER reject_binding"); err != nil {
		t.Fatal(err)
	}
	settleAt(t, b, start.Add(8*time.Second))
	if ms := listMessages(t, b); len(ms) != 1 || len(ms[0].EventIDs) != 1 {
		t.Fatal("rollback lost snapshot")
	}
	if !strings.Contains(buf.String(), `"stage":"window_sealed"`) {
		t.Fatal("missing committed seal log")
	}
}

func TestEventsSnapshotsByWindow(t *testing.T) {
	b := testBuilder(t, Config{QuietPeriod: time.Second, MaxWindow: time.Minute})
	start := time.Now().UTC()
	first := testEvent("z-first")
	first.EventType = "im.message"
	first.Source = &event.Source{Platform: "qq", ConversationType: "group", ConversationID: "group-A", SenderID: "sender-A"}
	second := testEvent("a-second")
	second.EventType = "im.message"
	second.Source = &event.Source{Platform: "qq", ConversationType: "private", ConversationID: "private-B", SenderID: "sender-B"}
	hb := testEvent("heartbeat")
	hb.EventType, hb.ProducerID = "system.heartbeat", "system.heartbeat"
	hb.Content = `{"version":1,"reason":"idle"}`
	for _, e := range []*event.Event{&first, &hb, &second} {
		e.ReceivedAt, e.OccurredAt = e.ReceivedAt.UTC(), e.OccurredAt.UTC()
	}
	want, err := json.Marshal([]event.Event{first, hb, second})
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range []event.Event{first, hb, second} {
		acceptAt(t, b, e, start.Add(time.Duration(i)*time.Millisecond))
	}
	first.Source.SenderID = "mutated after acceptance"
	settleAt(t, b, start.Add(2*time.Second))
	other := testEvent("other-window")
	acceptAt(t, b, other, start.Add(3*time.Second))
	settleAt(t, b, start.Add(4*time.Second))
	acceptAt(t, b, testEvent("unsealed"), start.Add(5*time.Second))
	ms := listMessages(t, b)
	if len(ms) != 2 {
		t.Fatalf("windows=%d", len(ms))
	}
	// Snapshots must not be reconstructed from formatted text or event_ids.
	if _, err := b.db.Exec("UPDATE messages SET text='untrusted',event_ids='[]',status='succeeded' WHERE id=?", ms[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = testBuilder(t, b.cfg)
	for i := 0; i < 2; i++ {
		got, err := b.Events(context.Background(), ms[0].ID)
		raw, marshalErr := json.Marshal(got)
		if err != nil || marshalErr != nil || !bytes.Equal(raw, want) {
			t.Fatalf("snapshots=%s want=%s err=%v/%v", raw, want, err, marshalErr)
		}
		got[0].Source.SenderID = "mutated query result"
		got[1].Content = "mutated query body"
		got[2] = event.Event{}
	}
	got, err := b.Events(context.Background(), ms[1].ID)
	if err != nil || len(got) != 1 || got[0].ID != other.ID {
		t.Fatalf("window isolation: %+v %v", got, err)
	}
	for _, id := range []string{"missing", "", first.ID, "unsealed"} {
		if got, err := b.Events(context.Background(), id); !errors.Is(err, ErrNotFound) || got != nil {
			t.Fatalf("unknown product %q: %+v %v", id, got, err)
		}
	}
	if _, err := b.db.Exec("UPDATE inbox SET snapshot='{' WHERE id=?", second.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Events(context.Background(), ms[0].ID); err == nil || got != nil {
		t.Fatalf("partial corrupt snapshot accepted: %+v %v", got, err)
	}
}

func TestEventsOperationLifecycle(t *testing.T) {
	b := testBuilder(t, Config{OperationTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Events(ctx, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query: %v", err)
	}
	conn, err := b.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := b.Events(context.Background(), "missing"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded operation: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := b.Events(context.Background(), "missing"); done <- err }()
	deadline := time.Now().Add(time.Second)
	for b.db.Stats().WaitCount < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.db.Stats().WaitCount < 2 {
		t.Fatal("query did not wait for connection")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("close did not cancel query: %v", err)
	}
	if _, err := b.Events(context.Background(), "missing"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed query: %v", err)
	}
}

func TestDeliveryPersistenceLogs(t *testing.T) {
	b := testBuilder(t, Config{})
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	for i, outcome := range []error{nil, context.Canceled, errors.Join(context.Canceled, errors.New("private-handler-error"))} {
		start := time.Now()
		e := testEvent(fmt.Sprintf("private-event-%d", i))
		e.Input.Content = "private-body"
		acceptAt(t, b, e, start)
		settleAt(t, b, start.Add(8*time.Second))
		m, found, err := b.claim(context.Background())
		if err != nil || !found {
			t.Fatal("claim failed")
		}
		buf.Reset()
		if err := b.finish(m, outcome); err != nil {
			t.Fatal("finish failed")
		}
		var record map[string]any
		if json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record) != nil {
			t.Fatal("invalid delivery log")
		}
		want := "INFO"
		if outcome == context.Canceled {
			want = "DEBUG"
		} else if outcome != nil {
			want = "WARN"
		}
		if record["level"] != want || record["persisted"] != true {
			t.Fatal("incorrect retry classification")
		}
		if strings.Contains(buf.String(), "private-") {
			t.Fatal("sensitive delivery data logged")
		}
		buf.Reset()
		if err := b.finish(m, nil); !errors.Is(err, ErrConflict) {
			t.Fatal("lost conflict detection")
		}
		if json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record) != nil || record["level"] != "ERROR" || record["persisted"] != false || record["stage"] != "delivery_persist" {
			t.Fatal("conflict reported delivery success")
		}
		// Reset only this fixture's retry row so the next case can claim in order.
		if _, err := b.db.Exec("UPDATE messages SET status='succeeded' WHERE sequence=?", m.Sequence); err != nil {
			t.Fatal("fixture reset failed")
		}
	}
}
