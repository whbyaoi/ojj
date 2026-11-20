package event

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const reliabilityWait = 5 * time.Second

// 仅使用合成标记，失败时也不输出捕获的日志或错误正文。
const reliabilityPrivateMarker = "synthetic-private-error-body"

type reliabilityPanicError struct{}

func (reliabilityPanicError) Error() string { panic(reliabilityPrivateMarker) }

type reliabilityLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *reliabilityLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *reliabilityLogBuffer) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func reliabilityLogRecords(t *testing.T, raw string) []map[string]any {
	t.Helper()
	if strings.Contains(raw, reliabilityPrivateMarker) {
		t.Error("log leaked private error body")
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal("invalid structured log")
		}
		records = append(records, record)
	}
	return records
}

func reliabilityLogFields(t *testing.T, record map[string]any, want map[string]any) {
	t.Helper()
	for key, value := range want {
		if record[key] != value {
			t.Errorf("unexpected log field %s", key)
		}
	}
}

func TestReliabilityInvokeErrorClassification(t *testing.T) {
	realErr := errors.New(reliabilityPrivateMarker)
	for _, tc := range []struct {
		name     string
		input    error
		sentinel error
	}{
		{"success", nil, nil},
		{"canceled", context.Canceled, context.Canceled},
		{"wrapped_canceled", fmt.Errorf("wrapped: %w", context.Canceled), context.Canceled},
		{"joined_canceled", errors.Join(context.Canceled, context.Canceled), context.Canceled},
		{"timeout", context.DeadlineExceeded, context.DeadlineExceeded},
		{"wrapped_timeout", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), context.DeadlineExceeded},
		{"joined_timeout", errors.Join(context.DeadlineExceeded, context.DeadlineExceeded), context.DeadlineExceeded},
		{"real_error", realErr, nil},
		{"mixed_canceled", errors.Join(context.Canceled, realErr), nil},
		{"mixed_timeout", errors.Join(context.DeadlineExceeded, realErr), nil},
		{"nested_mixed", fmt.Errorf("wrapped: %w", errors.Join(realErr, context.Canceled)), nil},
		{"both_sentinels", errors.Join(context.Canceled, context.DeadlineExceeded), nil},
		{"long_snapshot", errors.New(strings.Repeat("x", 2048)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := invoke(context.Background(), func(context.Context, Event) error { return tc.input }, Event{})
			if tc.input == nil || tc.sentinel != nil {
				if got != tc.sentinel {
					t.Fatal("invoke did not preserve nil or pure cancellation sentinel")
				}
				return
			}
			if got == nil || got == tc.input || errors.Is(got, context.Canceled) || errors.Is(got, context.DeadlineExceeded) || errors.Is(got, realErr) {
				t.Fatal("invoke must snapshot untrusted errors without cancellation identity")
			}
			want := tc.input.Error()
			if len(want) > 1024 {
				want = want[:1024]
			}
			if got.Error() != want {
				t.Fatal("invoke changed error snapshot")
			}
		})
	}
}

func TestReliabilityInvokePanicIsolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    Handler
	}{
		{"handler_panic", func(context.Context, Event) error { panic(reliabilityPrivateMarker) }},
		{"error_method_panic", func(context.Context, Event) error { return reliabilityPanicError{} }},
		{"joined_error_panic", func(context.Context, Event) error { return errors.Join(context.Canceled, reliabilityPanicError{}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := invoke(context.Background(), tc.h, Event{})
			if got == nil || got.Error() != "handler panicked" {
				t.Fatal("invoke did not isolate panic with safe fixed text")
			}
		})
	}
}

func TestReliabilityFinishLoggingAndPersistence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome error
		failure string
	}{
		{"canceled", context.Canceled, "canceled"},
		{"wrapped_canceled", fmt.Errorf("wrapped: %w", context.Canceled), "canceled"},
		{"joined_canceled", errors.Join(context.Canceled, context.Canceled), "canceled"},
		{"timeout", context.DeadlineExceeded, "timeout"},
		{"mixed_canceled", errors.Join(context.Canceled, errors.New(reliabilityPrivateMarker)), "handler_failure"},
		{"mixed_timeout", errors.Join(context.DeadlineExceeded, errors.New(reliabilityPrivateMarker)), "handler_failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs reliabilityLogBuffer
			cfg := reliabilityConfig(t)
			cfg.MaxAttempts = 2
			cfg.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			s := reliabilityOpen(t, cfg)
			reliabilityPark(t, s, SubscriptionSpec{ID: "worker"})
			r := reliabilityPublish(t, reliabilityProducer(t, s, "producer"), reliabilityInput(reliabilityPrivateMarker))
			manual := &Subscription{store: s, id: "worker", ctx: context.Background()}
			for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
				d := reliabilityClaim(t, manual)
				before := len(logs.text())
				if err := manual.finish(d, tc.outcome); err != nil {
					t.Fatal("finish failed")
				}
				stored := reliabilityDelivery(t, s, r.Event.ID, manual.id)
				status, level, message := StatusRetry, "WARN", "event delivery retry scheduled"
				if attempt == cfg.MaxAttempts {
					status, level, message = StatusFailed, "ERROR", "event delivery failed"
					if tc.failure == "timeout" {
						level, message = "WARN", "event delivery stopped"
					}
				}
				if tc.failure == "canceled" {
					level, message = "DEBUG", "event delivery canceled"
				}
				if stored.Status != status || stored.Attempts != attempt || stored.ErrorCount != attempt || stored.AttemptToken != "" || stored.LastErrorAt.IsZero() || stored.LastError != tc.outcome.Error() {
					t.Error("finish changed persisted failure or attempt metadata")
				}
				if status == StatusRetry {
					if stored.NextAttemptAt.IsZero() || !stored.CompletedAt.IsZero() {
						t.Error("retry scheduling metadata missing")
					}
				} else if stored.CompletedAt.IsZero() || !stored.NextAttemptAt.IsZero() {
					t.Error("terminal failure metadata missing")
				}
				records := reliabilityLogRecords(t, logs.text()[before:])
				if len(records) != 1 {
					t.Fatalf("finish emitted %d records, want 1", len(records))
				}
				reliabilityLogFields(t, records[0], map[string]any{
					"level": level, "msg": message, "component": "event", "failure": tc.failure,
					"subscription_id": manual.id, "event_id": r.Event.ID, "attempt": float64(attempt),
				})
				if tc.failure == "canceled" {
					reliabilityLogFields(t, records[0], map[string]any{"status": string(status)})
				}
				// 旧 token 的迟到成功不能改状态，也不能输出成功日志。
				before = len(logs.text())
				if err := manual.finish(d, nil); err != nil {
					t.Fatal("stale finish failed")
				}
				if after := reliabilityDelivery(t, s, r.Event.ID, manual.id); !reflect.DeepEqual(after, stored) {
					t.Error("stale result changed persisted delivery")
				}
				ignored := reliabilityLogRecords(t, logs.text()[before:])
				if len(ignored) != 1 {
					t.Fatal("stale finish must emit one ignored-result log")
				}
				reliabilityLogFields(t, ignored[0], map[string]any{"level": "DEBUG", "msg": "event delivery result ignored"})
			}
			reliabilityLogRecords(t, logs.text())
		})
	}
}

func TestReliabilityFinishWriteFailureDoesNotLogSuccess(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome error
	}{
		{"success", nil},
		{"canceled", context.Canceled},
		{"mixed", errors.Join(context.Canceled, errors.New(reliabilityPrivateMarker))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs reliabilityLogBuffer
			cfg := reliabilityConfig(t)
			cfg.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			s, openErr := Open(context.Background(), cfg)
			if openErr != nil {
				t.Fatal("cannot open test store")
			}
			var expectedCloseErr error
			t.Cleanup(func() {
				closeErr := s.Close()
				if (expectedCloseErr == nil && closeErr != nil) || (expectedCloseErr != nil && !errors.Is(closeErr, expectedCloseErr)) {
					t.Error("unexpected store close result")
				}
				reliabilityLogRecords(t, logs.text())
			})
			reliabilityPark(t, s, SubscriptionSpec{ID: "worker"})
			r := reliabilityPublish(t, reliabilityProducer(t, s, "producer"), reliabilityInput("test event"))
			manual := &Subscription{store: s, id: "worker", ctx: context.Background()}
			d := reliabilityClaim(t, manual)
			stored := reliabilityDelivery(t, s, r.Event.ID, manual.id)
			// 真正拒绝 UPDATE，保留数据库连接以检查原始 processing 状态。
			_, err := s.db.Exec(`CREATE TEMP TRIGGER reject_finish BEFORE UPDATE ON deliveries
 BEGIN SELECT RAISE(ABORT, 'synthetic-private-error-body'); END`)
			if err != nil {
				t.Fatal("cannot install finish failure trigger")
			}
			before := len(logs.text())
			err = manual.finish(d, tc.outcome)
			if err == nil {
				t.Fatal("finish reported success despite rejected database write")
			}
			if records := reliabilityLogRecords(t, logs.text()[before:]); len(records) != 0 {
				t.Error("failed database write emitted a committed-result log")
			}
			if after := reliabilityDelivery(t, s, r.Event.ID, manual.id); !reflect.DeepEqual(after, stored) {
				t.Error("failed database write changed delivery")
			}
			// 与调度器相同地上报持久化失败，不能输出 SQL 错误正文。
			before = len(logs.text())
			expectedCloseErr = err
			s.recordError("delivery", err)
			if !errors.Is(s.LastError(), err) {
				t.Error("storage failure was not retained")
			}
			records := reliabilityLogRecords(t, logs.text()[before:])
			if len(records) != 1 {
				t.Fatal("storage failure must emit one error log")
			}
			reliabilityLogFields(t, records[0], map[string]any{
				"level": "ERROR", "component": "event", "stage": "delivery", "failure": "storage_failure",
			})
		})
	}
}

func TestReliabilityLogFailureCancellationClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		level   string
		failure string
	}{
		{"canceled", context.Canceled, "DEBUG", "canceled"},
		{"wrapped_canceled", fmt.Errorf("wrapped: %w", context.Canceled), "DEBUG", "canceled"},
		{"timeout", context.DeadlineExceeded, "WARN", "timeout"},
		{"mixed_canceled", errors.Join(context.Canceled, errors.New(reliabilityPrivateMarker)), "ERROR", "storage_failure"},
		{"mixed_timeout", errors.Join(context.DeadlineExceeded, errors.New(reliabilityPrivateMarker)), "ERROR", "storage_failure"},
		{"nested_mixed", fmt.Errorf("wrapped: %w", errors.Join(errors.New(reliabilityPrivateMarker), context.Canceled)), "ERROR", "storage_failure"},
		{"panic_error", errors.Join(context.Canceled, reliabilityPanicError{}), "ERROR", "storage_failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs reliabilityLogBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})).With("component", "event")
			logFailure(logger, "cleanup", tc.err)
			records := reliabilityLogRecords(t, logs.text())
			if len(records) != 1 {
				t.Fatal("logFailure must emit exactly one record")
			}
			reliabilityLogFields(t, records[0], map[string]any{
				"level": tc.level, "msg": "event operation failed", "component": "event", "stage": "cleanup", "failure": tc.failure,
			})
		})
	}
}

func reliabilityConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Path:         filepath.Join(t.TempDir(), "events.sqlite"),
		PollInterval: 5 * time.Millisecond, RetryInitial: 5 * time.Millisecond,
		RetryMax: 10 * time.Millisecond, HandlerTimeout: 3 * time.Second,
		OperationTimeout: 2 * time.Second, CleanupInterval: time.Hour,
		Retention: time.Hour, MaxAttempts: 3,
	}
}

func reliabilityOpen(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func reliabilityProducer(t *testing.T, s *Store, id string) *Producer {
	t.Helper()
	p, err := s.BindProducer(id)
	if err != nil {
		t.Fatalf("BindProducer: %v", err)
	}
	return p
}

func reliabilityInput(content string) Input {
	return Input{EventType: "test.event", OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Content: content}
}

func reliabilityPublish(t *testing.T, p *Producer, in Input) PublishResult {
	t.Helper()
	r, err := p.Publish(context.Background(), in)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return r
}

func reliabilitySubscribe(t *testing.T, s *Store, spec SubscriptionSpec, h Handler) *Subscription {
	t.Helper()
	sub, err := s.Subscribe(context.Background(), spec, h)
	if err != nil {
		t.Fatalf("Subscribe %s: %v", spec.ID, err)
	}
	t.Cleanup(sub.Close)
	return sub
}

func reliabilityReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(reliabilityWait):
		t.Fatal("timed out waiting for channel")
		var zero T
		return zero
	}
}

func reliabilityEventually(t *testing.T, description string, check func() bool) {
	t.Helper()
	timer := time.NewTimer(reliabilityWait)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("timed out: %s", description)
		case <-ticker.C:
		}
	}
}

func reliabilityDetach(t *testing.T, sub *Subscription) {
	t.Helper()
	sub.Close()
	reliabilityReceive(t, sub.Done())
	if err := sub.Err(); err != nil {
		t.Fatalf("subscription stopped with error: %v", err)
	}
}

func reliabilityDeliveries(t *testing.T, s *Store, q DeliveryQuery) []Delivery {
	t.Helper()
	ds, err := s.ListDeliveries(context.Background(), q)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	return ds
}

func reliabilityDelivery(t *testing.T, s *Store, eventID, subID string) Delivery {
	t.Helper()
	ds := reliabilityDeliveries(t, s, DeliveryQuery{EventID: eventID, SubscriptionID: subID})
	if len(ds) != 1 {
		t.Fatalf("delivery %s/%s: got %d rows", eventID, subID, len(ds))
	}
	return ds[0]
}

func reliabilityStatus(t *testing.T, s *Store, eventID, subID string, status Status) Delivery {
	t.Helper()
	var d Delivery
	reliabilityEventually(t, fmt.Sprintf("%s/%s reaches %s", eventID, subID, status), func() bool {
		d = reliabilityDelivery(t, s, eventID, subID)
		return d.Status == status
	})
	return d
}

func reliabilityPark(t *testing.T, s *Store, spec SubscriptionSpec) {
	t.Helper()
	sub := reliabilitySubscribe(t, s, spec, func(context.Context, Event) error { return nil })
	reliabilityDetach(t, sub)
}

func TestReliabilityPublishPersistenceAndDeduplication(t *testing.T) {
	s := reliabilityOpen(t, reliabilityConfig(t))
	p := reliabilityProducer(t, s, "producer-a")
	in := reliabilityInput("original")
	in.DeduplicationKey = "key"
	first := reliabilityPublish(t, p, in)
	if first.Duplicate || first.MatchedSubscriptions != 0 || first.Event.ID == "" {
		t.Fatalf("unexpected initial result: %+v", first)
	}
	stored, err := s.Get(context.Background(), first.Event.ID)
	if err != nil || !reflect.DeepEqual(stored, first.Event) {
		t.Fatalf("saved event: %+v, err=%v", stored, err)
	}
	reliabilityPark(t, s, SubscriptionSpec{ID: "new"})
	if ds := reliabilityDeliveries(t, s, DeliveryQuery{}); len(ds) != 0 {
		t.Fatalf("new subscription replayed old event: %+v", ds)
	}
	// Equivalent timezone and default schema version normalize to the same input.
	equivalent := in
	equivalent.OccurredAt = in.OccurredAt.In(time.FixedZone("offset", 8*60*60))
	equivalent.SchemaVersion = 1
	duplicate := reliabilityPublish(t, p, equivalent)
	if !duplicate.Duplicate || duplicate.MatchedSubscriptions != 0 || !reflect.DeepEqual(duplicate.Event, first.Event) {
		t.Fatalf("duplicate result: %+v", duplicate)
	}
	mutations := map[string]func(*Input){
		"content":     func(v *Input) { v.Content = "changed" },
		"type":        func(v *Input) { v.EventType = "other.event" },
		"schema":      func(v *Input) { v.SchemaVersion = 2 },
		"occurred_at": func(v *Input) { v.OccurredAt = v.OccurredAt.Add(time.Second) },
		"source":      func(v *Input) { v.Source = &Source{Platform: "other"} },
		"correlation": func(v *Input) { v.CorrelationID = "other" },
		"causation":   func(v *Input) { v.CausationEventID = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := in
			mutate(&changed)
			want := ErrConflict
			if name == "schema" {
				want = ErrInvalid // 首版仅接受版本 1，非法输入先于去重检查拒绝。
			}
			if _, err := p.Publish(context.Background(), changed); !errors.Is(err, want) {
				t.Fatalf("changed input: got %v, want %v", err, want)
			}
		})
	}
	other := reliabilityPublish(t, reliabilityProducer(t, s, "producer-b"), in)
	if other.Duplicate || other.Event.ID == first.Event.ID || other.MatchedSubscriptions != 1 {
		t.Fatalf("producer-scoped deduplication: %+v", other)
	}
	future := in
	future.DeduplicationKey = "future"
	next := reliabilityPublish(t, p, future)
	again := reliabilityPublish(t, p, future)
	if next.MatchedSubscriptions != 1 || !again.Duplicate || again.Event.ID != next.Event.ID || again.MatchedSubscriptions != 1 {
		t.Fatalf("future event/duplicate: %+v / %+v", next, again)
	}
	if ds := reliabilityDeliveries(t, s, DeliveryQuery{}); len(ds) != 2 {
		t.Fatalf("duplicate created deliveries: %+v", ds)
	}
	plain1 := reliabilityPublish(t, p, reliabilityInput("no key"))
	plain2 := reliabilityPublish(t, p, reliabilityInput("no key"))
	if plain1.Event.ID == plain2.Event.ID || plain2.Duplicate {
		t.Fatal("keyless inputs must not deduplicate")
	}
	events, err := s.List(context.Background(), EventQuery{})
	if err != nil || len(events) != 5 {
		t.Fatalf("persisted events: count=%d err=%v", len(events), err)
	}
}

func TestReliabilityBroadcastIsolationAndOrder(t *testing.T) {
	cfg := reliabilityConfig(t)
	cfg.HandlerTimeout = 15 * time.Second
	s := reliabilityOpen(t, cfg)
	p := reliabilityProducer(t, s, "producer")
	blocked, slow, fast := make(chan struct{}), make(chan Event, 8), make(chan Event, 8)
	var release sync.Once
	defer release.Do(func() { close(blocked) })
	reliabilitySubscribe(t, s, SubscriptionSpec{ID: "slow"}, func(ctx context.Context, e Event) error {
		slow <- e
		select {
		case <-blocked:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	reliabilitySubscribe(t, s, SubscriptionSpec{ID: "fast"}, func(_ context.Context, e Event) error {
		fast <- e
		return nil
	})
	var published []Event
	for i := 0; i < 4; i++ {
		r := reliabilityPublish(t, p, reliabilityInput(fmt.Sprint(i)))
		if r.MatchedSubscriptions != 2 {
			t.Fatalf("broadcast matched %d subscriptions", r.MatchedSubscriptions)
		}
		published = append(published, r.Event)
	}
	firstSlow := reliabilityReceive(t, slow)
	for _, want := range published {
		got := reliabilityReceive(t, fast)
		if got.ID != want.ID || got.Sequence != want.Sequence {
			t.Fatalf("fast order: got %+v want %+v", got, want)
		}
		reliabilityStatus(t, s, want.ID, "fast", StatusSucceeded)
	}
	if firstSlow.ID != published[0].ID {
		t.Fatalf("slow started at %s", firstSlow.ID)
	}
	if d := reliabilityDelivery(t, s, published[0].ID, "slow"); d.Status != StatusProcessing {
		t.Fatalf("slow callback not isolated: %+v", d)
	}
	for _, e := range published[1:] {
		if d := reliabilityDelivery(t, s, e.ID, "slow"); d.Status != StatusPending || d.Attempts != 0 {
			t.Fatalf("slow subscription overtook blocked event: %+v", d)
		}
	}
	release.Do(func() { close(blocked) })
	for _, want := range published[1:] {
		if got := reliabilityReceive(t, slow); got.ID != want.ID {
			t.Fatalf("slow order: got %s want %s", got.ID, want.ID)
		}
	}
	for _, e := range published {
		reliabilityStatus(t, s, e.ID, "slow", StatusSucceeded)
	}
}

func TestReliabilityRetryExhaustionAndPanic(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			s := reliabilityOpen(t, reliabilityConfig(t))
			p := reliabilityProducer(t, s, "producer")
			reliabilityPark(t, s, SubscriptionSpec{ID: "worker"})
			bad := reliabilityPublish(t, p, reliabilityInput("bad"))
			good := reliabilityPublish(t, p, reliabilityInput("good"))
			calls := make(chan string, 16)
			reliabilitySubscribe(t, s, SubscriptionSpec{ID: "worker"}, func(_ context.Context, e Event) error {
				calls <- e.ID
				if e.ID == bad.Event.ID {
					if mode == "panic" {
						panic("private panic payload")
					}
					return errors.New("temporary failure")
				}
				return nil
			})
			d := reliabilityStatus(t, s, bad.Event.ID, "worker", StatusFailed)
			if d.Attempts != 3 || d.ErrorCount != 3 || d.CompletedAt.IsZero() || d.LastErrorAt.IsZero() || d.AttemptToken != "" {
				t.Fatalf("exhaustion metadata: %+v", d)
			}
			wantError := "temporary failure"
			if mode == "panic" {
				wantError = "handler panicked"
			}
			if d.LastError != wantError {
				t.Fatalf("last error = %q, want %q", d.LastError, wantError)
			}
			d = reliabilityStatus(t, s, good.Event.ID, "worker", StatusSucceeded)
			if d.Attempts != 1 || d.ErrorCount != 0 {
				t.Fatalf("following event: %+v", d)
			}
			for _, want := range []string{bad.Event.ID, bad.Event.ID, bad.Event.ID, good.Event.ID} {
				if got := reliabilityReceive(t, calls); got != want {
					t.Fatalf("retry order: got %s want %s", got, want)
				}
			}
			select {
			case extra := <-calls:
				t.Fatalf("unexpected extra callback: %s", extra)
			default:
			}
		})
	}
}

func TestReliabilityStableFilterBindingAndBacklog(t *testing.T) {
	s := reliabilityOpen(t, reliabilityConfig(t))
	filter := Filter{ProducerIDs: []string{"b", "a", "a"}, EventTypes: []string{"two", "one", "one"}}
	spec := SubscriptionSpec{ID: "filtered", Filter: filter}
	sub := reliabilitySubscribe(t, s, spec, func(context.Context, Event) error { return nil })
	if extra, err := s.Subscribe(context.Background(), spec, func(context.Context, Event) error { return nil }); !errors.Is(err, ErrBound) {
		if extra != nil {
			extra.Close()
		}
		t.Fatalf("second active binding: %v", err)
	}
	reliabilityDetach(t, sub)
	filter.ProducerIDs[0] = "mutated"
	info, err := s.GetSubscription(context.Background(), spec.ID)
	canonical := Filter{ProducerIDs: []string{"a", "b"}, EventTypes: []string{"one", "two"}}
	if err != nil || !info.Enabled || !reflect.DeepEqual(info.Filter, canonical) {
		t.Fatalf("stable canonical filter: %+v err=%v", info, err)
	}
	if _, err := s.Subscribe(context.Background(), SubscriptionSpec{ID: spec.ID}, func(context.Context, Event) error { return nil }); !errors.Is(err, ErrFilterConflict) {
		t.Fatalf("changed filter: %v", err)
	}
	var expected []string
	for _, producer := range []string{"a", "b", "c"} {
		p := reliabilityProducer(t, s, producer)
		for _, kind := range []string{"one", "two", "three"} {
			in := reliabilityInput(producer + kind)
			in.EventType = kind
			r := reliabilityPublish(t, p, in)
			matches := 0
			if producer != "c" && kind != "three" {
				matches = 1
				expected = append(expected, r.Event.ID)
			}
			if r.MatchedSubscriptions != matches {
				t.Fatalf("filter %s/%s: %+v", producer, kind, r)
			}
		}
	}
	ds := reliabilityDeliveries(t, s, DeliveryQuery{SubscriptionID: spec.ID})
	if len(ds) != 4 {
		t.Fatalf("detached backlog: %+v", ds)
	}
	for _, d := range ds {
		if d.Status != StatusPending || d.Attempts != 0 {
			t.Fatalf("backlog consumed while detached: %+v", d)
		}
	}
	seen := make(chan string, 8)
	reliabilitySubscribe(t, s, SubscriptionSpec{ID: spec.ID, Filter: canonical}, func(_ context.Context, e Event) error { seen <- e.ID; return nil })
	for _, want := range expected {
		if got := reliabilityReceive(t, seen); got != want {
			t.Fatalf("backlog order: got %s want %s", got, want)
		}
		reliabilityStatus(t, s, want, spec.ID, StatusSucceeded)
	}
}

// 不启动调度器，内部 claim 后不 finish，模拟崩溃留下的持久化 processing。
// 这是数据库恢复测试，并非真实杀进程测试。
func reliabilityClaim(t *testing.T, sub *Subscription) Delivery {
	t.Helper()
	var d Delivery
	reliabilityEventually(t, "claim available delivery", func() bool {
		_, got, found, err := sub.claim()
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		d = got
		return found
	})
	return d
}

func TestReliabilityReopenProcessingAndStaleToken(t *testing.T) {
	cfg := reliabilityConfig(t)
	s := reliabilityOpen(t, cfg)
	spec := SubscriptionSpec{ID: "recover"}
	reliabilityPark(t, s, spec)
	p := reliabilityProducer(t, s, "producer")
	in := reliabilityInput("recover me")
	in.DeduplicationKey = "persisted-key"
	r := reliabilityPublish(t, p, in)
	manual := &Subscription{store: s, id: spec.ID, ctx: context.Background()}
	first := reliabilityClaim(t, manual)
	if first.Attempts != 1 || first.AttemptToken == "" {
		t.Fatalf("first claim: %+v", first)
	}
	if err := manual.finish(first, errors.New("first failure")); err != nil {
		t.Fatal(err)
	}
	interrupted := reliabilityClaim(t, manual)
	if interrupted.Attempts != 2 {
		t.Fatalf("second claim: %+v", interrupted)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = reliabilityOpen(t, cfg)
	stored, err := s.Get(context.Background(), r.Event.ID)
	if err != nil || !reflect.DeepEqual(stored, r.Event) {
		t.Fatalf("reopened event: %+v %v", stored, err)
	}
	d := reliabilityDelivery(t, s, r.Event.ID, spec.ID)
	if d.Status != StatusRetry || d.Attempts != 2 || d.ErrorCount != 2 || d.AttemptToken != "" || d.LastError != "interrupted by store restart" {
		t.Fatalf("recovery must retain attempt history: %+v", d)
	}
	dup := reliabilityPublish(t, reliabilityProducer(t, s, "producer"), in)
	if !dup.Duplicate || dup.Event.ID != r.Event.ID || dup.MatchedSubscriptions != 1 {
		t.Fatalf("reopened dedup: %+v", dup)
	}
	manual = &Subscription{store: s, id: spec.ID, ctx: context.Background()}
	current := reliabilityClaim(t, manual)
	if current.Attempts != 3 || current.AttemptToken == interrupted.AttemptToken {
		t.Fatalf("new attempt: %+v", current)
	}
	before := reliabilityDelivery(t, s, r.Event.ID, spec.ID)
	for _, outcome := range []error{nil, errors.New("late failure")} {
		if err := manual.finish(interrupted, outcome); err != nil {
			t.Fatal(err)
		}
		if after := reliabilityDelivery(t, s, r.Event.ID, spec.ID); !reflect.DeepEqual(after, before) {
			t.Fatalf("stale token changed current attempt: before=%+v after=%+v", before, after)
		}
	}
	if err := manual.finish(current, nil); err != nil {
		t.Fatal(err)
	}
	d = reliabilityDelivery(t, s, r.Event.ID, spec.ID)
	if d.Status != StatusSucceeded || d.Attempts != 3 || d.ErrorCount != 2 {
		t.Fatalf("finish current attempt: %+v", d)
	}
	if err := manual.finish(interrupted, errors.New("late after success")); err != nil {
		t.Fatal(err)
	}
	if after := reliabilityDelivery(t, s, r.Event.ID, spec.ID); !reflect.DeepEqual(after, d) {
		t.Fatalf("stale finish changed terminal record: %+v", after)
	}
}

func TestReliabilityReopenExhaustedProcessing(t *testing.T) {
	cfg := reliabilityConfig(t)
	cfg.MaxAttempts = 1
	s := reliabilityOpen(t, cfg)
	reliabilityPark(t, s, SubscriptionSpec{ID: "recover"})
	p := reliabilityProducer(t, s, "producer")
	bad := reliabilityPublish(t, p, reliabilityInput("interrupted"))
	good := reliabilityPublish(t, p, reliabilityInput("following"))
	reliabilityClaim(t, &Subscription{store: s, id: "recover", ctx: context.Background()})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = reliabilityOpen(t, cfg)
	d := reliabilityDelivery(t, s, bad.Event.ID, "recover")
	if d.Status != StatusFailed || d.Attempts != 1 || d.ErrorCount != 1 || d.CompletedAt.IsZero() {
		t.Fatalf("exhausted recovery: %+v", d)
	}
	seen := make(chan string, 4)
	reliabilitySubscribe(t, s, SubscriptionSpec{ID: "recover"}, func(_ context.Context, e Event) error { seen <- e.ID; return nil })
	if got := reliabilityReceive(t, seen); got != good.Event.ID {
		t.Fatalf("replayed exhausted event: %s", got)
	}
	reliabilityStatus(t, s, good.Event.ID, "recover", StatusSucceeded)
}

func TestReliabilityCapacityAtomicRejection(t *testing.T) {
	for _, mode := range []string{"backlog", "storage"} {
		t.Run(mode, func(t *testing.T) {
			cfg := reliabilityConfig(t)
			cfg.MaxPendingPerSubscription = 1
			s := reliabilityOpen(t, cfg)
			reliabilityPark(t, s, SubscriptionSpec{ID: "full"})
			p := reliabilityProducer(t, s, "producer")
			in := reliabilityInput("accepted")
			in.DeduplicationKey = "key"
			accepted := reliabilityPublish(t, p, in)
			// 新订阅有空闲容量；另一个订阅满时也不能写入部分广播。
			reliabilityPark(t, s, SubscriptionSpec{ID: "empty"})
			beforeEvents, err := s.List(context.Background(), EventQuery{})
			if err != nil {
				t.Fatal(err)
			}
			beforeDeliveries := reliabilityDeliveries(t, s, DeliveryQuery{})
			usage, err := s.StorageUsage(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "storage" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				cfg.MaxPendingPerSubscription = 100
				cfg.MaxStorageBytes = usage + 1
				s = reliabilityOpen(t, cfg)
				p = reliabilityProducer(t, s, "producer")
			}
			if _, err := p.Publish(context.Background(), reliabilityInput("rejected")); !errors.Is(err, ErrCapacity) {
				t.Fatalf("capacity: %v", err)
			}
			afterEvents, err := s.List(context.Background(), EventQuery{})
			if err != nil || !reflect.DeepEqual(beforeEvents, afterEvents) {
				t.Fatalf("partial event after rejection: %+v %v", afterEvents, err)
			}
			if after := reliabilityDeliveries(t, s, DeliveryQuery{}); !reflect.DeepEqual(beforeDeliveries, after) {
				t.Fatalf("partial broadcast: %+v", after)
			}
			if after, err := s.StorageUsage(context.Background()); err != nil || after != usage {
				t.Fatalf("usage changed: %d -> %d, %v", usage, after, err)
			}
			dup := reliabilityPublish(t, p, in)
			if !dup.Duplicate || dup.Event.ID != accepted.Event.ID || dup.MatchedSubscriptions != 1 {
				t.Fatalf("duplicate rejected at capacity: %+v", dup)
			}
		})
	}
}

func TestReliabilityCleanupRetention(t *testing.T) {
	s := reliabilityOpen(t, reliabilityConfig(t))
	p := reliabilityProducer(t, s, "producer")
	orphan := reliabilityPublish(t, p, reliabilityInput("orphan"))
	reliabilityPark(t, s, SubscriptionSpec{ID: "worker"})
	statuses := []Status{StatusPending, StatusProcessing, StatusRetry, StatusSucceeded, StatusFailed, StatusCanceled}
	ids := make(map[Status]string)
	for _, status := range statuses {
		ids[status] = reliabilityPublish(t, p, reliabilityInput(string(status))).Event.ID
	}
	// 只在本测试的真实 SQLite 库中调整时间，避免等待真实保留期。
	exec := func(query string, args ...any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), reliabilityWait)
		defer cancel()
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * s.cfg.Retention).UnixNano()
	exec("UPDATE events SET received_at=?", old)
	for _, status := range statuses {
		exec("UPDATE deliveries SET status=?,completed_at=? WHERE event_sequence=(SELECT sequence FROM events WHERE id=?)", status, time.Now().UnixNano(), ids[status])
	}
	if n, err := s.Cleanup(context.Background()); err != nil || n != 1 {
		t.Fatalf("only old orphan should be cleaned: %d %v", n, err)
	}
	if _, err := s.Get(context.Background(), orphan.Event.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan remains: %v", err)
	}
	for _, id := range ids {
		if _, err := s.Get(context.Background(), id); err != nil {
			t.Fatalf("premature cleanup: %v", err)
		}
	}
	exec("UPDATE deliveries SET completed_at=?", old)
	if n, err := s.Cleanup(context.Background()); err != nil || n != 3 {
		t.Fatalf("expired terminal cleanup: %d %v", n, err)
	}
	for _, status := range statuses {
		_, err := s.Get(context.Background(), ids[status])
		unfinished := status == StatusPending || status == StatusProcessing || status == StatusRetry
		if unfinished && err != nil {
			t.Fatalf("unfinished %s removed: %v", status, err)
		}
		if !unfinished && !errors.Is(err, ErrNotFound) {
			t.Fatalf("terminal %s retained: %v", status, err)
		}
	}
	if ds := reliabilityDeliveries(t, s, DeliveryQuery{}); len(ds) != 3 {
		t.Fatalf("delivery cascade: %+v", ds)
	}
	if n, err := s.Cleanup(context.Background()); err != nil || n != 0 {
		t.Fatalf("repeated cleanup: %d %v", n, err)
	}
}

func TestReliabilityTimeoutNonCooperativeNoOverlap(t *testing.T) {
	for _, mode := range []string{"retry", "rebind", "close"} {
		t.Run(mode, func(t *testing.T) {
			cfg := reliabilityConfig(t)
			cfg.HandlerTimeout = 100 * time.Millisecond
			s := reliabilityOpen(t, cfg)
			p := reliabilityProducer(t, s, "producer")
			spec := SubscriptionSpec{ID: "worker"}
			reliabilityPark(t, s, spec)
			first := reliabilityPublish(t, p, reliabilityInput("first"))
			next := reliabilityPublish(t, p, reliabilityInput("next"))
			started, deadline, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			gate := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(gate) })
			var active, calls, overlap atomic.Int32
			handler := func(ctx context.Context, e Event) error {
				if active.Add(1) != 1 {
					overlap.Add(1)
				}
				defer active.Add(-1)
				if calls.Add(1) == 1 {
					close(started)
					<-ctx.Done()
					close(deadline)
					<-gate // 故意不合作：超时后仍不返回。
					defer close(exited)
				}
				return nil
			}
			sub := reliabilitySubscribe(t, s, spec, handler)
			reliabilityReceive(t, started)
			reliabilityReceive(t, deadline)
			d := reliabilityStatus(t, s, first.Event.ID, spec.ID, StatusRetry)
			if d.Attempts != 1 || d.ErrorCount != 1 || !strings.Contains(d.LastError, "deadline exceeded") {
				t.Fatalf("timeout not recorded: %+v", d)
			}
			// 到达重试时间后仍应被旧回调阻挡，而不是仅在超时瞬间检查。
			reliabilityEventually(t, "retry becomes due", func() bool { return time.Now().After(d.NextAttemptAt.Add(30 * time.Millisecond)) })
			if calls.Load() != 1 || active.Load() != 1 || overlap.Load() != 0 {
				t.Fatal("overlapping callback while old call is alive")
			}
			if pending := reliabilityDelivery(t, s, next.Event.ID, spec.ID); pending.Status != StatusPending || pending.Attempts != 0 {
				t.Fatalf("next event overtook timeout: %+v", pending)
			}
			if mode == "rebind" {
				reliabilityDetach(t, sub)
				if extra, err := s.Subscribe(context.Background(), spec, handler); !errors.Is(err, ErrBound) {
					if extra != nil {
						extra.Close()
					}
					t.Fatalf("rebound with old callback alive: %v", err)
				}
			}
			if mode == "close" {
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				if err := reliabilityReceive(t, closed); err != nil {
					t.Fatal(err)
				}
				if active.Load() != 1 {
					t.Fatal("test callback was not still running at Close")
				}
				release.Do(func() { close(gate) })
				reliabilityReceive(t, exited)
				return
			}
			release.Do(func() { close(gate) })
			reliabilityReceive(t, exited)
			if mode == "rebind" {
				reliabilityEventually(t, "old binding released", func() bool {
					s.mu.Lock()
					defer s.mu.Unlock()
					return s.bindings[spec.ID] == nil
				})
				reliabilitySubscribe(t, s, spec, handler)
			}
			d = reliabilityStatus(t, s, first.Event.ID, spec.ID, StatusSucceeded)
			if d.Attempts != 2 || d.ErrorCount != 1 {
				t.Fatalf("late nil erased timeout or attempts: %+v", d)
			}
			reliabilityStatus(t, s, next.Event.ID, spec.ID, StatusSucceeded)
			if calls.Load() != 3 || overlap.Load() != 0 {
				t.Fatalf("calls=%d overlaps=%d", calls.Load(), overlap.Load())
			}
		})
	}
}

func TestReliabilityCloseIdempotentAndOwnership(t *testing.T) {
	cfg := reliabilityConfig(t)
	s := reliabilityOpen(t, cfg)
	p := reliabilityProducer(t, s, "producer")
	if second, err := Open(context.Background(), cfg); !errors.Is(err, ErrOwned) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second owner: %v", err)
	}
	reliabilityPublish(t, p, reliabilityInput("owner still works"))
	closed := make(chan error, 8)
	for i := 0; i < cap(closed); i++ {
		go func() { closed <- s.Close() }()
	}
	for i := 0; i < cap(closed); i++ {
		if err := reliabilityReceive(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	if _, err := p.Publish(context.Background(), reliabilityInput("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	if _, err := s.BindProducer("closed"); !errors.Is(err, ErrClosed) {
		t.Fatalf("bind after close: %v", err)
	}
	reopened := reliabilityOpen(t, cfg)
	reliabilityPublish(t, reliabilityProducer(t, reopened, "producer"), reliabilityInput("new owner works"))
}

func TestReliabilityTimelyResultSurvivesLockDelay(t *testing.T) {
	cfg := reliabilityConfig(t)
	cfg.HandlerTimeout = 200 * time.Millisecond
	s := reliabilityOpen(t, cfg)
	gate, started, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(gate) })
	reliabilitySubscribe(t, s, SubscriptionSpec{ID: "worker"}, func(context.Context, Event) error {
		close(started)
		<-gate
		close(returned)
		return nil
	})
	r := reliabilityPublish(t, reliabilityProducer(t, s, "producer"), reliabilityInput("on time"))
	reliabilityReceive(t, started)
	// 模拟内部清理锁竞争，锁持有时间跨越回调截止时间。
	s.mu.Lock()
	release.Do(func() { close(gate) })
	<-returned
	time.Sleep(300 * time.Millisecond)
	s.mu.Unlock()
	d := reliabilityStatus(t, s, r.Event.ID, "worker", StatusSucceeded)
	if d.Attempts != 1 || d.ErrorCount != 0 {
		t.Fatalf("timely result incorrectly retried: %+v", d)
	}
}

func TestReliabilityCancellationDuringRegistrySQLWait(t *testing.T) {
	s := reliabilityOpen(t, reliabilityConfig(t))
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waiting := make(chan error, 1)
	before := s.db.Stats().WaitCount
	go func() {
		_, err := s.Subscribe(context.Background(), SubscriptionSpec{ID: "waiting"}, func(context.Context, Event) error { return nil })
		waiting <- err
	}()
	reliabilityEventually(t, "registration waiting for database", func() bool { return s.db.Stats().WaitCount > before })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, op := range []func() error{
		func() error { _, err := s.Get(ctx, "missing"); return err },
		func() error {
			_, err := s.Subscribe(ctx, SubscriptionSpec{ID: "canceled"}, func(context.Context, Event) error { return nil })
			return err
		},
		func() error { return s.DisableSubscription(ctx, "waiting") },
	} {
		result := make(chan error, 1)
		go func() { result <- op() }()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled operation: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("canceled operation blocked behind registry SQL")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-waiting:
		if err == nil {
			t.Fatal("registration succeeded while closing")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not cancel registration SQL wait")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reliabilityReceive(t, closed); err != nil {
		t.Fatal(err)
	}
}
