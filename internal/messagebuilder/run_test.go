package messagebuilder

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitRun(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit")
		return nil
	}
}

func startRun(t *testing.T, b *Builder, handler Handler) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- b.Run(ctx, handler) }()
	t.Cleanup(cancel)
	return cancel, result
}

func TestDeliveryPersistsBeforeCallbackAndRetriesInOrder(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 1, PollInterval: time.Millisecond, RetryInitial: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	acceptAt(t, b, testEvent("first"), time.Now())
	acceptAt(t, b, testEvent("second"), time.Now())
	messages := listMessages(t, b)
	var mu sync.Mutex
	var calls []Message
	cancel, result := startRun(t, b, func(ctx context.Context, m Message) error {
		stored, err := b.Get(ctx, m.ID)
		if err != nil {
			t.Error(err)
			return err
		}
		if stored.Status != StatusProcessing || stored.Attempts != m.Attempts || stored.Text != m.Text {
			t.Error("callback before persistence")
		}
		mu.Lock()
		calls = append(calls, m)
		mu.Unlock()
		if m.Sequence == messages[0].Sequence && m.Attempts == 1 {
			return errors.New("sensitive body")
		}
		return nil
	})
	waitFor(t, func() bool {
		m, err := b.Get(context.Background(), messages[1].ID)
		return err == nil && m.Status == StatusSucceeded
	})
	cancel()
	if err := waitRun(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 || calls[0].ID != calls[1].ID || calls[2].ID != messages[1].ID || calls[1].Attempts != 2 {
		t.Fatalf("delivery order: %+v", calls)
	}
	if calls[1].StartedAt.Sub(calls[0].StartedAt) < b.cfg.RetryInitial {
		t.Fatal("retry skipped backoff")
	}
	m, err := b.Get(context.Background(), messages[0].ID)
	if err != nil || m.LastError != "" || m.CompletedAt.IsZero() {
		t.Fatal("success state", err)
	}
	if b.retryDelay(1) != 10*time.Millisecond || b.retryDelay(2) != 20*time.Millisecond || b.retryDelay(1000) != 20*time.Millisecond {
		t.Fatal("backoff cap")
	}
}

func TestDeliveryFailureClassification(t *testing.T) {
	for _, kind := range []string{"handler_failed", "handler_panic", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			b := testBuilder(t, Config{MaxWindowEvents: 1, PollInterval: time.Millisecond, DeliveryTimeout: 10 * time.Millisecond, RetryInitial: time.Hour, RetryMax: time.Hour})
			acceptAt(t, b, testEvent("one"), time.Now())
			id := listMessages(t, b)[0].ID
			cancel, result := startRun(t, b, func(ctx context.Context, m Message) error {
				switch kind {
				case "handler_panic":
					panic("sensitive panic")
				case "timeout":
					<-ctx.Done()
					return nil
				default:
					return errors.New("sensitive failure")
				}
			})
			waitFor(t, func() bool { m, err := b.Get(context.Background(), id); return err == nil && m.Status == StatusRetry })
			m, err := b.Get(context.Background(), id)
			if err != nil || m.LastError != kind || m.Attempts != 1 || m.NextAttemptAt.IsZero() || !m.CompletedAt.IsZero() {
				t.Fatalf("failure state: %+v %v", m, err)
			}
			cancel()
			if err := waitRun(t, result); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestSlowCallbackDoesNotBlockSealingAndRunWaits(t *testing.T) {
	b := testBuilder(t, Config{QuietPeriod: 10 * time.Millisecond, PollInterval: time.Millisecond, DeliveryTimeout: time.Hour})
	acceptAt(t, b, testEvent("first"), time.Now().Add(-time.Second))
	settleAt(t, b, time.Now())
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	cancel, result := startRun(t, b, func(context.Context, Message) error { close(entered); <-release; return nil })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback not started")
	}
	if err := b.Handle(context.Background(), testEvent("second")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(listMessages(t, b)) == 2 })
	if err := b.Run(context.Background(), nil); !errors.Is(err, ErrRunning) {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		t.Fatalf("Run abandoned callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	if err := waitRun(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cancel, result = startRun(t, b, nil)
	waitFor(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.running })
	cancel()
	if err := waitRun(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCloseWaitsAndPreservesJoinedCallbackFailure(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 1, PollInterval: time.Millisecond})
	acceptAt(t, b, testEvent("first"), time.Now())
	id := listMessages(t, b)[0].ID
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	failure := errors.New("real callback failure")
	_, result := startRun(t, b, func(ctx context.Context, _ Message) error {
		close(entered)
		<-ctx.Done()
		<-release
		return errors.Join(ctx.Err(), failure)
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback not started")
	}
	closed := make(chan error, 2)
	go func() { closed <- b.Close() }()
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close abandoned callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	other, err := Open(context.Background(), b.cfg)
	if other != nil {
		other.Close()
		t.Fatal("ownership released early")
	}
	if !errors.Is(err, ErrOwned) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	if err := waitRun(t, result); !errors.Is(err, failure) || !errors.Is(err, context.Canceled) {
		t.Fatal("lost joined error", err)
	}
	for range 2 {
		if err := waitRun(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	b = testBuilder(t, b.cfg)
	m, err := b.Get(context.Background(), id)
	if err != nil || m.Status != StatusRetry || m.LastError != "handler_failed" {
		t.Fatal("interrupted callback not persisted", err)
	}
}

func TestRecoveryPreservesIDsAttemptsAndRetrySchedule(t *testing.T) {
	cfg := Config{Path: filepath.Join(t.TempDir(), "messages.db"), MaxWindowEvents: 1, RetryInitial: time.Hour, RetryMax: time.Hour}
	b := testBuilder(t, cfg)
	acceptAt(t, b, testEvent("first"), time.Now())
	acceptAt(t, b, testEvent("second"), time.Now())
	initial := listMessages(t, b)
	m, found, err := b.claim(context.Background())
	if err != nil || !found {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = testBuilder(t, cfg)
	recovered, err := b.Get(context.Background(), m.ID)
	if err != nil || recovered.Status != StatusRetry || recovered.Attempts != 1 || recovered.LastError != "interrupted" {
		t.Fatalf("recovery: %+v %v", recovered, err)
	}
	m, found, err = b.claim(context.Background())
	if err != nil || !found || m.ID != initial[0].ID || m.Attempts != 2 {
		t.Fatal("unstable recovery", err)
	}
	if err = b.finish(m, errors.New("private")); err != nil {
		t.Fatal(err)
	}
	before, err := b.Get(context.Background(), m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	b = testBuilder(t, cfg)
	after, err := b.Get(context.Background(), m.ID)
	if err != nil || !after.NextAttemptAt.Equal(before.NextAttemptAt) || after.Attempts != 2 {
		t.Fatal("retry schedule changed", err)
	}
	if _, found, err = b.claim(context.Background()); err != nil || found {
		t.Fatal("retry order bypassed", err)
	}
	pending, err := b.Get(context.Background(), initial[1].ID)
	if err != nil || pending.Status != StatusPending || pending.Attempts != 0 {
		t.Fatal("pending changed", err)
	}
}

func TestNilHandlerOnlyBuilds(t *testing.T) {
	b := testBuilder(t, Config{QuietPeriod: time.Millisecond, PollInterval: time.Millisecond})
	acceptAt(t, b, testEvent("one"), time.Now())
	cancel, result := startRun(t, b, nil)
	waitFor(t, func() bool { return len(listMessages(t, b)) == 1 })
	cancel()
	if err := waitRun(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m := listMessages(t, b)[0]
	if m.Status != StatusPending || m.Attempts != 0 {
		t.Fatal("nil handler delivered")
	}
}
