package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"ojj/internal/event"
)

type publishFunc func(context.Context, event.Input) (event.PublishResult, error)

func (f publishFunc) Publish(ctx context.Context, in event.Input) (event.PublishResult, error) {
	return f(ctx, in)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

func TestTrackingAndStoredHeartbeat(t *testing.T) {
	ctx := context.Background()
	store, err := event.Open(ctx, event.Config{Path: filepath.Join(t.TempDir(), "events.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hp, err := store.BindProducer("system.heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	external, err := store.BindProducer("qq.websocket")
	if err != nil {
		t.Fatal(err)
	}
	var stored event.Event
	g, err := New(time.Minute, publishFunc(func(ctx context.Context, in event.Input) (event.PublishResult, error) {
		result, err := hp.Publish(ctx, in)
		stored = result.Event
		return result, err
	}))
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := g.Track(external)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	g.idleSince, g.next = old, old
	input := event.Input{EventType: "im.message", OccurredAt: old, Content: "hello", DeduplicationKey: "one", Source: &event.Source{Platform: "qq", AccountID: "account", ConversationType: "private", ConversationID: "user", SenderID: "user", MessageID: "one"}}
	if _, err := tracked.Publish(ctx, input); err != nil {
		t.Fatal(err)
	}
	since, next := g.idleSince, g.next
	if !since.After(old) || next.Sub(since) != time.Minute {
		t.Fatal("new message did not reset idle")
	}
	result, err := tracked.Publish(ctx, input)
	if err != nil || !result.Duplicate {
		t.Fatalf("duplicate: %+v %v", result, err)
	}
	if g.idleSince != since || g.next != next {
		t.Fatal("duplicate reset idle")
	}
	input.Content = "conflict"
	if _, err := tracked.Publish(ctx, input); !errors.Is(err, event.ErrConflict) {
		t.Fatal(err)
	}
	if g.next != next {
		t.Fatal("failure reset idle")
	}
	if _, err := tracked.Publish(ctx, event.Input{EventType: "other", OccurredAt: old, Content: "other"}); err != nil {
		t.Fatal(err)
	}
	if g.next != next {
		t.Fatal("non-message reset idle")
	}
	if delay, err := g.step(ctx); err != nil || delay <= 0 || stored.ID != "" {
		t.Fatal("published before deadline", err)
	}
	g.next = old
	if _, err := g.step(ctx); err != nil {
		t.Fatal(err)
	}
	var content Content
	if err := json.Unmarshal([]byte(stored.Content), &content); err != nil {
		t.Fatal(err)
	}
	if stored.ID == "" || stored.ProducerID != "system.heartbeat" || stored.EventType != EventType || stored.Source != nil || stored.SchemaVersion != 1 {
		t.Fatalf("invalid event: %+v", stored)
	}
	if !content.SkipMemory || content.Reason != "idle" || content.Version != 1 || content.Interval != "1m0s" || !content.IdleSince.Equal(since) {
		t.Fatalf("invalid content: %+v", content)
	}
	if g.idleSince != since || !g.next.After(time.Now()) {
		t.Fatal("heartbeat reset idle or failed to reschedule")
	}
}

func TestRunRepeatedCancellationAndRestart(t *testing.T) {
	events := make(chan time.Time, 10)
	g, err := New(30*time.Millisecond, publishFunc(func(ctx context.Context, in event.Input) (event.PublishResult, error) {
		events <- time.Now()
		return event.PublishResult{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- g.Run(ctx) }()
		first := receive(t, events)
		if first.Sub(start) < g.interval {
			t.Fatal("startup or restart emitted early")
		}
		if err := g.Run(ctx); !errors.Is(err, ErrRunning) {
			t.Fatal(err)
		}
		second := receive(t, events)
		if second.Sub(first) < g.interval {
			t.Fatal("idle heartbeats emitted too quickly")
		}
		cancel()
		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}

func TestPublishFailureAndInFlightCancellation(t *testing.T) {
	failure := errors.New("storage failed")
	g, _ := New(time.Millisecond, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
		return event.PublishResult{}, failure
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := g.Run(ctx); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	g, _ = New(time.Millisecond, publishFunc(func(ctx context.Context, in event.Input) (event.PublishResult, error) {
		close(entered)
		<-ctx.Done()
		return event.PublishResult{}, ctx.Err()
	}))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	receive(t, entered)
	stop()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExternalCommitSerializesDueCheck(t *testing.T) {
	beats := 0
	g, _ := New(time.Minute, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
		beats++
		return event.PublishResult{}, nil
	}))
	g.idleSince, g.next = time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	tracked, _ := g.Track(publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
		close(entered)
		<-release
		return event.PublishResult{}, nil
	}))
	published, stepped := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := tracked.Publish(context.Background(), event.Input{EventType: "im.message"})
		published <- err
	}()
	receive(t, entered)
	go func() { _, err := g.step(context.Background()); stepped <- err }()
	close(release)
	if err := receive(t, published); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, stepped); err != nil {
		t.Fatal(err)
	}
	if beats != 0 {
		t.Fatal("heartbeat overtook committed external message")
	}
}

func TestRunningResetAndOfflineRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var beats atomic.Int32
		g, _ := New(time.Minute, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
			beats.Add(1)
			return event.PublishResult{}, nil
		}))
		tracked, _ := g.Track(publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
			return event.PublishResult{}, nil
		}))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- g.Run(ctx) }()
		synctest.Wait()
		time.Sleep(40 * time.Second)
		if _, err := tracked.Publish(ctx, event.Input{EventType: "im.message"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(40 * time.Second)
		synctest.Wait()
		if beats.Load() != 0 {
			t.Fatal("old deadline was not reset")
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if beats.Load() != 1 {
			t.Fatal("missing reset heartbeat", beats.Load())
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Minute)
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		go func() { done <- g.Run(ctx) }()
		synctest.Wait()
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if beats.Load() != 1 {
			t.Fatal("replayed offline heartbeats")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if beats.Load() != 2 {
			t.Fatal("restart did not wait full interval")
		}
		cancel()
		<-done
	})
}

func TestLockWaitCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g, _ := New(time.Minute, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }))
		release := make(chan struct{})
		tracked, _ := g.Track(publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
			<-release
			return event.PublishResult{}, nil
		}))
		owner := make(chan struct{})
		go func() { defer close(owner); tracked.Publish(context.Background(), event.Input{}) }()
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		results := make(chan error, 3)
		go func() { _, err := tracked.Publish(ctx, event.Input{}); results <- err }()
		go func() { results <- g.Run(ctx) }()
		go func() { _, err := g.step(ctx); results <- err }()
		synctest.Wait()
		cancel()
		for range 3 {
			if err := <-results; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		close(release)
		<-owner
	})
}

func TestConfiguration(t *testing.T) {
	p := publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil })
	if _, err := New(-time.Second, p); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := New(time.Second, nil); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	g, err := New(0, p)
	if err != nil || g.interval != 5*time.Minute {
		t.Fatal("invalid default", err)
	}
	if _, err := g.Track(nil); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}
