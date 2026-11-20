package startup

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/event"
)

type publishFunc func(context.Context, event.Input) (event.PublishResult, error)

func (f publishFunc) Publish(ctx context.Context, in event.Input) (event.PublishResult, error) {
	return f(ctx, in)
}

func TestRunOncePerStart(t *testing.T) {
	var calls atomic.Int32
	for i := 1; i <= 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		published := make(chan event.Input, 1)
		done := make(chan error, 1)
		before := time.Now()
		go func() {
			done <- Run(ctx, publishFunc(func(call context.Context, in event.Input) (event.PublishResult, error) {
				calls.Add(1)
				if _, ok := call.Deadline(); !ok {
					return event.PublishResult{}, errors.New("missing publish deadline")
				}
				published <- in
				return event.PublishResult{}, nil
			}))
		}()
		select {
		case in := <-published:
			var content struct {
				Version    int    `json:"version"`
				Reason     string `json:"reason"`
				SkipMemory bool   `json:"skip_memory"`
			}
			if in.EventType != EventType || in.SchemaVersion != 1 || in.Source != nil || in.DeduplicationKey != "" || in.OccurredAt.Before(before) || in.OccurredAt.After(time.Now()) {
				t.Error("invalid startup event")
			}
			if err := json.Unmarshal([]byte(in.Content), &content); err != nil || content.Version != 1 || content.Reason != "service_started" || !content.SkipMemory {
				t.Error("invalid startup content")
			}
		case <-ctx.Done():
			cancel()
			t.Fatal("startup event not published")
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("successful publish stopped service: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("startup runner did not stop")
		}
		if calls.Load() != int32(i) {
			t.Fatal("startup event was repeated or restart was suppressed")
		}
	}
}

func TestRunFailureAndCancellation(t *testing.T) {
	failure := errors.New("publish failed")
	calls := 0
	publisher := publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
		calls++
		return event.PublishResult{}, failure
	})
	if err := Run(context.Background(), publisher); !errors.Is(err, failure) || calls != 1 {
		t.Fatal("publish failure was swallowed or retried", err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, publisher); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("published despite prior cancellation", err, calls)
	}
	if err := Run(context.Background(), nil); err == nil {
		t.Fatal("accepted nil publisher")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if err := Run(ctx, publishFunc(func(call context.Context, _ event.Input) (event.PublishResult, error) {
		cancel()
		<-call.Done()
		return event.PublishResult{}, call.Err()
	})); !errors.Is(err, context.Canceled) {
		t.Fatal("in-flight cancellation was lost", err)
	}
}
