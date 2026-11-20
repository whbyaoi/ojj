package brain

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
)

type modelFunc func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error)

func (f modelFunc) Generate(c context.Context, r minipi.Request, u func(minipi.Message)) (minipi.Response, error) {
	return f(c, r, u)
}

func answer(text string) minipi.Response {
	return minipi.Response{Message: minipi.Message{Role: minipi.RoleAssistant, Content: text}, StopReason: minipi.StopEnd}
}

func testConfig(t *testing.T, model minipi.Model) Config {
	t.Helper()
	return Config{Path: filepath.Join(t.TempDir(), "brain.db"), ID: "main", Agent: minipi.Config{Model: model}, PollInterval: time.Millisecond, OperationTimeout: time.Second}
}

func openRegion(t *testing.T, cfg Config) *Region {
	t.Helper()
	r, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func receive(t *testing.T, r *Region, id, text string) {
	t.Helper()
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: id, Text: text}); err != nil {
		t.Fatal(err)
	}
}

func messages(t *testing.T, r *Region, id string) []StoredMessage {
	t.Helper()
	out, err := r.ListMessages(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func startRun(t *testing.T, r *Region) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, done
}

func waitStatus(t *testing.T, r *Region, id string, want Status) Input {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		in, err := r.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if in.Status == want {
			return in
		}
		if in.Status != StatusPending && in.Status != StatusProcessing {
			t.Fatalf("%s: got %s, want %s: %s", id, in.Status, want, in.LastError)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", id, want)
	return Input{}
}

func awaitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish")
		return nil
	}
}

func TestHandleDurableIdempotentAndVerbatim(t *testing.T) {
	var calls atomic.Int32
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		calls.Add(1)
		return answer("ok"), nil
	}))
	r := openRegion(t, cfg)
	text := " \n原文\t\"引号\"\x00 " + strings.Repeat("长", 100)
	receive(t, r, "same", text)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Handle(context.Background(), messagebuilder.Message{ID: "same", Text: text}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 0 {
		t.Fatal("Handle called model")
	}
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: "same", Text: "other"}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	got := messages(t, r, "same")
	if len(got) != 1 || got[0].Message.Role != "user" || got[0].Message.Content[0].Text != text {
		t.Fatalf("not verbatim: %+v", got)
	}
	raw, err := json.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"Role"`) || !strings.Contains(string(raw), `"role":"user"`) {
		t.Fatalf("wrong JSON: %s", raw)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openRegion(t, cfg)
	receive(t, r, "same", text)
	in, err := r.Get(context.Background(), "same")
	if err != nil || in.Status != StatusPending || in.Text != text || in.Sequence != 1 {
		t.Fatalf("reopen: %+v %v", in, err)
	}
	if len(messages(t, r, "same")) != 1 {
		t.Fatal("duplicate user log")
	}
}

func TestCapacityAndPagination(t *testing.T) {
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return answer("ok"), nil
	}))
	cfg.MaxStoredBytes = 180
	r := openRegion(t, cfg)
	receive(t, r, "a", "first")
	receive(t, r, "b", "second")
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: "c", Text: strings.Repeat("x", 100)}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	receive(t, r, "a", "first")
	if _, err := r.Get(context.Background(), "c"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	one := messages(t, r, "a")
	page, err := r.ListMessages(context.Background(), "a", one[0].Sequence, 1)
	if err != nil || len(page) != 0 {
		t.Fatalf("page: %+v %v", page, err)
	}
	if _, err = r.ListMessages(context.Background(), "missing", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MaxStoredBytes = 1
	cfg.MaxInputBytes = 1
	r = openRegion(t, cfg)
	receive(t, r, "a", "first")
	if err = r.Handle(context.Background(), messagebuilder.Message{ID: "a", Text: "changed"}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}
