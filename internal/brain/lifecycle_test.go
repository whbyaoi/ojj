package brain

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
)

func signal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for callback")
	}
}

func TestRecoveryNeverReplaysUncertainEffects(t *testing.T) {
	var calls atomic.Int32
	cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		calls.Add(1)
		if len(req.Messages) != 1 || req.Messages[0].Content != "pending" {
			return minipi.Response{}, errors.New("recovered uncertain history")
		}
		return answer("done"), nil
	}))
	r := openRegion(t, cfg)
	receive(t, r, "uncertain", "effect")
	in, err := r.claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = r.appendEvent(context.Background(), in.ID, minipi.Event{Type: minipi.EventMessageEnd, Message: toolCalls("uncertain-call").Message, Turn: 1}); err != nil {
		t.Fatal(err)
	}
	receive(t, r, "pending", "pending")
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openRegion(t, cfg)
	interrupted := waitStatus(t, r, "uncertain", StatusInterrupted)
	if interrupted.StartedAt.IsZero() || interrupted.CompletedAt.IsZero() || interrupted.LastError == "" || interrupted.StopReason != minipi.StopCanceled {
		t.Fatalf("recovery metadata: %+v", interrupted)
	}
	if len(messages(t, r, "uncertain")) != 2 {
		t.Fatal("recovery removed evidence")
	}
	cancel, done := startRun(t, r)
	waitStatus(t, r, "pending", StatusSucceeded)
	cancel()
	awaitRun(t, done)
	if calls.Load() != 1 {
		t.Fatalf("replayed work: %d", calls.Load())
	}
}

func TestParentCancellationPersistsFinalEvidence(t *testing.T) {
	entered := make(chan struct{})
	cfg := testConfig(t, modelFunc(func(ctx context.Context, _ minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		close(entered)
		<-ctx.Done()
		return answer("observed final"), ctx.Err()
	}))
	r := openRegion(t, cfg)
	receive(t, r, "a", "question")
	cancel, done := startRun(t, r)
	signal(t, entered)
	cancel()
	if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	in := waitStatus(t, r, "a", StatusInterrupted)
	if in.CompletedAt.IsZero() || in.LastError == "" {
		t.Fatalf("missing finalization: %+v", in)
	}
	logs := messages(t, r, "a")
	if len(logs) != 2 || logs[1].Message.Content[0].Text != "observed final" {
		t.Fatalf("lost canceled evidence: %+v", logs)
	}
}

func TestInputContextCancellationAndClose(t *testing.T) {
	for _, stage := range []string{"restore", "model", "tool"} {
		for _, closing := range []bool{false, true} {
			name := stage + "/cancel"
			if closing {
				name = stage + "/close"
			}
			t.Run(name, func(t *testing.T) {
				entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				deadline := time.Now().Add(time.Minute)
				block := func(ctx context.Context) error {
					if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
						t.Error("lost parent deadline")
					}
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-release
					return ctx.Err()
				}
				var models, tools atomic.Int32
				cfg := testConfig(t, modelFunc(func(ctx context.Context, _ minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
					models.Add(1)
					if stage == "model" {
						return answer("canceled"), block(ctx)
					}
					return toolCalls("call"), nil
				}))
				cfg.InputContext = func(ctx context.Context, _ string) (context.Context, error) {
					if stage == "restore" {
						return nil, block(ctx)
					}
					return context.Background(), nil
				}
				cfg.Agent.Tools = []minipi.Tool{fakeTool(func(ctx context.Context, _ minipi.ToolCall, _ func(minipi.ToolResult)) (minipi.ToolResult, error) {
					tools.Add(1)
					return minipi.ToolResult{}, block(ctx)
				})}
				r := openRegion(t, cfg)
				receive(t, r, "external", "question")
				ctx, cancel := context.WithDeadline(context.Background(), deadline)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- r.Run(ctx) }()
				signal(t, entered)
				closed := make(chan error, 1)
				if closing {
					go func() { closed <- r.Close() }()
				} else {
					cancel()
				}
				signal(t, canceled)
				select {
				case err := <-done:
					t.Fatalf("Run abandoned callback: %v", err)
				case err := <-closed:
					t.Fatalf("Close abandoned callback: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				once.Do(func() { close(release) })
				if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if closing {
					if err := awaitRun(t, closed); err != nil {
						t.Fatal(err)
					}
					r = openRegion(t, cfg)
				}
				waitStatus(t, r, "external", StatusInterrupted)
				if stage == "restore" && (models.Load() != 0 || tools.Load() != 0) {
					t.Fatal("canceled recovery allowed agent side effects")
				}
			})
		}
	}
}

func TestCloseWaitsForCallbacksAndConcurrentCalls(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	cfg := testConfig(t, modelFunc(func(ctx context.Context, _ minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return answer("last evidence"), ctx.Err()
	}))
	r := openRegion(t, cfg)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	receive(t, r, "a", "question")
	_, done := startRun(t, r)
	signal(t, entered)
	if err := r.Run(context.Background()); !errors.Is(err, ErrRunning) {
		t.Fatalf("second Run: %v", err)
	}
	closed := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { closed <- r.Close() }()
	}
	signal(t, canceled)
	select {
	case err := <-closed:
		t.Fatalf("Close returned before callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if other, err := Open(context.Background(), cfg); !errors.Is(err, ErrOwned) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("owner lock released early: %v", err)
	}
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: "b", Text: "closed"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := r.Get(context.Background(), "a"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := r.ListMessages(context.Background(), "a", 0, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := awaitRun(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	reopened := openRegion(t, cfg)
	waitStatus(t, reopened, "a", StatusInterrupted)
	if len(messages(t, reopened, "a")) != 2 {
		t.Fatal("Close did not flush final event")
	}
}

func TestOpenIdentityLockAndDurabilitySettings(t *testing.T) {
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return answer("ok"), nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	r, err := Open(ctx, cfg)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	receive(t, r, "a", "open context is not lifetime")
	for _, path := range []string{cfg.Path, cfg.Path + ".lock", cfg.Path + "-wal", cfg.Path + "-shm"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("mode %s: %o", path, info.Mode().Perm())
		}
	}
	var mode string
	var full, foreign int
	if err = r.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("WAL: %q %v", mode, err)
	}
	if err = r.db.QueryRow("PRAGMA synchronous").Scan(&full); err != nil || full != 2 {
		t.Fatalf("FULL: %d %v", full, err)
	}
	if err = r.db.QueryRow("PRAGMA foreign_keys").Scan(&foreign); err != nil || foreign != 1 {
		t.Fatalf("foreign keys: %d %v", foreign, err)
	}
	alias := filepath.Join(filepath.Dir(cfg.Path), "alias.db")
	if err = os.Symlink(cfg.Path, alias); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.Path, alias} {
		otherCfg := cfg
		otherCfg.Path = path
		other, err := Open(context.Background(), otherCfg)
		if other != nil {
			other.Close()
		}
		if !errors.Is(err, ErrOwned) {
			t.Fatalf("lock: %v", err)
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := cfg
	wrong.ID = "another"
	if other, err := Open(context.Background(), wrong); !errors.Is(err, ErrIdentity) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("wrong region accepted: %v", err)
	}
	reopened := openRegion(t, cfg)
	receive(t, reopened, "a", "open context is not lifetime")
	otherCfg := cfg
	otherCfg.Path = filepath.Join(filepath.Dir(cfg.Path), "foreign.db")
	db, err := sql.Open("sqlite", otherCfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE foreign_data(id INTEGER)")
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(context.Background(), otherCfg); !errors.Is(err, ErrIdentity) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("wrong schema accepted: %v", err)
	}
}

func TestBoundedStorageWaitAndCloseCancelsOperations(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "close"}[closing], func(t *testing.T) {
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				return answer("ok"), nil
			}))
			cfg.OperationTimeout = 50 * time.Millisecond
			r := openRegion(t, cfg)
			conn, err := r.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			done := make(chan error, 1)
			go func() { done <- r.Handle(context.Background(), messagebuilder.Message{ID: "a", Text: "blocked"}) }()
			if !closing {
				if err := awaitRun(t, done); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				return
			}
			deadline := time.Now().Add(time.Second)
			for r.db.Stats().WaitCount == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if r.db.Stats().WaitCount == 0 {
				t.Fatal("operation did not wait")
			}
			closed := make(chan error, 1)
			go func() { closed <- r.Close() }()
			if err := awaitRun(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			conn.Close()
			if err := awaitRun(t, closed); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeCapacityStopsQueue(t *testing.T) {
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return toolCalls("a"), nil
	}))
	cfg.MaxStoredBytes = 200
	var tools atomic.Int32
	cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
		tools.Add(1)
		return minipi.ToolResult{}, nil
	})}
	r := openRegion(t, cfg)
	receive(t, r, "a", "question")
	_, done := startRun(t, r)
	if err := awaitRun(t, done); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	waitStatus(t, r, "a", StatusFailed)
	if tools.Load() != 0 || len(messages(t, r, "a")) != 1 {
		t.Fatal("capacity barrier failed")
	}
	receive(t, r, "a", "question")
}

func TestNextModelContextCapacityAndFinalizationFailure(t *testing.T) {
	t.Run("next-model", func(t *testing.T) {
		var calls atomic.Int32
		cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
			calls.Add(1)
			return toolCalls("a"), nil
		}))
		cfg.MaxContextBytes = 600
		cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
			return minipi.ToolResult{Content: strings.Repeat("x", 1000)}, nil
		})}
		r := openRegion(t, cfg)
		receive(t, r, "a", "question")
		cancel, done := startRun(t, r)
		in := waitStatus(t, r, "a", StatusFailed)
		cancel()
		awaitRun(t, done)
		if calls.Load() != 1 || !strings.Contains(in.LastError, ErrContextCapacity.Error()) {
			t.Fatalf("next model capacity: %+v", in)
		}
		if logs := messages(t, r, "a"); len(logs) < 3 || len(logs[2].Message.Content[0].Text) != 1000 {
			t.Fatal("tool evidence truncated")
		}
	})
	t.Run("finalization", func(t *testing.T) {
		cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
			return answer("final"), nil
		}))
		r := openRegion(t, cfg)
		receive(t, r, "a", "question")
		receive(t, r, "b", "future")
		_, err := r.db.Exec(`CREATE TRIGGER reject_finish BEFORE UPDATE ON inputs WHEN NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'finish failed'); END`)
		if err != nil {
			t.Fatal(err)
		}
		_, done := startRun(t, r)
		if err := awaitRun(t, done); err == nil || !strings.Contains(err.Error(), "finish failed") {
			t.Fatal(err)
		}
		waitStatus(t, r, "a", StatusProcessing)
		waitStatus(t, r, "b", StatusPending)
		if len(messages(t, r, "a")) != 2 {
			t.Fatal("lost evidence after finalization failure")
		}
	})
}
