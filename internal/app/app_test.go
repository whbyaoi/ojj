package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/messagebuilder"
)

func TestMessageBuilderConfig(t *testing.T) {
	for _, name := range []string{"OJJ_MESSAGE_DB", "OJJ_MESSAGE_QUIET_PERIOD", "OJJ_MESSAGE_MAX_WINDOW"} {
		t.Setenv(name, "")
	}
	cfg, err := messageBuilderConfig("custom.db")
	if err != nil || cfg.Path != "custom.db.messages.db" || cfg.QuietPeriod != 8*time.Second || cfg.MaxWindow != 30*time.Second {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	t.Setenv("OJJ_MESSAGE_DB", "messages.db")
	t.Setenv("OJJ_MESSAGE_QUIET_PERIOD", "300ms")
	t.Setenv("OJJ_MESSAGE_MAX_WINDOW", "3s")
	cfg, err = messageBuilderConfig("custom.db")
	if err != nil || cfg.Path != "messages.db" || cfg.QuietPeriod != 300*time.Millisecond || cfg.MaxWindow != 3*time.Second {
		t.Fatalf("overrides: %+v %v", cfg, err)
	}
	for _, name := range []string{"OJJ_MESSAGE_QUIET_PERIOD", "OJJ_MESSAGE_MAX_WINDOW"} {
		for _, value := range []string{"0s", "-1s", "bad"} {
			t.Run(name+value, func(t *testing.T) {
				t.Setenv(name, value)
				t.Setenv("QQ_APP_ID", "test")
				t.Setenv("QQ_CLIENT_SECRET", "test")
				t.Setenv("OJJ_HEARTBEAT_INTERVAL", "5m")
				path := filepath.Join(t.TempDir(), "events.db")
				t.Setenv("OJJ_EVENT_DB", path)
				messagePath := path + ".messages.db"
				t.Setenv("OJJ_MESSAGE_DB", messagePath)
				if err := Run(context.Background()); err == nil {
					t.Fatal("accepted invalid message config")
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("opened event database before validation", err)
				}
				if _, err := os.Stat(messagePath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("opened message database before validation", err)
				}
			})
		}
	}
}

func TestRunWithMessagesPersistenceAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	store, err := event.Open(ctx, event.Config{Path: filepath.Join(dir, "events.db"), PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := messagebuilder.Config{Path: filepath.Join(dir, "messages.db"), QuietPeriod: 5 * time.Millisecond, PollInterval: time.Millisecond}
	builder, err := messagebuilder.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { builder.Close() }()
	producer, err := store.BindProducer("test")
	if err != nil {
		t.Fatal(err)
	}
	// 首次订阅不能回放已有历史。
	if _, err = producer.Publish(ctx, event.Input{EventType: "test.message", OccurredAt: time.Now(), Content: "old"}); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("test completed")
	var first messagebuilder.Message
	err = runWithMessages(ctx, store, builder, nil, func(child context.Context) error {
		published, err := producer.Publish(child, event.Input{EventType: "test.message", OccurredAt: time.Now(), Content: "new"})
		if err != nil {
			return err
		}
		for child.Err() == nil {
			messages, err := builder.List(child, messagebuilder.MessageQuery{})
			if err != nil {
				return err
			}
			if len(messages) != 0 {
				first = messages[0]
				if len(messages) != 1 || len(first.EventIDs) != 1 || first.EventIDs[0] != published.Event.ID || first.Status != messagebuilder.StatusPending || first.Attempts != 0 {
					return errors.New("incorrect first message or delivery status")
				}
				return stop
			}
			time.Sleep(time.Millisecond)
		}
		return child.Err()
	})
	if !errors.Is(err, stop) {
		t.Fatal("first run", err)
	}
	// 解绑期间订阅仍积压；重开业务库后恢复稳定 ID 和待处理事件。
	if err = builder.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = producer.Publish(ctx, event.Input{EventType: "test.message", OccurredAt: time.Now(), Content: "offline"}); err != nil {
		t.Fatal(err)
	}
	builder, err = messagebuilder.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = runWithMessages(ctx, store, builder, nil, func(child context.Context) error {
		for child.Err() == nil {
			messages, err := builder.List(child, messagebuilder.MessageQuery{})
			if err != nil {
				return err
			}
			if len(messages) == 2 {
				if messages[0].ID != first.ID || messages[1].Status != messagebuilder.StatusPending {
					return errors.New("recovery changed ID or acknowledged undelivered message")
				}
				return stop
			}
			time.Sleep(time.Millisecond)
		}
		return child.Err()
	})
	if !errors.Is(err, stop) {
		t.Fatal("recovery run", err)
	}
}

func TestRunWithMessagesDeliveryAndAdmissionFailure(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "delivery"
		if reject {
			name = "admission failure"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			store, err := event.Open(ctx, event.Config{Path: filepath.Join(dir, "events.db"), PollInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			cfg := messagebuilder.Config{Path: filepath.Join(dir, "messages.db"), QuietPeriod: time.Millisecond, PollInterval: time.Millisecond}
			if reject {
				cfg.MaxEventBytes = 1
			}
			builder, err := messagebuilder.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
			producer, err := store.BindProducer("test")
			if err != nil {
				t.Fatal(err)
			}
			checked := make(chan error, 1)
			downstream := func(call context.Context, msg messagebuilder.Message) error {
				persisted, err := builder.Get(call, msg.ID)
				if err == nil && (persisted.Status != messagebuilder.StatusProcessing || persisted.Text != msg.Text || len(persisted.EventIDs) != 1 || persisted.Attempts != 1) {
					err = errors.New("callback ran before complete message was persisted")
				}
				select {
				case checked <- err:
				default:
				}
				return err
			}
			stopped := make(chan struct{})
			stop := errors.New("delivery verified")
			err = runWithMessages(ctx, store, builder, downstream, func(child context.Context) error {
				defer close(stopped)
				if _, err := producer.Publish(child, event.Input{EventType: "test.message", OccurredAt: time.Now(), Content: "payload"}); err != nil {
					return err
				}
				if reject {
					<-child.Done()
					return child.Err()
				}
				select {
				case err := <-checked:
					if err != nil {
						return err
					}
				case <-child.Done():
					return child.Err()
				}
				for child.Err() == nil {
					messages, err := builder.List(child, messagebuilder.MessageQuery{Status: messagebuilder.StatusSucceeded})
					if err != nil {
						return err
					}
					if len(messages) == 1 {
						return stop
					}
					time.Sleep(time.Millisecond)
				}
				return child.Err()
			})
			want := stop
			if reject {
				want = messagebuilder.ErrCapacity
			}
			if !errors.Is(err, want) {
				t.Fatalf("want %v, got %v", want, err)
			}
			select {
			case <-stopped:
			default:
				t.Fatal("returned before producer stopped")
			}
			if reject {
				select {
				case <-checked:
					t.Fatal("delivered rejected event")
				default:
				}
			}
		})
	}
}

func TestHeartbeatInterval(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{"", 5 * time.Minute, false}, {"30s", 30 * time.Second, false}, {"1h", time.Hour, false},
		{"0", 0, true}, {"-1m", 0, true}, {"5", 0, true}, {"bad", 0, true},
	} {
		got, err := heartbeatInterval(tc.value)
		if (err != nil) != tc.invalid || got != tc.want {
			t.Fatalf("%q: %v %v", tc.value, got, err)
		}
	}
}

func TestRunInvalidIntervalDoesNotOpenStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	t.Setenv("QQ_APP_ID", "test")
	t.Setenv("QQ_CLIENT_SECRET", "test")
	t.Setenv("OJJ_EVENT_DB", path)
	t.Setenv("OJJ_HEARTBEAT_INTERVAL", "0s")
	if err := Run(context.Background()); err == nil {
		t.Fatal("accepted invalid interval")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("opened database before config validation", err)
	}
}

func TestRunTogetherStopsAndWaits(t *testing.T) {
	failure := errors.New("producer failed")
	for _, failFirst := range []bool{true, false} {
		exited := make(chan struct{})
		worker := func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(time.Millisecond)
			close(exited)
			return ctx.Err()
		}
		failed := func(context.Context) error { return failure }
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var err error
		if failFirst {
			err = runTogether(ctx, failed, worker)
		} else {
			err = runTogether(ctx, worker, failed)
		}
		cancel()
		if !errors.Is(err, failure) {
			t.Fatal("lost failure", err)
		}
		select {
		case <-exited:
		default:
			t.Fatal("returned before worker exited")
		}
	}
}

func TestRunTogetherJoinedCancellationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.New("rollback failed")
	err := runTogether(ctx, func(context.Context) error { return errors.Join(context.Canceled, failure) })
	if !errors.Is(err, failure) {
		t.Fatal("joined storage failure was hidden", err)
	}
}

func TestRunTogetherParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	worker := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	if err := runTogether(ctx, worker, worker); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("storage failure during shutdown")
	if err := runTogether(ctx, worker, func(context.Context) error { return failure }); !errors.Is(err, failure) {
		t.Fatal("lost shutdown failure", err)
	}
}
