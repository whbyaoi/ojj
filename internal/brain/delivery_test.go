package brain

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
)

func TestModelSkippedHistoryAndQueue(t *testing.T) {
	ctx := WithModelSkipped(context.Background(), true)
	models, tools := 0, 0
	cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		models++
		if len(req.Messages) != 2 || req.Messages[0].Content != "skipped question" || req.Messages[1].Content != "next question" {
			t.Error("skipped user input missing from subsequent history")
		}
		return answer("answer"), nil
	}))
	cfg.InputContext = func(ctx context.Context, id string) (context.Context, error) {
		return WithModelSkipped(replydelivery.WithTracking(ctx), id == "old"), nil
	}
	cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
		tools++
		return minipi.ToolResult{}, nil
	})}
	r := openRegion(t, cfg)
	receive(t, r, "old", "skipped question")
	receive(t, r, "next", "next question")
	in, err := r.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// No history capacity is needed for a host-level skip.
	r.cfg.MaxContextBytes = 1
	if err := r.process(ctx, in); err != nil {
		t.Fatal(err)
	}
	stored := waitStatus(t, r, "old", StatusSucceeded)
	if models != 0 || tools != 0 || stored.Turns != 0 || stored.Usage != (Usage{}) || stored.StopReason != "" || stored.LastError != "" || stored.CompletedAt.IsZero() {
		t.Fatal("skip must be a successful zero-work input")
	}
	original := messages(t, r, "old")
	if len(original) != 1 || decodeMessage(original[0].Message).Role != minipi.RoleUser {
		t.Fatal("skip created assistant or tool evidence")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r = openRegion(t, cfg)
	var state replydelivery.State
	if err := r.db.QueryRow(`SELECT qq_delivery FROM inputs WHERE id='old'`).Scan(&state); err != nil || state != replydelivery.NotSent {
		t.Fatal("skip delivery state was not persisted", err)
	}
	in, err = r.claim(ctx)
	if err != nil || in.ID != "next" {
		t.Fatal("skip blocked the next input", err)
	}
	if err := r.process(ctx, in); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r, "next", StatusSucceeded)
	if models != 1 || tools != 0 || !reflect.DeepEqual(original, messages(t, r, "old")) {
		t.Fatal("explicit false did not mask parent skip or audit changed")
	}
}

func TestModelSkippedCancellationWins(t *testing.T) {
	for _, detached := range []bool{false, true} {
		name := "parent"
		if detached {
			name = "callback"
		}
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				t.Error("canceled skip invoked model")
				return answer("unexpected"), nil
			}))
			cfg.InputContext = func(ctx context.Context, _ string) (context.Context, error) {
				if detached {
					var stop context.CancelFunc
					ctx, stop = context.WithCancel(context.Background())
					stop()
				} else {
					cancel()
					ctx = context.Background()
				}
				return WithModelSkipped(ctx, true), nil
			}
			r := openRegion(t, cfg)
			receive(t, r, "old", "question")
			in, err := r.claim(parent)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.process(parent, in); err != nil {
				t.Fatal(err)
			}
			got := waitStatus(t, r, "old", StatusInterrupted)
			if got.StopReason != minipi.StopCanceled || got.LastError == "" || len(messages(t, r, "old")) != 1 {
				t.Fatal("cancellation was reported as a successful skip")
			}
		})
	}
}

func TestDeliveryHistoryAndRestart(t *testing.T) {
	for _, name := range []string{"untracked", "no_tool", "not_sent", "unknown", "sent", "sent_unknown", "unknown_sent", "validation"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
				if name != "no_tool" && req.Messages[len(req.Messages)-1].Role == minipi.RoleUser {
					out := toolCalls("call")
					out.Message.Content = "I already sent the reply"
					return out, nil
				}
				if name != "no_tool" && (len(req.Messages) != 3 || req.Messages[1].Content != "I already sent the reply" || len(req.Messages[1].ToolCalls) != 1) {
					t.Error("current tool round was filtered")
				}
				return answer("I did not send anything"), nil
			}))
			if name != "untracked" {
				cfg.InputContext = func(ctx context.Context, _ string) (context.Context, error) {
					return replydelivery.WithTracking(ctx), nil
				}
			}
			tool := fakeTool(func(ctx context.Context, _ minipi.ToolCall, _ func(minipi.ToolResult)) (minipi.ToolResult, error) {
				switch name {
				case "unknown":
					replydelivery.Started(ctx)
				case "sent", "sent_unknown":
					replydelivery.Confirmed(ctx)
					if name == "sent_unknown" {
						replydelivery.Started(ctx)
					}
				case "unknown_sent":
					replydelivery.Started(ctx)
					replydelivery.Confirmed(ctx)
				}
				return minipi.ToolResult{Content: "sent not_sent unknown", IsError: true}, nil
			})
			if name == "validation" {
				tool.Validate = func(json.RawMessage) error { return errors.New("invalid") }
				tool.Execute = func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
					t.Error("invalid tool executed")
					return minipi.ToolResult{}, nil
				}
			}
			cfg.Agent.Tools = []minipi.Tool{tool}
			r := openRegion(t, cfg)
			receive(t, r, "old", "question")
			in, err := r.claim(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.process(ctx, in); err != nil {
				t.Fatal(err)
			}
			waitStatus(t, r, "old", StatusSucceeded)
			wantState := replydelivery.NotSent
			switch name {
			case "untracked":
				wantState = replydelivery.Untracked
			case "unknown":
				wantState = replydelivery.Unknown
			case "sent", "sent_unknown", "unknown_sent":
				wantState = replydelivery.Sent
			}
			var state replydelivery.State
			if err := r.db.QueryRow(`SELECT qq_delivery FROM inputs WHERE id='old'`).Scan(&state); err != nil || state != wantState {
				t.Fatalf("state=%q, want=%q, err=%v", state, wantState, err)
			}
			original := messages(t, r, "old")
			// Even a persisted not-sent state must not filter this input's own rounds.
			current, err := r.history(ctx, in)
			if err != nil || len(current.Messages) != len(original) {
				t.Fatalf("current history: %v", err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			r = openRegion(t, cfg)
			receive(t, r, "next", "next question")
			next, err := r.Get(ctx, "next")
			if err != nil {
				t.Fatal(err)
			}
			history, err := r.history(ctx, next)
			if err != nil {
				t.Fatal(err)
			}
			expected := []minipi.Message{{Role: minipi.RoleUser, Content: "question"}}
			if wantState != replydelivery.NotSent {
				if name != "no_tool" {
					expected = append(expected, minipi.Message{Role: minipi.RoleAssistant, Content: "I already sent the reply"})
				}
				expected = append(expected, minipi.Message{Role: minipi.RoleAssistant, Content: "I did not send anything"})
			}
			expected = append(expected, minipi.Message{Role: minipi.RoleUser, Content: "next question"})
			if !reflect.DeepEqual(history.Messages, expected) {
				t.Fatalf("history mismatch: got %+v, want %+v", history.Messages, expected)
			}
			if !reflect.DeepEqual(messages(t, r, "old"), original) {
				t.Fatal("audit records changed")
			}
			// Context byte accounting must use the filtered copy, not original prose.
			budget := len(r.cfg.SystemPrompt)
			for _, m := range expected {
				raw, err := json.Marshal(encodeMessage(m))
				if err != nil {
					t.Fatal(err)
				}
				budget += len(raw)
			}
			r.cfg.MaxContextBytes = budget
			if _, err := r.history(ctx, next); err != nil {
				t.Fatal(err)
			}
			r.cfg.MaxContextBytes--
			if _, err := r.history(ctx, next); !errors.Is(err, ErrContextCapacity) {
				t.Fatalf("capacity: %v", err)
			}
		})
	}
}

func TestDeliveryV1Migration(t *testing.T) {
	for _, name := range []string{"normal", "wrong_identity", "rollback"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
				return answer("unused"), nil
			}))
			db, err := sql.Open("sqlite", cfg.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			legacy := strings.Replace(schema, ",\n qq_delivery TEXT NOT NULL DEFAULT '' CHECK(qq_delivery IN ('','not_sent','unknown','sent'))", "", 1)
			if legacy == schema {
				t.Fatal("legacy fixture still has delivery column")
			}
			if _, err := db.Exec(legacy); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO brain_meta VALUES(1,1,'main',0); INSERT INTO inputs(id,text,status,received_at) VALUES('old','old','succeeded',1)`); err != nil {
				t.Fatal(err)
			}
			for i, m := range []minipi.Message{{Role: minipi.RoleUser, Content: "old"}, {Role: minipi.RoleAssistant, Content: "legacy answer"}} {
				raw, err := json.Marshal(encodeMessage(m))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO messages(input_id,message,turn,input_tokens,output_tokens,stop_reason,error,created_at) VALUES('old',?,?,0,0,'','',1)`, raw, i); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`UPDATE brain_meta SET stored_bytes=stored_bytes+?`, len(raw)); err != nil {
					t.Fatal(err)
				}
			}
			if name == "wrong_identity" {
				cfg.ID = "other"
			}
			if name == "rollback" {
				if _, err := db.Exec(`CREATE TRIGGER fail_version BEFORE UPDATE OF version ON brain_meta BEGIN SELECT RAISE(ABORT, 'failure'); END;`); err != nil {
					t.Fatal(err)
				}
			}
			var before int64
			if err := db.QueryRow(`SELECT stored_bytes FROM brain_meta`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			r, err := Open(ctx, cfg)
			if name != "normal" {
				if err == nil {
					r.Close()
					t.Fatal("invalid migration succeeded")
				}
				if name == "wrong_identity" && !errors.Is(err, ErrIdentity) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				var version int
				var after int64
				if err := db.QueryRow(`SELECT version,stored_bytes FROM brain_meta`).Scan(&version, &after); err != nil || version != 2 || after != before {
					r.Close()
					t.Fatalf("migration metadata: version=%d bytes=%d/%d err=%v", version, after, before, err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				r, err = Open(ctx, cfg)
				if err != nil {
					t.Fatal("reopen migrated database:", err)
				}
				defer r.Close()
				receive(t, r, "next", "next")
				next, err := r.Get(ctx, "next")
				if err != nil {
					t.Fatal(err)
				}
				history, err := r.history(ctx, next)
				if err != nil || len(history.Messages) != 3 || history.Messages[1].Content != "legacy answer" {
					t.Fatalf("legacy history changed: %+v %v", history, err)
				}
				var state string
				if err := db.QueryRow(`SELECT qq_delivery FROM inputs WHERE id='old'`).Scan(&state); err != nil || state != "" {
					t.Fatalf("legacy state: %q %v", state, err)
				}
				return
			}
			var version, columns int
			var after int64
			if err := db.QueryRow(`SELECT version,stored_bytes FROM brain_meta`).Scan(&version, &after); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('inputs') WHERE name='qq_delivery'`).Scan(&columns); err != nil {
				t.Fatal(err)
			}
			if version != 1 || columns != 0 || before != after {
				t.Fatal("failed migration changed database")
			}
		})
	}
}
