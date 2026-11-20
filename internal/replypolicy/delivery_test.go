package replypolicy

import (
	"context"
	"errors"
	"testing"

	"ojj/internal/im/qq"
	"ojj/internal/replydelivery"
)

func TestHostDeliveryEvidence(t *testing.T) {
	for _, name := range []string{"none", "skip", "invalid", "reserve_failure", "pre_cancel", "ack", "ack_error", "empty_ack", "error", "panic", "cancel", "ack_cancel", "ack_storage"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			e := f.event("group", "user", "hello", true)
			ctx, cancel := context.WithCancel(replydelivery.WithTracking(bind(t, f.p, e)))
			defer cancel()
			msg := message(e)
			want := replydelivery.NotSent
			var wantErr error
			calls := 0
			f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				calls++
				switch name {
				case "panic":
					panic("transport panic")
				case "cancel", "ack_cancel":
					cancel()
				case "error":
					return qq.SentMessage{}, errors.New("unknown transport outcome")
				case "ack_error":
					return qq.SentMessage{ID: "ack"}, errors.New("error after ack")
				}
				if name == "empty_ack" || name == "cancel" {
					return qq.SentMessage{}, nil
				}
				return qq.SentMessage{ID: "ack"}, nil
			})
			switch name {
			case "skip":
				msg.TargetID = "other"
				wantErr = ErrSkipped
			case "invalid":
				msg.Content = ""
				wantErr = ErrSkipped
			case "reserve_failure":
				if _, err := f.p.db.Exec(`CREATE TRIGGER fail_reserve BEFORE INSERT ON reservations BEGIN SELECT RAISE(ABORT, 'failure'); END;`); err != nil {
					t.Fatal(err)
				}
				wantErr = ErrStorage
			case "pre_cancel":
				cancel()
				wantErr = context.Canceled
			case "ack", "ack_error", "ack_cancel", "ack_storage":
				want = replydelivery.Sent
				if name == "ack_error" {
					wantErr = ErrSend
				}
				if name == "ack_cancel" {
					wantErr = context.Canceled
				}
				if name == "ack_storage" {
					if _, err := f.p.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE OF expires ON actors WHEN NEW.expires > OLD.expires BEGIN SELECT RAISE(ABORT, 'failure'); END;`); err != nil {
						t.Fatal(err)
					}
					wantErr = ErrStorage
				}
			default:
				if name != "none" {
					want = replydelivery.Unknown
					wantErr = ErrSend
					if name == "cancel" {
						wantErr = context.Canceled
					}
				}
			}
			if name != "none" {
				func() {
					defer func() {
						p := recover()
						if (p != nil) != (name == "panic") {
							t.Fatal("unexpected panic outcome")
						}
					}()
					_, err := f.p.SendText(ctx, msg)
					if err != wantErr {
						t.Fatalf("error=%v, want %v", err, wantErr)
					}
				}()
			}
			if got := replydelivery.Outcome(ctx); got != want {
				t.Fatalf("delivery=%q, want %q", got, want)
			}
			if (calls == 0) != (want == replydelivery.NotSent) {
				t.Fatalf("transport calls=%d, state=%q", calls, want)
			}
			fresh := replydelivery.WithTracking(ctx)
			if replydelivery.Outcome(fresh) != replydelivery.NotSent || replydelivery.Outcome(context.Background()) != replydelivery.Untracked || replydelivery.Outcome(nil) != replydelivery.Untracked {
				t.Fatal("tracking leaked between inputs")
			}
		})
	}
}
