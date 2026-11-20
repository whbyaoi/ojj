package replypolicy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"ojj/internal/im/qq"
	"ojj/internal/replydelivery"
)

type attachmentSenderStub struct {
	Sender
	attachment func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error)
}

func (s *attachmentSenderStub) SendAttachment(ctx context.Context, msg qq.AttachmentMessage) (qq.SentMessage, error) {
	return s.attachment(ctx, msg)
}

type attachmentVetoStub struct{ *attachmentSenderStub }

func (*attachmentVetoStub) SupportsAttachments() bool { return false }

func imageMessage(m qq.TextMessage) qq.AttachmentMessage {
	return qq.AttachmentMessage{TargetType: m.TargetType, TargetID: m.TargetID, MsgID: m.MsgID, MsgSeq: m.MsgSeq,
		Attachment: qq.OutgoingAttachment{Kind: qq.AttachmentImage, URL: "https://example.com/image.png"}}
}

func installAttachmentStub(p *Policy, calls *atomic.Int64) *attachmentSenderStub {
	s := &attachmentSenderStub{Sender: p.sender, attachment: func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error) {
		calls.Add(1)
		return qq.SentMessage{ID: "sent"}, nil
	}}
	p.sender = s
	return s
}

func TestAttachmentCapabilitiesAndPreflight(t *testing.T) {
	var absent *Policy
	if absent.SupportsAttachments() || (&Policy{}).SupportsAttachments() {
		t.Fatal("nil policy reported attachment support")
	}
	for _, name := range []string{"text_only", "typed_nil", "veto", "nested_veto", "unbound", "wrong_owner", "wrong_target", "wrong_message", "silent", "invalid_url", "invalid_kind", "empty_id", "canceled", "reserve_failure"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			e := f.event("group", "user", "hello", true)
			if name == "silent" {
				e.ReceivedAt = e.ReceivedAt.Add(1) // Future input is a silent candidate.
			}
			ctx := bind(t, f.p, e)
			msg := imageMessage(message(e))
			s := installAttachmentStub(f.p, &f.calls)
			if !f.p.SupportsAttachments() {
				t.Fatal("optional attachment sender not recognized")
			}
			want := error(ErrSkipped)
			switch name {
			case "text_only":
				f.p.sender = s.Sender
			case "typed_nil":
				var nilStub *attachmentSenderStub
				f.p.sender = nilStub
			case "veto":
				f.p.sender = &attachmentVetoStub{s}
			case "nested_veto":
				f.p.sender = &Policy{sender: &attachmentVetoStub{s}}
			case "unbound":
				ctx = context.Background()
			case "wrong_owner":
				ctx = bind(t, f.open(t), e)
			case "wrong_target":
				msg.TargetID = "other"
			case "wrong_message":
				msg.MsgID = "other"
			case "invalid_url":
				msg.Attachment.URL = "http://127.0.0.1/private"
			case "invalid_kind":
				msg.Attachment.Kind = qq.AttachmentVideo
			case "empty_id":
				msg.MsgID = ""
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				msg.Attachment.URL = "invalid"
				want = context.Canceled
			case "reserve_failure":
				if _, err := f.p.db.Exec(`CREATE TRIGGER fail_reserve BEFORE INSERT ON reservations BEGIN SELECT RAISE(ABORT, 'failure'); END;`); err != nil {
					t.Fatal("trigger setup failed")
				}
				want = ErrStorage
			}
			if name == "text_only" || name == "typed_nil" || name == "veto" || name == "nested_veto" {
				if f.p.SupportsAttachments() {
					t.Fatal("unsupported sender reported support")
				}
			}
			ctx = replydelivery.WithTracking(ctx)
			_, err := f.p.SendAttachment(ctx, msg)
			if err != want || f.calls.Load() != 0 || replydelivery.Outcome(ctx) != replydelivery.NotSent {
				t.Fatal("preflight did not reject without invoking sender")
			}
			var count int
			if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != 0 {
				t.Fatal("preflight consumed reservation")
			}
		})
	}
}

func TestAttachmentTextConcurrentInstances(t *testing.T) {
	f := newFixture(t, nil)
	second := f.open(t)
	installAttachmentStub(f.p, &f.calls)
	installAttachmentStub(second, &f.calls)
	e := f.event("group", "user", "hello", true)
	contexts := []context.Context{bind(t, f.p, e), bind(t, second, e)}
	policies := []*Policy{f.p, second}
	msg := message(e)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var success atomic.Int64
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p, ctx := policies[i%2], contexts[i%2]
			var err error
			if i%4 < 2 {
				_, err = p.SendAttachment(ctx, imageMessage(msg))
			} else {
				_, err = p.SendText(ctx, msg)
			}
			if err == nil {
				success.Add(1)
			} else if err != ErrSkipped {
				t.Error("unexpected concurrent error class")
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if success.Load() != 1 || f.calls.Load() != 1 {
		t.Fatal("mixed payloads bypassed atomic idempotency")
	}
}

func TestAttachmentDeliveryFailures(t *testing.T) {
	for _, name := range []string{"upload_failure", "send_failure", "empty_ack", "ack", "ack_error", "cancel", "ack_cancel", "ack_storage"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, func(c *Config) { c.TotalLimit = 1 })
			e := f.event("group", "user", "hello", true)
			ctx, cancel := context.WithCancel(replydelivery.WithTracking(bind(t, f.p, e)))
			defer cancel()
			uploads, sends := 0, 0
			stub := installAttachmentStub(f.p, &f.calls)
			stub.attachment = func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				uploads++
				if replydelivery.Outcome(ctx) != replydelivery.Unknown {
					t.Error("attachment operation not marked started")
				}
				if name == "upload_failure" {
					return qq.SentMessage{}, errors.New("private upload detail")
				}
				// Upload success does not publish an ACK or change delivery evidence.
				if replydelivery.Outcome(ctx) != replydelivery.Unknown {
					t.Error("upload success confirmed delivery")
				}
				sends++
				if name == "cancel" || name == "ack_cancel" {
					cancel()
				}
				if name == "send_failure" || name == "cancel" {
					return qq.SentMessage{}, errors.New("private send detail")
				}
				if name == "empty_ack" {
					return qq.SentMessage{ID: "  "}, nil
				}
				if name == "ack_error" {
					return qq.SentMessage{ID: "sent"}, errors.New("private post-ack detail")
				}
				return qq.SentMessage{ID: "sent"}, nil
			}
			if name == "ack_storage" {
				if _, err := f.p.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE OF expires ON actors WHEN NEW.expires > OLD.expires BEGIN SELECT RAISE(ABORT, 'failure'); END;`); err != nil {
					t.Fatal("trigger setup failed")
				}
			}
			wantErr, wantOutcome := error(ErrSend), replydelivery.Unknown
			switch name {
			case "ack":
				wantErr, wantOutcome = nil, replydelivery.Sent
			case "ack_error":
				wantOutcome = replydelivery.Sent
			case "cancel":
				wantErr = context.Canceled
			case "ack_cancel":
				wantErr, wantOutcome = context.Canceled, replydelivery.Sent
			case "ack_storage":
				wantErr, wantOutcome = ErrStorage, replydelivery.Sent
			}
			sent, err := f.p.SendAttachment(ctx, imageMessage(message(e)))
			if err != wantErr || replydelivery.Outcome(ctx) != wantOutcome || uploads != 1 {
				t.Fatal("attachment outcome or sanitized error mismatch")
			}
			if (sends == 0) != (name == "upload_failure") {
				t.Fatal("attachment send stage mismatch")
			}
			if (sent.ID != "") != (name == "ack" || name == "ack_cancel" || name == "ack_storage") {
				t.Fatal("acknowledged result was not preserved")
			}
			var expires int64
			if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || (expires > 0) != (name == "ack") {
				t.Fatal("unconfirmed operation authorized session")
			}
			fresh := bind(t, f.p, e)
			expectACKReservation(t, f, f.p, fresh, message(e), 1)
			if _, err := f.p.SendAttachment(fresh, imageMessage(message(e))); err != ErrSkipped || uploads != 1 {
				t.Fatal("failed attachment retried")
			}
			next := f.event("group", "other", "hello", true)
			expectSend(t, f.p, bind(t, f.p, next), message(next), ErrSkipped)
		})
	}
}

func TestAttachmentDelayedACKCannotUndoNewState(t *testing.T) {
	for _, followup := range []string{"结束", "继续", "new direct"} {
		t.Run(followup, func(t *testing.T) {
			f := newFixture(t, nil)
			second := f.open(t)
			entered, release := make(chan struct{}), make(chan struct{})
			stub := installAttachmentStub(f.p, &f.calls)
			stub.attachment = func(ctx context.Context, _ qq.AttachmentMessage) (qq.SentMessage, error) {
				f.calls.Add(1)
				close(entered)
				select {
				case <-release:
					return qq.SentMessage{ID: "sent"}, nil
				case <-ctx.Done():
					return qq.SentMessage{}, ctx.Err()
				}
			}
			first := f.event("group", "user", "hello", true)
			ctx, cancel := context.WithCancel(replydelivery.WithTracking(bind(t, f.p, first)))
			defer cancel()
			done := make(chan ackResult, 1)
			go func() {
				sent, err := f.p.SendAttachment(ctx, imageMessage(message(first)))
				done <- ackResult{sent, err}
			}()
			<-entered
			next := f.event("group", "user", followup, followup == "new direct")
			bind(t, second, next)
			close(release)
			expectACK(t, <-done)
			if replydelivery.Outcome(ctx) != replydelivery.Sent {
				t.Fatal("superseded session lost attachment ACK evidence")
			}
			expectACKReservation(t, f, f.p, ctx, message(first), 1)
			var expires int64
			if err := second.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || expires != 0 {
				t.Fatal("old attachment ACK authorized newer request")
			}
		})
	}
}

func TestAttachmentTextSharedReservations(t *testing.T) {
	for _, firstImage := range []bool{false, true} {
		t.Run(fmt.Sprint(firstImage), func(t *testing.T) {
			f := newFixture(t, func(c *Config) { c.TotalLimit = 2 })
			installAttachmentStub(f.p, &f.calls)
			e := f.event("group", "user", "hello", true)
			ctx, msg := bind(t, f.p, e), message(e)
			send := func(image bool, ctx context.Context, msg qq.TextMessage, want error) {
				t.Helper()
				var err error
				if image {
					_, err = f.p.SendAttachment(ctx, imageMessage(msg))
				} else {
					_, err = f.p.SendText(ctx, msg)
				}
				if err != want {
					t.Fatal("shared reservation result mismatch")
				}
			}
			send(firstImage, ctx, msg, nil)
			msg.MsgSeq++
			send(!firstImage, ctx, msg, ErrSkipped)
			e = f.event("group", "other", "hello", true)
			send(!firstImage, bind(t, f.p, e), message(e), nil)
			e = f.event("group", "third", "hello", true)
			send(firstImage, bind(t, f.p, e), message(e), ErrSkipped)
			if f.calls.Load() != 2 {
				t.Fatal("text and attachments did not share total budget")
			}
		})
	}
}
