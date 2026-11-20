package replypolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/minipi"
)

func capturePolicyLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := new(bytes.Buffer)
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

func policyRecord(t *testing.T, buf *bytes.Buffer, stage string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var record map[string]any
		if json.Unmarshal(line, &record) != nil {
			t.Fatal("invalid diagnostic JSON")
		}
		if record["component"] == "replypolicy" && record["stage"] == stage {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("missing stage %s", stage)
	}
	return found
}

func TestPolicySkipDiagnostics(t *testing.T) {
	for _, reason := range []string{
		"binding_missing", "no_trusted_candidate", "event_received_expired", "candidate_superseded",
		"message_reserved", "total_quota", "active_quota", "active_interval",
		"clock_rollback", "policy_generation_changed",
	} {
		t.Run(reason, func(t *testing.T) {
			buf := capturePolicyLogs(t)
			f := newFixture(t, func(c *Config) {
				if reason == "total_quota" {
					c.TotalLimit = 1
				}
				if reason == "active_quota" {
					c.ActiveLimit = 1
				}
			})
			e := f.event("private-group", "private-user", "private-body", true)
			if reason == "active_quota" || reason == "active_interval" {
				editBody(&e, func(c *qq.Content) {
					c.QQEvent = "GROUP_MESSAGE_CREATE"
					c.Text = "请问数据库连接超时应该怎么排查？"
				})
			}
			const inputID = "0123456789abcdef0123456789abcdef"
			base := minipi.WithInputID(context.Background(), inputID)
			ctx, err := f.p.Bind(base, []event.Event{e})
			if err != nil {
				t.Fatal("initial bind failed")
			}
			m := message(e)
			m.Content = "private-reply"
			switch reason {
			case "binding_missing":
				ctx = base
			case "no_trusted_candidate":
				bad := e
				bad.ProducerID = "private-untrusted"
				ctx, err = f.p.Bind(base, []event.Event{bad})
			case "event_received_expired":
				f.tick(maxEventAge + time.Nanosecond)
			case "candidate_superseded":
				bind(t, f.p, f.event("private-group", "private-user", "private-later", true))
			case "message_reserved", "total_quota", "active_quota", "active_interval":
				expectSend(t, f.p, ctx, m, nil)
				if reason == "message_reserved" {
					m.MsgSeq = 999
				} else {
					next := f.event("private-group", "private-other", "private-body", true)
					if reason != "total_quota" {
						next.Content = strings.ReplaceAll(e.Content, "private-user", "private-other")
					}
					ctx, err = f.p.Bind(base, []event.Event{next})
					m = message(next)
				}
			case "clock_rollback":
				f.tick(-time.Nanosecond)
			case "policy_generation_changed":
				f.cfg.Proactive = !f.cfg.Proactive
				f.open(t)
			}
			if err != nil {
				t.Fatal("setup bind failed")
			}
			before := f.calls.Load()
			requirePrecheck(t, f.p, ctx, false)
			precheck := policyRecord(t, buf, "precheck")
			if precheck["outcome"] != "blocked" || precheck["level"] != "INFO" || precheck["input_id"] != inputID {
				t.Fatal("unexpected precheck diagnostic")
			}
			switch reason {
			case "binding_missing", "no_trusted_candidate", "policy_generation_changed":
			default:
				candidate := policyRecord(t, buf, "precheck_candidate")
				if candidate["reason"] != reason || candidate["outcome"] != "blocked" || candidate["input_id"] != inputID {
					t.Fatal("unexpected precheck candidate diagnostic")
				}
			}
			sent, err := f.p.SendText(ctx, m)
			if err != ErrSkipped || sent.ID != "" || f.calls.Load() != before {
				t.Fatal("skip changed sentinel or called sender")
			}
			r := policyRecord(t, buf, "send")
			if r["reason"] != reason || r["outcome"] != "skipped" || r["level"] != "INFO" || r["input_id"] != inputID {
				t.Fatalf("unexpected diagnostic: %v", r)
			}
			if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), f.cfg.Path) {
				t.Fatal("sensitive data logged")
			}
			for _, c := range f.p.window([]event.Event{e}, f.now()) {
				for _, secret := range []string{c.actor, c.target, c.message, c.token} {
					if strings.Contains(buf.String(), secret) {
						t.Fatal("digest logged")
					}
				}
			}
		})
	}
}

func TestPolicyACKDiagnostics(t *testing.T) {
	for _, reason := range []string{"session_updated", "ack_event_received_expired", "ack_session_superseded_or_expired", "clock_rollback", "policy_generation_changed", "ack_session_update_failed", "ack_session_update_canceled"} {
		t.Run(reason, func(t *testing.T) {
			buf := capturePolicyLogs(t)
			f := newFixture(t, nil)
			e := f.event("private-group", "private-user", "private-body", true)
			ctx, cancel := context.WithCancel(bind(t, f.p, e))
			defer cancel()
			f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				switch reason {
				case "ack_event_received_expired":
					f.tick(maxEventAge + time.Nanosecond)
				case "ack_session_superseded_or_expired":
					bind(t, f.p, f.event("private-group", "private-user", "private-later", false))
				case "clock_rollback":
					f.tick(-time.Nanosecond)
				case "policy_generation_changed":
					f.cfg.Proactive = true
					f.open(t)
				case "ack_session_update_failed":
					_, err := f.p.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE OF expires ON actors BEGIN SELECT RAISE(ABORT, 'private-storage-detail'); END;`)
					if err != nil {
						t.Fatal("trigger setup failed")
					}
				case "ack_session_update_canceled":
					cancel()
				}
				return qq.SentMessage{ID: "private-ack"}, nil
			})
			buf.Reset()
			sent, err := f.p.SendText(ctx, message(e))
			var want error
			level := "INFO"
			if reason == "ack_session_update_failed" {
				want, level = ErrStorage, "ERROR"
			} else if reason == "ack_session_update_canceled" {
				want, level = context.Canceled, "DEBUG"
			}
			if sent.ID != "private-ack" || err != want || errors.Is(err, ErrSkipped) {
				t.Fatal("ACK lost sent result or exact sentinel")
			}
			r := policyRecord(t, buf, "send_ack")
			if r["outcome"] != "sent" || r["reason"] != reason || r["level"] != level {
				t.Fatalf("ACK diagnostic mismatch: %v", r)
			}
			if strings.Contains(buf.String(), `"outcome":"skipped"`) || strings.Contains(buf.String(), "private-") {
				t.Fatal("ACK reported skipped or leaked sensitive data")
			}
			var count int
			if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != 1 {
				t.Fatal("ACK changed durable reservation")
			}
		})
	}
}

func TestPolicyUnknownDiagnostics(t *testing.T) {
	for _, reason := range []string{"transport_error", "ack_missing"} {
		t.Run(reason, func(t *testing.T) {
			buf := capturePolicyLogs(t)
			f := newFixture(t, func(c *Config) { c.TotalLimit = 1 })
			e := f.event("private-group", "private-user", "private-body", true)
			ctx := bind(t, f.p, e)
			requirePrecheck(t, f.p, ctx, true)
			f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
				if reason == "transport_error" {
					return qq.SentMessage{}, errors.New("private-transport-error")
				}
				return qq.SentMessage{}, nil
			})
			sent, err := f.p.SendText(ctx, message(e))
			if err != ErrSend || sent.ID != "" {
				t.Fatal("unknown send semantics changed")
			}
			r := policyRecord(t, buf, "send")
			if r["outcome"] != "unknown" || r["reason"] != reason || strings.Contains(buf.String(), "private-") {
				t.Fatal("unknown send diagnostic mismatch")
			}
			requirePrecheck(t, f.p, ctx, false)
			next := f.event("private-group", "private-other", "private-body", true)
			requirePrecheck(t, f.p, bind(t, f.p, next), false)
			if f.p.Close() != nil {
				t.Fatal("close failed")
			}
			f.p = f.open(t)
			requirePrecheck(t, f.p, bind(t, f.p, next), false)
			f.tick(f.cfg.TotalWindow)
			requirePrecheck(t, f.p, bind(t, f.p, next), true)
			if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), f.cfg.Path) {
				t.Fatal("sensitive precheck data logged")
			}
		})
	}
}

func TestTimeFailureReasons(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	n := now.UnixNano()
	for _, tc := range []struct {
		name               string
		received, occurred int64
		want               string
	}{
		{"now", n, n, ""},
		{"exact-age", n - int64(maxEventAge), n - int64(maxEventAge), ""},
		{"occurrence-exact-age", n, n - int64(maxEventAge), ""},
		{"zero", n, 0, "event_occurred_invalid"},
		{"negative", n, -1, "event_occurred_invalid"},
		{"occurrence-future", n, n + 1, "event_occurred_in_future"},
		{"both-future", n + 1, n + 1, "event_occurred_in_future"},
		{"receive-future", n + 1, n, "event_received_in_future"},
		{"inverted", n - 1, n, "event_time_inverted"},
		{"receive-nonpositive", 0, n, "event_time_inverted"},
		{"both-expired", n - int64(maxEventAge) - 1, n - int64(maxEventAge) - 1, "event_received_expired"},
		{"occurrence-expired", n, n - int64(maxEventAge) - 1, "event_occurred_expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := timeFailureReason(tc.received, tc.occurred, now); got != tc.want {
				t.Fatalf("reason = %q, want %q", got, tc.want)
			}
			if freshTimes(tc.received, tc.occurred, now) != (tc.want == "") {
				t.Fatal("authorization boundary changed")
			}
			buf := capturePolicyLogs(t)
			f := newFixture(t, nil)
			e := f.event("private-group", "private-user", "private-body", true)
			e.ReceivedAt, e.OccurredAt = time.Unix(0, tc.received), time.Unix(0, tc.occurred)
			// Exercise new candidates and the same-token path.
			for i := 0; i < 2; i++ {
				buf.Reset()
				ctx := bind(t, f.p, e)
				if tc.want == "" {
					if policyRecord(t, buf, "bind")["eligible"] != float64(1) {
						t.Fatal("fresh candidate blocked")
					}
					continue
				}
				sent, err := f.p.SendText(ctx, message(e))
				if err != ErrSkipped || sent.ID != "" || f.calls.Load() != 0 {
					t.Fatal("time rejection changed send semantics")
				}
				for _, stage := range []string{"bind_candidate", "send"} {
					if policyRecord(t, buf, stage)["reason"] != tc.want {
						t.Fatalf("lost time diagnosis at %s", stage)
					}
				}
				var count int
				if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != 0 {
					t.Fatal("blocked event consumed reservation")
				}
				if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), f.cfg.Path) || strings.Contains(buf.String(), now.Format("2006-01-02")) {
					t.Fatal("sensitive data or event timestamp logged")
				}
				for _, c := range f.p.window([]event.Event{e}, f.now()) {
					for _, secret := range []string{c.actor, c.target, c.message, c.token} {
						if strings.Contains(buf.String(), secret) {
							t.Fatal("digest logged")
						}
					}
				}
			}
		})
	}
}

func TestTimeDiagnosisTransitions(t *testing.T) {
	for _, stage := range []string{"bind", "reserve", "ack"} {
		for _, occurrenceOnly := range []bool{false, true} {
			for _, over := range []time.Duration{0, time.Nanosecond} {
				t.Run(stage+"/"+time.Duration(over).String()+"/"+map[bool]string{false: "both", true: "occurrence"}[occurrenceOnly], func(t *testing.T) {
					buf := capturePolicyLogs(t)
					f := newFixture(t, nil)
					e := f.event("private-group", "private-user", "private-body", true)
					delay := maxEventAge + over
					want := "event_received_expired"
					if occurrenceOnly {
						e.OccurredAt = e.OccurredAt.Add(-time.Minute)
						delay -= time.Minute
						want = "event_occurred_expired"
					}
					var ctx context.Context
					switch stage {
					case "bind":
						// Age between window classification and advance's recheck.
						calls := 0
						f.p.now = func() time.Time {
							calls++
							if calls == 2 {
								f.tick(delay)
							}
							return f.now()
						}
						ctx = bind(t, f.p, e)
						f.p.now = f.now
					case "reserve":
						ctx = bind(t, f.p, e)
						f.tick(delay)
					case "ack":
						ctx = bind(t, f.p, e)
						f.p.sender = senderFunc(func(context.Context, qq.TextMessage) (qq.SentMessage, error) {
							f.calls.Add(1)
							f.tick(delay)
							return qq.SentMessage{ID: "sent"}, nil
						})
					}
					sent, err := f.p.SendText(ctx, message(e))
					if over == 0 || stage == "ack" {
						if err != nil || sent.ID != "sent" || f.calls.Load() != 1 {
							t.Fatal("boundary or ACK result changed")
						}
					} else if err != ErrSkipped || sent.ID != "" || f.calls.Load() != 0 {
						t.Fatal("expired event was sent")
					}
					if over == 0 {
						want = "session_updated"
					} else if stage == "ack" {
						want = "ack_" + want
					}
					logStage := "send"
					if sent.ID != "" {
						logStage = "send_ack"
					}
					if policyRecord(t, buf, logStage)["reason"] != want {
						t.Fatalf("lost %s time reason", stage)
					}
					if stage == "bind" && over != 0 && policyRecord(t, buf, "bind_candidate")["reason"] != want {
						t.Fatal("advance did not refresh diagnosis")
					}
					var count, expires int64
					if err := f.p.db.QueryRow(`SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil || count != f.calls.Load() {
						t.Fatal("reservation changed")
					}
					if err := f.p.db.QueryRow(`SELECT expires FROM actors`).Scan(&expires); err != nil || (expires > 0) != (over == 0) {
						t.Fatal("session boundary changed")
					}
				})
			}
		}
	}
}

func TestRebindRetainsWindowTimeReason(t *testing.T) {
	buf := capturePolicyLogs(t)
	f := newFixture(t, nil)
	e := f.event("private-group", "private-user", "private-body", true)
	e.ReceivedAt = e.ReceivedAt.Add(time.Second)
	e.OccurredAt = e.ReceivedAt
	bind(t, f.p, e)
	calls := 0
	f.p.now = func() time.Time {
		calls++
		if calls == 2 {
			f.tick(time.Second)
		}
		return f.now()
	}
	buf.Reset()
	ctx := bind(t, f.p, e)
	f.p.now = f.now
	sent, err := f.p.SendText(ctx, message(e))
	if err != ErrSkipped || sent.ID != "" || f.calls.Load() != 0 {
		t.Fatal("clock catch-up revived window authorization")
	}
	for _, stage := range []string{"bind_candidate", "send"} {
		if policyRecord(t, buf, stage)["reason"] != "event_occurred_in_future" {
			t.Fatal("window diagnosis was erased at transaction time")
		}
	}
}

func TestPolicyBindDiagnostics(t *testing.T) {
	for _, reason := range []string{"event_received_expired", "clock_rollback", "policy_generation_changed"} {
		t.Run(reason, func(t *testing.T) {
			buf := capturePolicyLogs(t)
			f := newFixture(t, nil)
			e := f.event("private-group", "private-user", "private-body", true)
			bind(t, f.p, e)
			switch reason {
			case "event_received_expired":
				f.tick(maxEventAge + time.Nanosecond)
			case "clock_rollback":
				f.tick(-time.Nanosecond)
			case "policy_generation_changed":
				f.cfg.Proactive = true
				f.open(t)
			}
			buf.Reset()
			ctx := bind(t, f.p, e)
			r := policyRecord(t, buf, "bind")
			if r["outcome"] != "blocked" || r["eligible"] != float64(0) || r["accepted"] != float64(1) || r["filtered"] != float64(0) {
				t.Fatal("incorrect bind counters")
			}
			if reason == "event_received_expired" {
				r = policyRecord(t, buf, "bind_candidate")
			}
			if r["reason"] != reason {
				t.Fatal("lost bind rejection reason")
			}
			_, err := f.p.SendText(ctx, message(e))
			if err != ErrSkipped || policyRecord(t, buf, "send")["reason"] != reason {
				t.Fatal("lost send rejection reason")
			}
		})
	}
}
