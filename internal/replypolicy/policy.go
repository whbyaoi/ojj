// Package replypolicy enforces host-owned, current-window QQ reply authorization.
package replypolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"database/sql"
	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
)

var (
	ErrSkipped = errors.New("replypolicy: reply skipped")
	ErrInvalid = errors.New("replypolicy: invalid configuration or input")
	ErrClosed  = errors.New("replypolicy: closed")
	ErrStorage = errors.New("replypolicy: storage unavailable")
	ErrSend    = errors.New("replypolicy: send outcome unknown")
)

// Sender deliberately does not depend on agenttools.
type Sender interface {
	SendText(context.Context, qq.TextMessage) (qq.SentMessage, error)
}

type Config struct {
	Path, AccountID                                       string
	BotUserID                                             string
	Aliases                                               []string
	Proactive                                             bool
	ActiveInterval, ActiveWindow, SessionTTL, TotalWindow time.Duration
	ActiveLimit, TotalLimit                               int
}

// DefaultConfig requires the host to supply an independent Path and AccountID.
func DefaultConfig() Config {
	return Config{ActiveInterval: 90 * time.Second, ActiveWindow: 10 * time.Minute,
		ActiveLimit: 3, SessionTTL: 2 * time.Minute, TotalWindow: time.Minute, TotalLimit: 12}
}

// Validate checks configuration without opening storage or modifying the caller.
// Zero budget fields use the same defaults as Open.
func (c Config) Validate() error {
	_, err := c.normalized()
	return err
}

func (c Config) normalized() (Config, error) {
	d := DefaultConfig()
	if c.ActiveInterval == 0 {
		c.ActiveInterval = d.ActiveInterval
	}
	if c.ActiveWindow == 0 {
		c.ActiveWindow = d.ActiveWindow
	}
	if c.SessionTTL == 0 {
		c.SessionTTL = d.SessionTTL
	}
	if c.TotalWindow == 0 {
		c.TotalWindow = d.TotalWindow
	}
	if c.ActiveLimit == 0 {
		c.ActiveLimit = d.ActiveLimit
	}
	if c.TotalLimit == 0 {
		c.TotalLimit = d.TotalLimit
	}
	if strings.TrimSpace(c.Path) == "" || c.Path == ":memory:" || strings.HasPrefix(c.Path, "file:") ||
		strings.ContainsRune(c.Path, '\x00') || !utf8.ValidString(c.Path) ||
		!validID(c.AccountID) || (c.BotUserID != "" && !validID(c.BotUserID)) ||
		c.ActiveInterval < 0 || c.ActiveWindow < c.ActiveInterval || c.SessionTTL < 0 ||
		c.TotalWindow < 0 || c.ActiveLimit < 1 || c.TotalLimit < 1 || len(c.Aliases) > 32 {
		return c, ErrInvalid
	}
	c.Aliases = append([]string(nil), c.Aliases...)
	for _, a := range c.Aliases {
		if strings.TrimSpace(a) != a || a == "" || len(a) > 128 || !utf8.ValidString(a) {
			return c, ErrInvalid
		}
	}
	return c, nil
}

// Policy is safe for concurrent Bind, SendText and Close. Close cancels in-flight
// operations and waits for Sender to return; Sender must honor context cancellation.
type Policy struct {
	generation int64 // Immutable after Open; fences every state transaction.
	cfg        Config
	sender     Sender
	db         *sql.DB
	now        func() time.Time // Set only before operations, for package tests.
	mu         sync.Mutex
	closed     bool
	life       context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	done       chan struct{}
	closeErr   error
}

func (p *Policy) operation(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	p.wg.Add(1)
	op, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.life, cancel)
	return op, func() { stop(); cancel(); p.wg.Done() }, nil
}

func (p *Policy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.done
		return p.closeErr
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()
	p.wg.Wait()
	if p.db.Close() != nil {
		p.closeErr = ErrStorage
	}
	close(p.done)
	return p.closeErr
}

func safeError(ctx context.Context, fallback error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fallback
}

func nilSender(s Sender) bool {
	if s == nil {
		return true
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func configDigest(c Config) string {
	b, _ := json.Marshal(c)
	return digest(string(b))
}

func digest(parts ...string) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validID(s string) bool {
	return s != "" && strings.TrimSpace(s) == s && len(s) <= 256 && utf8.ValidString(s)
}

const (
	modeSilent = iota
	modeDirect
	modeContinue
	modeActive
	modePrivate
	modeShort       // Classified separately; persisted as modeContinue with a stricter guard.
	maxEventAge     = 2 * time.Minute
	maxReservations = 1_000_000
	maxActors       = 100_000
	maxWindowEvents = 4096
)

// skipReason is internal diagnostics only; public callers still receive ErrSkipped.
// Values must be fixed labels, never content, identifiers or raw errors.
type skipReason string

func (r skipReason) Error() string { return ErrSkipped.Error() }
func (r skipReason) Unwrap() error { return ErrSkipped }

func reasonOf(err error) string {
	var reason skipReason
	if errors.As(err, &reason) {
		return string(reason)
	}
	return "policy_skipped"
}

func logDecision(ctx context.Context, stage, outcome, reason string, started time.Time, attrs ...slog.Attr) {
	fields := []slog.Attr{slog.String("outcome", outcome), slog.String("reason", reason)}
	minipi.LogOperation(ctx, "replypolicy", stage, started, nil, slog.LevelInfo, append(fields, attrs...)...)
}

type candidate struct {
	actor, target, message, token string
	received, occurred            int64
	sequence                      int64
	mode                          int
	fresh                         bool
	short                         bool
	reason                        string     // Fixed reason for a silent candidate; never persisted.
	fence                         actorFence // Final actor state of this binding, not this message's state.
	expires                       int64      // Original continuation deadline; later events cannot extend it.
}

// ACK/session/focus updates are checked separately; they must not invalidate
// unrelated direct candidates. Ordering and mode changes always fence a binding.
type actorFence struct {
	token                        string
	received, occurred, sequence int64
	mode                         int
}

type binding struct {
	owner      *Policy
	candidates map[string]candidate
	reason     string // Bind rejection retained for send diagnostics only.
}

type bindingKey struct{}

// Bind must only receive events restored by the host from its trusted event store,
// never model parameters or history. Even an empty window masks older bindings.
// All trusted events advance/close state; each message keeps its own classification.
// Earlier events already superseded before this Bind remain ineligible on replay.
func (p *Policy) Bind(ctx context.Context, events []event.Event) (result context.Context, err error) {
	started := time.Now()
	outcome, reason := "blocked", "operation_unavailable"
	accepted, eligible := 0, 0
	defer func() {
		minipi.LogOperation(ctx, "replypolicy", "bind", started, err, slog.LevelInfo,
			slog.String("outcome", outcome), slog.String("reason", reason),
			slog.Int("events", len(events)), slog.Int("accepted", accepted),
			slog.Int("filtered", len(events)-accepted), slog.Int("eligible", eligible))
	}()
	op, finish, err := p.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if len(events) > maxWindowEvents {
		reason = "window_capacity"
		return nil, ErrInvalid
	}
	candidates := p.window(events, p.now())
	accepted = len(candidates)
	reason = "bind_state_error"
	bound := &binding{owner: p, candidates: make(map[string]candidate)}
	conflict := p.conflictingWindow(events)
	err = p.transaction(op, func(tx *sql.Tx, now time.Time) error {
		if conflict {
			return skipReason("window_conflict")
		}
		if err := checkClock(op, tx, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(op, `UPDATE actors SET expires=0 WHERE expires>0 AND expires<=?`, now.UnixNano()); err != nil {
			return err
		}
		for _, c := range candidates {
			if _, duplicate := bound.candidates[c.message]; duplicate {
				continue // Conflicting duplicates were rejected before any state change.
			}
			if err := p.advance(op, tx, &c, now); err != nil {
				return err
			}
			bound.candidates[c.message] = c
		}
		// Capture only after every event has advanced, in the same transaction.
		// Publish nothing until commit; never re-authorize an event_older candidate.
		fences := make(map[string]actorFence)
		for key, c := range bound.candidates {
			if c.mode == modeSilent {
				continue
			}
			fence, ok := fences[c.actor]
			if !ok {
				if err := tx.QueryRowContext(op, `SELECT token,received,occurred,sequence,mode FROM actors WHERE actor=?`, c.actor).
					Scan(&fence.token, &fence.received, &fence.occurred, &fence.sequence, &fence.mode); err != nil {
					return err
				}
				fences[c.actor] = fence
			}
			c.fence = fence
			if fence.mode == modeSilent || (c.mode == modeContinue && fence.mode != modeContinue) || (c.short && fence.token != c.token) {
				c.mode, c.reason = modeSilent, "candidate_superseded"
			}
			bound.candidates[key] = c
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, ErrSkipped) {
			return nil, err
		}
		if err := op.Err(); err != nil {
			return nil, err
		}
		// A policy skip must not stop host input restoration. The transaction
		// rolled back, so discard partial permissions and mask any parent binding.
		reason = reasonOf(err)
		bound = &binding{owner: p, reason: reason}
	} else {
		// Aggregate fixed reasons only after commit; rolled-back permissions are never logged as ready.
		counts := make(map[string]int)
		for _, c := range bound.candidates {
			if c.mode != modeSilent {
				eligible++
			} else {
				r := c.reason
				if r == "" {
					r = "silent_candidate"
				}
				counts[r]++
			}
		}
		for r, count := range counts {
			logDecision(ctx, "bind_candidate", "silent", r, started, slog.Int("count", count))
		}
		outcome, reason = "ready", "candidates_available"
		if eligible == 0 {
			outcome, reason = "blocked", "no_eligible_candidate"
			if accepted == 0 {
				reason = "no_trusted_candidate"
				bound.reason = reason
			}
		}
	}
	return context.WithValue(ctx, bindingKey{}, bound), nil
}

// Precheck reports whether any current bound candidate can still send. It does
// not reserve quota, advance clocks or mutate bindings. A positive result is
// advisory only: SendText must still atomically check and reserve immediately
// before sending. Policy blocks are normal false results; storage errors are not.
func (p *Policy) Precheck(ctx context.Context) (ready bool, err error) {
	started := time.Now()
	outcome, reason := "blocked", "operation_unavailable"
	checked := 0
	defer func() {
		minipi.LogOperation(ctx, "replypolicy", "precheck", started, err, slog.LevelInfo,
			slog.String("outcome", outcome), slog.String("reason", reason), slog.Int("checked", checked))
	}()
	op, finish, err := p.operation(ctx)
	if err != nil {
		return false, err
	}
	defer finish()
	b, _ := ctx.Value(bindingKey{}).(*binding)
	if b == nil || b.owner != p {
		reason = "binding_unavailable"
		return false, safeError(op, nil)
	}
	if b.reason != "" {
		reason = b.reason
		return false, safeError(op, nil)
	}
	reason = "no_eligible_candidate"
	err = p.transaction(op, func(tx *sql.Tx, now time.Time) error {
		for _, c := range b.candidates {
			if c.mode == modeSilent {
				continue
			}
			checked++
			if checkErr := p.checkReservation(op, tx, c, now); checkErr != nil {
				if !errors.Is(checkErr, ErrSkipped) {
					return checkErr
				}
				logDecision(ctx, "precheck_candidate", "blocked", reasonOf(checkErr), started)
				continue
			}
			ready = true
			return nil
		}
		return nil
	})
	if op.Err() != nil {
		return false, op.Err()
	}
	if errors.Is(err, ErrSkipped) {
		reason = reasonOf(err)
		return false, nil
	}
	if err != nil {
		reason = "precheck_state_error"
		return false, err
	}
	if ready {
		outcome, reason = "ready", "candidate_available"
	}
	return ready, nil
}

// SendText reserves durably before calling Sender exactly once. All failed or
// unknown sends retain both quota and idempotency; changing MsgSeq cannot retry.
// After a platform ACK, policy skips only suppress session updates. Storage errors
// remain errors, but retain the acknowledged result and never mean "not sent".
func (p *Policy) SendText(ctx context.Context, msg qq.TextMessage) (result qq.SentMessage, err error) {
	started := time.Now()
	stage, outcome, reason := "send", "not_sent", "operation_unavailable"
	defer func() {
		logErr := err
		if errors.Is(err, ErrSkipped) {
			outcome, logErr = "skipped", nil
			if reason == "reserve" {
				reason = reasonOf(err)
			}
			// Internal diagnostics must not change the public sentinel.
			err = ErrSkipped
		}
		minipi.LogOperation(ctx, "replypolicy", stage, started, logErr, slog.LevelInfo,
			slog.String("outcome", outcome), slog.String("reason", reason))
	}()
	op, finish, err := p.operation(ctx)
	if err != nil {
		return qq.SentMessage{}, err
	}
	defer finish()
	b, _ := ctx.Value(bindingKey{}).(*binding)
	if b == nil {
		reason = "binding_missing"
		return qq.SentMessage{}, ErrSkipped
	}
	if b.owner != p {
		reason = "binding_owner_mismatch"
		return qq.SentMessage{}, ErrSkipped
	}
	if msg.MsgID == "" || qq.ValidateTextMessage(msg) != nil {
		reason = "invalid_reply_parameters"
		return qq.SentMessage{}, ErrSkipped
	}
	if b.reason != "" {
		reason = b.reason
		return qq.SentMessage{}, ErrSkipped
	}
	target := digest(p.cfg.AccountID, msg.TargetType, msg.TargetID)
	message := digest(target, msg.MsgID)
	chosen, ok := b.candidates[message]
	reason = "candidate_not_in_binding"
	if !ok || chosen.target != target || chosen.message != message {
		return qq.SentMessage{}, ErrSkipped
	}
	if chosen.mode == modeSilent {
		reason = chosen.reason
		if reason == "" {
			reason = "silent_candidate"
		}
		return qq.SentMessage{}, ErrSkipped
	}
	reason = "reserve"
	err = p.transaction(op, func(tx *sql.Tx, now time.Time) error { return p.reserve(op, tx, chosen, now) })
	if err != nil {
		return qq.SentMessage{}, err
	}
	// Cancellation after a successful reservation is intentionally charged.
	reason = "reserved_before_send"
	if err := op.Err(); err != nil {
		return qq.SentMessage{}, err
	}
	outcome, reason = "unknown", "transport_error"
	replydelivery.Started(ctx)
	sent, err := p.sender.SendText(op, msg)
	if strings.TrimSpace(sent.ID) != "" {
		replydelivery.Confirmed(ctx)
	}
	// A nil transport error alone is not a platform acknowledgement.
	if err != nil || strings.TrimSpace(sent.ID) == "" {
		if err == nil {
			reason = "ack_missing"
		}
		return qq.SentMessage{}, safeError(op, ErrSend)
	}
	stage, outcome, reason = "send_ack", "sent", "platform_ack"
	if chosen.mode == modeDirect || chosen.mode == modeContinue {
		reason = "session_updated"
		err = p.transaction(op, func(tx *sql.Tx, now time.Time) error {
			// Neither delayed ACK nor another instance may reopen a superseded session.
			if err := checkClock(op, tx, now); err != nil {
				return err
			}
			if timeReason := timeFailureReason(chosen.received, chosen.occurred, now); timeReason != "" {
				reason = "ack_" + timeReason
				return nil
			}
			updated, e := tx.ExecContext(op, `UPDATE actors SET expires=? WHERE actor=? AND token=? AND mode=? AND (mode<>? OR expires>?) AND (?=0 OR short_ok=1)`,
				now.Add(p.cfg.SessionTTL).UnixNano(), chosen.actor, chosen.token, chosen.mode, modeContinue, now.UnixNano(), chosen.short)
			if e == nil {
				// RowsAffected is diagnostic only; never introduce a new failure path.
				if n, rowsErr := updated.RowsAffected(); rowsErr == nil && n == 0 {
					reason = "ack_session_superseded_or_expired"
				}
			}
			return e
		})
		if err != nil {
			reason = "ack_session_update_failed"
			// Cancellation takes precedence even when the state fence skipped.
			if cancelErr := op.Err(); cancelErr != nil {
				reason = "ack_session_update_canceled"
				return sent, cancelErr
			}
			if !errors.Is(err, ErrSkipped) {
				return sent, err
			}
			reason = reasonOf(err)
		}
	}
	return sent, nil
}
