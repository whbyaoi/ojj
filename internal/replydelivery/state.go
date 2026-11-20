// Package replydelivery carries host-only QQ delivery evidence for one input.
// It never infers delivery from model messages or tool result text.
package replydelivery

import (
	"context"
	"sync/atomic"
)

type State string

const (
	Untracked State = ""
	NotSent   State = "not_sent"
	Unknown   State = "unknown"
	Sent      State = "sent"
)

type key struct{}
type tracker struct{ flags atomic.Uint32 }

// WithTracking is installed only by a host that observes every QQ send boundary.
// A fresh tracker belongs to exactly one input, including all its tool rounds.
func WithTracking(ctx context.Context) context.Context {
	return context.WithValue(ctx, key{}, &tracker{})
}

// Started must run before entering the transport, so panic and cancellation leave
// an unknown outcome. Evidence is monotonic across multiple sends in one input.
func Started(ctx context.Context) {
	if t, _ := ctx.Value(key{}).(*tracker); t != nil {
		t.flags.Or(1)
	}
}

// Confirmed runs immediately after a platform ACK, before any state persistence.
func Confirmed(ctx context.Context) {
	if t, _ := ctx.Value(key{}).(*tracker); t != nil {
		t.flags.Or(2)
	}
}

// Outcome returns Untracked for old or unsupported host paths. Any confirmed or
// uncertain send prevents treating an aggregate input as definitely not sent.
func Outcome(ctx context.Context) State {
	if ctx == nil {
		return Untracked
	}
	t, _ := ctx.Value(key{}).(*tracker)
	if t == nil {
		return Untracked
	}
	flags := t.flags.Load()
	if flags&2 != 0 {
		return Sent
	}
	if flags&1 != 0 {
		return Unknown
	}
	return NotSent
}
