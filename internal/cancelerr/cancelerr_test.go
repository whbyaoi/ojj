package cancelerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type matchingWrapper struct{ err error }

func (w matchingWrapper) Error() string        { return "wrapper" }
func (w matchingWrapper) Unwrap() error        { return w.err }
func (w matchingWrapper) Is(target error) bool { return target == context.Canceled }

type errorGroup []error

func (g errorGroup) Error() string   { return "group" }
func (g errorGroup) Unwrap() []error { return []error(g) }

func TestOnly(t *testing.T) {
	failure := errors.New("rollback failed")
	wrapped := fmt.Errorf("publish: %w", context.Canceled)
	for _, tc := range []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"nil", nil, context.Canceled, false},
		{"nil target", context.Canceled, nil, false},
		{"both nil", nil, nil, false},
		{"canceled", context.Canceled, context.Canceled, true},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded, true},
		{"different cancellation", context.DeadlineExceeded, context.Canceled, false},
		{"wrapped", wrapped, context.Canceled, true},
		{"joined cancellations", errors.Join(context.Canceled, wrapped), context.Canceled, true},
		{"nested cancellations", fmt.Errorf("outer: %w", errors.Join(wrapped, errors.Join(context.Canceled, wrapped))), context.Canceled, true},
		{"rollback failure", errors.Join(context.Canceled, failure), context.Canceled, false},
		{"failure first", errors.Join(failure, wrapped), context.Canceled, false},
		{"nested failure", fmt.Errorf("outer: %w", errors.Join(wrapped, errors.Join(context.Canceled, failure))), context.Canceled, false},
		{"mixed cancellations", errors.Join(context.Canceled, context.DeadlineExceeded), context.Canceled, false},
		{"matching wrapper cannot hide failure", matchingWrapper{failure}, context.Canceled, false},
		{"empty group", errorGroup{}, context.Canceled, false},
		{"nil children", errorGroup{nil, nil}, context.Canceled, false},
		{"non-nil cancellation child", errorGroup{nil, context.Canceled}, context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Only(tc.err, tc.target); got != tc.want {
				t.Fatalf("Only(%v, %v) = %v, want %v", tc.err, tc.target, got, tc.want)
			}
		})
	}
}
