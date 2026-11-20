// Package cancelerr classifies cancellation errors without hiding joined failures.
package cancelerr

import "errors"

// Only reports whether err has at least one non-nil leaf and every leaf matches
// target. Wrappers and joined errors are inspected before matching, so a matching
// branch (or a wrapper's Is method) cannot hide a different underlying failure.
// A nil err or target never matches.
func Only(err, target error) bool {
	if err == nil || target == nil {
		return false
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		found := false
		for _, child := range wrapped.Unwrap() {
			if child == nil {
				continue
			}
			if !Only(child, target) {
				return false
			}
			found = true
		}
		return found
	case interface{ Unwrap() error }:
		if child := wrapped.Unwrap(); child != nil {
			return Only(child, target)
		}
	}
	return errors.Is(err, target)
}
