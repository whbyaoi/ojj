//go:build !linux && !darwin

package agenttools

import "os/exec"

func commandSupported() bool { return false }

func prepareCommand(_ *exec.Cmd) func() { return func() {} }
