//go:build linux || darwin

package agenttools

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func commandSupported() bool { return true }

// Kill the shell's process group, including ordinary pipeline/background jobs.
// A process that deliberately creates a new session can escape this cleanup;
// strong process isolation requires a container or another OS-level supervisor.
func prepareCommand(cmd *exec.Cmd) func() {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	kill := func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.Cancel = kill
	return func() { _ = kill() }
}
