package agenttools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ojj/internal/cancelerr"
	"ojj/internal/minipi"
)

// CommandConfig is trusted host configuration, never supplied by the model.
// This tool is NOT a sandbox: commands have the host process's filesystem and
// network permissions. Dir is a starting directory, not a confinement boundary.
// Use a dedicated unprivileged container/account for untrusted agents.
type CommandConfig struct {
	Dir            string
	Shell          string
	Timeout        time.Duration
	MaxOutputBytes int
	// Env replaces the entire environment. Nil uses only a minimal PATH, not
	// os.Environ(), to avoid automatically forwarding application credentials.
	// Shell commands can still read any secrets accessible to the OS account.
	Env []string
}

type commandArguments struct {
	Command        string `json:"command"`
	Cwd            string `json:"cwd,omitempty"`
	TimeoutSeconds *int   `json:"timeout_seconds,omitempty"`
}

type commandOutput struct {
	Output    string `json:"output"`
	ExitCode  int    `json:"exit_code"`
	Truncated bool   `json:"truncated"`
	TimedOut  bool   `json:"timed_out"`
}

// NewCommand returns command_exec, executing a non-interactive POSIX shell -c.
// Defaults: /bin/sh, current directory, 60s, 64 KiB combined stdout/stderr.
// Calls do not retain shell state. Output is a bounded prefix; excess is drained
// and discarded. Binary output is converted to valid UTF-8 in the JSON result.
// Parent cancellation aborts the agent; a tool-local timeout is an ordinary tool
// error that the model can handle. No streaming updates are emitted.
func NewCommand(cfg CommandConfig) (minipi.Tool, error) {
	if !commandSupported() {
		return minipi.Tool{}, errors.New("command tool supports Linux and macOS only")
	}
	if cfg.Shell == "" {
		cfg.Shell = "/bin/sh"
	}
	if !filepath.IsAbs(cfg.Shell) {
		return minipi.Tool{}, errors.New("command shell must be an absolute path")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	if cfg.MaxOutputBytes == 0 {
		cfg.MaxOutputBytes = 64 << 10
	}
	if cfg.Timeout < time.Second || cfg.Timeout > 24*time.Hour || cfg.MaxOutputBytes < 1 || cfg.MaxOutputBytes > 16<<20 {
		return minipi.Tool{}, errors.New("invalid command timeout or output limit")
	}
	if cfg.Dir == "" {
		cfg.Dir = "."
	}
	var err error
	cfg.Dir, err = filepath.Abs(cfg.Dir)
	if err != nil {
		return minipi.Tool{}, err
	}
	if info, err := os.Stat(cfg.Dir); err != nil || !info.IsDir() {
		return minipi.Tool{}, errors.New("command directory must exist")
	}
	if info, err := os.Stat(cfg.Shell); err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		return minipi.Tool{}, errors.New("command shell must be executable")
	}
	if cfg.Env == nil {
		cfg.Env = []string{"PATH=/usr/bin:/bin"}
	} else {
		cfg.Env = append([]string{}, cfg.Env...)
	}
	for _, entry := range cfg.Env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" || strings.ContainsRune(entry, 0) {
			return minipi.Tool{}, errors.New("invalid command environment")
		}
	}
	parse := func(raw json.RawMessage) (commandArguments, error) {
		var args commandArguments
		if err := decodeArguments(raw, &args); err != nil {
			return args, err
		}
		if strings.TrimSpace(args.Command) == "" || len(args.Command) > 64<<10 || strings.ContainsRune(args.Command, 0) {
			return args, errors.New("command must be nonempty, without NUL, at most 64 KiB")
		}
		if len(args.Cwd) > 4096 || strings.ContainsRune(args.Cwd, 0) {
			return args, errors.New("invalid cwd")
		}
		if args.TimeoutSeconds != nil && (*args.TimeoutSeconds < 1 || *args.TimeoutSeconds > int(cfg.Timeout/time.Second)) {
			return args, errors.New("timeout_seconds exceeds configured bounds")
		}
		return args, nil
	}
	parameters := fmt.Sprintf(`{"type":"object","additionalProperties":false,"required":["command"],"properties":{"command":{"type":"string","minLength":1,"maxLength":65536},"cwd":{"type":"string","maxLength":4096,"description":"Absolute directory or path relative to configured starting directory; not a sandbox"},"timeout_seconds":{"type":"integer","minimum":1,"maximum":%d}}}`, int(cfg.Timeout/time.Second))
	return minipi.Tool{
		Definition: minipi.ToolDefinition{Name: "command_exec", Description: "在宿主机执行非交互 shell 命令（不是沙箱）。支持管道、重定向；默认不继承应用环境变量。返回合并输出、退出码与截断标志。命令副作用不可回滚，禁止启动后台守护进程。", Parameters: json.RawMessage(parameters)},
		Validate:   func(raw json.RawMessage) error { _, err := parse(raw); return err },
		Execute: func(ctx context.Context, call minipi.ToolCall, _ func(minipi.ToolResult)) (out minipi.ToolResult, err error) {
			started := time.Now()
			stage := "validate"
			stats := commandOutput{ExitCode: -1}
			defer func() {
				minipi.LogOperation(ctx, "agenttools", stage, started, err, slog.LevelInfo,
					slog.String("tool", "command_exec"), slog.Int("exit_code", stats.ExitCode), slog.Bool("truncated", stats.Truncated), slog.Bool("timed_out", stats.TimedOut))
			}()
			args, err := parse(call.Arguments)
			if err != nil {
				return minipi.ToolResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return minipi.ToolResult{}, err
			}
			duration := cfg.Timeout
			if args.TimeoutSeconds != nil {
				duration = time.Duration(*args.TimeoutSeconds) * time.Second
			}
			runCtx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			cmd := exec.CommandContext(runCtx, cfg.Shell, "-c", args.Command)
			cmd.Dir = cfg.Dir
			if args.Cwd != "" {
				if filepath.IsAbs(args.Cwd) {
					cmd.Dir = args.Cwd
				} else {
					cmd.Dir = filepath.Join(cfg.Dir, args.Cwd)
				}
			}
			cmd.Env = append([]string{}, cfg.Env...)
			output := &boundedOutput{limit: cfg.MaxOutputBytes}
			cmd.Stdout, cmd.Stderr = output, output
			// Bound pipe draining if a descendant holds descriptors after shell exit.
			cmd.WaitDelay = 200 * time.Millisecond
			cleanup := prepareCommand(cmd)
			defer cleanup()
			stage = "execute"
			err = cmd.Run()
			result := commandOutput{Output: strings.ToValidUTF8(string(output.data), ""), ExitCode: -1, Truncated: output.truncated}
			if cmd.ProcessState != nil {
				result.ExitCode = cmd.ProcessState.ExitCode()
			}
			err, result.TimedOut = commandError(err, runCtx.Err(), ctx.Err())
			stats.ExitCode, stats.Truncated, stats.TimedOut = result.ExitCode, result.Truncated, result.TimedOut
			body, _ := json.Marshal(result)
			return minipi.ToolResult{Content: string(body), IsError: err != nil}, err
		},
	}, nil
}

// exec may return the context error before the process has even started.
// A tool-local deadline must not become an agent-level cancellation.
func commandError(err, localErr, parentErr error) (error, bool) {
	if parentErr != nil {
		return errors.Join(err, parentErr), false
	}
	if localErr == nil {
		return err, false
	}
	if cancelerr.Only(err, localErr) {
		err = nil
	}
	return errors.Join(err, errors.New("command execution timed out")), true
}

type boundedOutput struct {
	mu        sync.Mutex
	data      []byte
	limit     int
	truncated bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	remaining := b.limit - len(b.data)
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	b.data = append(b.data, p...)
	return n, nil
}
