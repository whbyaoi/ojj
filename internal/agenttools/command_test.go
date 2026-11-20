//go:build linux || darwin

package agenttools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ojj/internal/minipi"
)

func runCommand(t *testing.T, tool minipi.Tool, ctx context.Context, raw string) (commandOutput, error) {
	t.Helper()
	result, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(raw)}, nil)
	if result.Terminate || result.IsError != (err != nil) {
		t.Fatalf("unexpected flags: %+v, %v", result, err)
	}
	var out commandOutput
	if e := json.Unmarshal([]byte(result.Content), &out); e != nil {
		t.Fatalf("decode output: %v; result=%+v", e, result)
	}
	return out, err
}

func TestCommandLogsOmitPayloads(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	tool, err := NewCommand(CommandConfig{Dir: t.TempDir(), Env: []string{"PRIVATE=private-config"}})
	if err != nil {
		t.Fatal("tool construction failed")
	}
	for _, raw := range []string{`{"command":"printf private-output; exit 7"}`, `{"command":"printf private-output","cwd":"private-missing-directory"}`} {
		buf.Reset()
		_, err := tool.Execute(context.Background(), minipi.ToolCall{ID: "private-call", Name: "private-tool", Arguments: json.RawMessage(raw)}, nil)
		if err == nil {
			t.Fatal("expected execution failure")
		}
		if strings.Contains(buf.String(), "private-") {
			t.Fatal("sensitive command data logged")
		}
		var record map[string]any
		if json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record) != nil {
			t.Fatal("invalid command log")
		}
		if record["level"] != "ERROR" || record["tool"] != "command_exec" || record["stage"] != "execute" {
			t.Fatal("incorrect execution classification")
		}
	}
}

func TestCommandExecution(t *testing.T) {
	t.Setenv("OJJ_TEST_SECRET", "must-not-be-inherited")
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	tool, err := NewCommand(CommandConfig{Dir: dir, MaxOutputBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, raw, output string
		code              int
		truncated         bool
	}{
		{"pipeline", `{"command":"printf hello | tr a-z A-Z; printf err >&2"}`, "HELLOerr", 0, false},
		{"exit", `{"command":"printf failed; exit 7"}`, "failed", 7, false},
		{"limit", `{"command":"printf 12345678901234567890"}`, "1234567890123456", 0, true},
		{"secret", `{"command":"printf '%s' \"${OJJ_TEST_SECRET-unset}\""}`, "unset", 0, false},
		{"cwd", `{"command":"printf '%s' \"${PWD##*/}\"","cwd":"child"}`, "child", 0, false},
		{"binary", `{"command":"printf '\\377ok'"}`, "ok", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runCommand(t, tool, context.Background(), tc.raw)
			if out.Output != tc.output || out.ExitCode != tc.code || out.Truncated != tc.truncated || out.TimedOut || (err != nil) != (tc.code != 0) {
				t.Fatalf("output=%+v err=%v", out, err)
			}
		})
	}
	out, err := runCommand(t, tool, context.Background(), `{"command":"pwd","cwd":"does-not-exist"}`)
	if err == nil || out.ExitCode != -1 {
		t.Fatalf("bad directory: %+v %v", out, err)
	}
	env := []string{"EXPLICIT=original"}
	tool, err = NewCommand(CommandConfig{Env: env})
	if err != nil {
		t.Fatal(err)
	}
	env[0] = "EXPLICIT=mutated"
	out, err = runCommand(t, tool, context.Background(), `{"command":"printf '%s' \"$EXPLICIT\""}`)
	if err != nil || out.Output != "original" {
		t.Fatalf("environment snapshot: %+v %v", out, err)
	}
}

func TestCommandValidation(t *testing.T) {
	tool, err := NewCommand(CommandConfig{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		``, `null`, `[]`, `{}`, `{"command":" "}`, `{"command":1}`,
		`{"command":"true","other":1}`, `{"COMMAND":"true"}`,
		`{"command":"true","COMMAND":"false"}`, `{"command":"true","command":"false"}`,
		`{"command":"true","cwd":null}`, `{"command":"true"} {}`,
		`{"command":"true"} garbage`, `{"command":"true",}`, `{"command":`,
		`{"command":"true","timeout_seconds":0}`, `{"command":"true","timeout_seconds":2}`,
		`{"command":"true","timeout_seconds":1.5}`, `{"command":"a\u0000b"}`,
		`{"command":"true","cwd":"a\u0000b"}`, "{\"command\":\"\xff\"}",
	} {
		if err := tool.Validate(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %q", raw)
		}
		if _, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(raw)}, nil); err == nil {
			t.Errorf("execute accepted %q", raw)
		}
	}
	for _, cfg := range []CommandConfig{
		{Shell: "sh"}, {Shell: "/no/such/shell"}, {Shell: t.TempDir()},
		{Timeout: -1}, {Timeout: 25 * time.Hour}, {MaxOutputBytes: -1},
		{MaxOutputBytes: 17 << 20}, {Dir: "/no/such/directory"}, {Env: []string{"BAD"}},
		{Env: []string{"=value"}}, {Env: []string{"A=\x00"}},
	} {
		if _, err := NewCommand(cfg); err == nil {
			t.Errorf("accepted config %+v", cfg)
		}
	}
}

func TestCommandErrorClassification(t *testing.T) {
	failure := errors.New("process failed")
	for _, err := range []error{nil, context.DeadlineExceeded, errors.Join(context.DeadlineExceeded), failure} {
		got, timedOut := commandError(err, context.DeadlineExceeded, nil)
		if got == nil || !timedOut || errors.Is(got, context.DeadlineExceeded) || errors.Is(got, context.Canceled) {
			t.Fatalf("local deadline: %v, %v", got, timedOut)
		}
		if err == failure && !errors.Is(got, failure) {
			t.Fatal("lost process failure")
		}
	}
	got, timedOut := commandError(failure, context.DeadlineExceeded, context.DeadlineExceeded)
	if timedOut || !errors.Is(got, failure) || !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("parent deadline: %v, %v", got, timedOut)
	}
}

func TestCommandCancellationAndTimeout(t *testing.T) {
	tool, err := NewCommand(CommandConfig{Dir: t.TempDir(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	out, err := runCommand(t, tool, context.Background(), `{"command":"sleep 30 & wait","timeout_seconds":1}`)
	if err == nil || !out.TimedOut || errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 4*time.Second {
		t.Fatalf("local timeout: %+v %v", out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`{"command":"true"}`)}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancel: %v", err)
	}
	dir := t.TempDir()
	tool, err = NewCommand(CommandConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the shell to start instead of assuming startup finishes in 200ms.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	var result minipi.ToolResult
	go func() {
		result, err = tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`{"command":"(sleep 1; printf survived > marker) & printf ready; printf started > started; wait"}`)}, nil)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "started")); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("shell did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if decodeErr := json.Unmarshal([]byte(result.Content), &out); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !errors.Is(err, context.Canceled) || out.TimedOut || out.Output != "ready" {
		t.Fatalf("parent cancellation: %+v %v", out, err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, "marker")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant survived cancellation: %v", err)
	}
	start = time.Now()
	out, err = runCommand(t, tool, context.Background(), `{"command":"sleep 30 & printf done"}`)
	if err == nil || !strings.Contains(err.Error(), "WaitDelay") || out.Output != "done" || time.Since(start) > 3*time.Second {
		t.Fatalf("background pipe: %+v %v", out, err)
	}
}
