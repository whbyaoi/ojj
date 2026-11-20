package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"ojj/internal/im/qq"
	"ojj/internal/minipi"
)

const testPortrait = "测试人格：小鲸鱼，短句聊天，不越权。"
const testSystemPrompt = mainSystemPrompt + "\n\n## 人格设定（服从宿主规则）\n" + testPortrait

func brainTestEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"OJJ_MODEL_PROTOCOL", "OPENAI_TOKEN_LIMIT_FIELD", "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "ANTHROPIC_BASE_URL", "OPENAI_BASE_URL", "OJJ_BRAIN_DB", "OJJ_BRAIN_MAX_INPUT_BYTES", "OJJ_BRAIN_MAX_CONTEXT_BYTES", "OJJ_BRAIN_MAX_HISTORY_MESSAGES", "OJJ_BRAIN_MAX_STORED_BYTES", "OJJ_QQ_SEND_ALLOWLIST", "OJJ_ENABLE_COMMAND", "OJJ_COMMAND_DIR", "OJJ_MESSAGE_DB", "OJJ_MESSAGE_QUIET_PERIOD", "OJJ_MESSAGE_MAX_WINDOW", "OJJ_HEARTBEAT_INTERVAL",
		"OJJ_ENABLE_MEM0", "OJJ_MEM0_BASE_URL", "OJJ_MEM0_API_KEY", "OJJ_MEM0_TIMEOUT",
		"OJJ_REPLY_DB", "OJJ_REPLY_BOT_USER_ID", "OJJ_REPLY_ALIASES", "OJJ_REPLY_PROACTIVE",
		"OJJ_REPLY_ACTIVE_INTERVAL", "OJJ_REPLY_ACTIVE_WINDOW", "OJJ_REPLY_ACTIVE_LIMIT",
		"OJJ_REPLY_SESSION_TTL", "OJJ_REPLY_TOTAL_WINDOW", "OJJ_REPLY_TOTAL_LIMIT"} {
		t.Setenv(key, "")
	}
	portraitPath := filepath.Join(t.TempDir(), "portrait.txt")
	if err := os.WriteFile(portraitPath, []byte(testPortrait), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OJJ_PORTRAIT_FILE", portraitPath)
	t.Setenv("OPENAI_API_KEY", "fake-test-key")
	t.Setenv("OPENAI_MODEL", "test-model")
	t.Setenv("QQ_APP_ID", "test")
	t.Setenv("QQ_CLIENT_SECRET", "test")
}

func TestRunConfigurationLogsSafeStage(t *testing.T) {
	const private = "synthetic-private-config"
	for _, tc := range []struct{ key, value, stage string }{
		{"QQ_APP_ID", "", "qq_config"},
		{"OJJ_HEARTBEAT_INTERVAL", private, "heartbeat_config"},
		{"OJJ_MESSAGE_QUIET_PERIOD", private, "messagebuilder_config"},
		{"OPENAI_BASE_URL", "ftp://" + private, "brain_config"},
		{"OJJ_PORTRAIT_FILE", filepath.Join(t.TempDir(), private), "brain_config"},
		{"OJJ_REPLY_PROACTIVE", private, "reply_config"},
		{"OJJ_REPLY_ACTIVE_INTERVAL", private, "reply_config"},
		{"OJJ_REPLY_ACTIVE_WINDOW", "1s", "reply_config"},
		{"OJJ_REPLY_SESSION_TTL", "0s", "reply_config"},
		{"OJJ_REPLY_TOTAL_WINDOW", "-1s", "reply_config"},
		{"OJJ_REPLY_ACTIVE_LIMIT", private, "reply_config"},
		{"OJJ_REPLY_TOTAL_LIMIT", "0", "reply_config"},
		{"OJJ_REPLY_ALIASES", private + ",", "reply_config"},
		{"OJJ_REPLY_BOT_USER_ID", " " + private, "reply_config"},
		{"OJJ_REPLY_DB", "file:" + private, "reply_config"},
		{"OJJ_REPLY_DB", ":memory:", "reply_config"},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			brainTestEnv(t)
			dir := t.TempDir()
			t.Setenv("OJJ_EVENT_DB", filepath.Join(dir, private+".db"))
			t.Setenv("QQ_CLIENT_SECRET", private+"-qq-secret")
			t.Setenv("OPENAI_API_KEY", private+"-api-key")
			t.Setenv(tc.key, tc.value)
			var buf bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(old)
			if err := Run(context.Background()); err == nil {
				t.Fatal("accepted invalid configuration")
			} else if strings.Contains(err.Error(), private) || strings.Contains(err.Error(), dir) {
				t.Fatal("configuration leaked into returned error")
			}
			if strings.Contains(buf.String(), private) || strings.Contains(buf.String(), dir) {
				t.Fatal("configuration leaked into logs")
			}
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
				t.Fatal("expected exactly one structured failure log")
			}
			if record["component"] != "app" || record["stage"] != tc.stage || record["level"] != "ERROR" || record["failure"] != "failure" || record["duration"] == nil {
				t.Fatal("missing startup failure diagnostics")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("opened storage before validation")
			}
		})
	}
}

func TestMainBrainConfig(t *testing.T) {
	brainTestEnv(t)
	cfg, allowed, err := mainBrainConfig("events.db")
	if err != nil || cfg.Path != "events.db.brain.main.db" || cfg.ID != "main" || len(allowed) != 0 || len(cfg.Agent.Tools) != 0 || cfg.MaxInputBytes != 16<<20 || cfg.MaxContextBytes != 64<<20 || cfg.MaxHistoryMessages != 1000 {
		t.Fatalf("defaults: %+v %v %v", cfg, allowed, err)
	}
	if cfg.SystemPrompt != testSystemPrompt {
		t.Fatal("missing host rules or portrait")
	}
	t.Setenv("OPENAI_BASE_URL", "http://example.test:8080/v1")
	t.Setenv("OJJ_ENABLE_COMMAND", "true")
	t.Setenv("OJJ_COMMAND_DIR", t.TempDir())
	t.Setenv("OJJ_QQ_SEND_ALLOWLIST", "private:user_1, group:group-2")
	t.Setenv("OJJ_BRAIN_MAX_HISTORY_MESSAGES", "25")
	cfg, allowed, err = mainBrainConfig("events.db")
	if err != nil || len(cfg.Agent.Tools) != 1 || cfg.Agent.Tools[0].Definition.Name != "command_exec" || len(allowed) != 2 || cfg.MaxHistoryMessages != 25 {
		t.Fatalf("overrides: %+v %v %v", cfg, allowed, err)
	}
}

func TestMem0StartupConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"OJJ_ENABLE_MEM0", "invalid-private-value"},
		{"OJJ_MEM0_BASE_URL", ""},
		{"OJJ_MEM0_BASE_URL", "ftp://invalid-private-value"},
		{"OJJ_MEM0_API_KEY", ""},
		{"OJJ_MEM0_API_KEY", "invalid-private-value\n"},
		{"OJJ_MEM0_TIMEOUT", "invalid-private-value"},
		{"OJJ_MEM0_TIMEOUT", "0s"},
		{"OJJ_MEM0_TIMEOUT", "-1s"},
		{"OJJ_MEM0_TIMEOUT", "1ns"},
		{"OJJ_MEM0_TIMEOUT", "6m"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			brainTestEnv(t)
			t.Setenv("OJJ_ENABLE_MEM0", "true")
			t.Setenv("OJJ_MEM0_BASE_URL", "http://example.invalid")
			t.Setenv("OJJ_MEM0_API_KEY", "synthetic-key")
			t.Setenv(tc.key, tc.value)
			dir := t.TempDir()
			t.Setenv("OJJ_EVENT_DB", filepath.Join(dir, "events.db"))
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(old)
			if err := Run(context.Background()); err == nil || strings.Contains(err.Error(), "invalid-private-value") || strings.Contains(err.Error(), "synthetic-key") {
				t.Fatal("invalid mem0 configuration accepted or leaked")
			}
			if !strings.Contains(logs.String(), `"stage":"brain_config"`) || strings.Contains(logs.String(), "invalid-private-value") || strings.Contains(logs.String(), "synthetic-key") || strings.Contains(logs.String(), "example.invalid") {
				t.Fatal("missing safe startup diagnostics")
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatal("opened storage before validation")
			}
		})
	}
	brainTestEnv(t)
	t.Setenv("OJJ_MEM0_BASE_URL", "invalid-private-value")
	t.Setenv("OJJ_MEM0_API_KEY", "invalid-private-value\n")
	t.Setenv("OJJ_MEM0_TIMEOUT", "invalid-private-value")
	cfg, _, err := mainBrainConfig("unused.db")
	if err != nil || len(cfg.Agent.Tools) != 0 {
		t.Fatal("disabled mem0 should ignore its settings")
	}
}

func TestMem0ConfiguredAgentCallsOnDemand(t *testing.T) {
	brainTestEnv(t)
	var memoryRequests, modelRequests atomic.Int32
	memoryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		memoryRequests.Add(1)
		if r.Method != http.MethodPost || r.Header.Get("X-API-Key") != "synthetic-key" {
			t.Error("incorrect memory request")
		}
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error("invalid memory request body")
			w.WriteHeader(400)
			return
		}
		switch r.URL.Path {
		case "/memories":
			if string(body) != `{"messages":[{"role":"user","content":"likes tea"}],"agent_id":"ojj"}` {
				t.Error("incorrect fixed add scope")
			}
			fmt.Fprint(w, `{"results":[{"id":"saved-1","event":"ADD"}]}`)
		case "/search":
			if string(body) != `{"query":"tea","filters":{"agent_id":"ojj"}}` {
				t.Error("incorrect fixed search scope")
			}
			fmt.Fprint(w, `{"results":[{"id":"saved-1","memory":"likes tea","agent_id":"ojj"}]}`)
		default:
			t.Error("unexpected memory path")
			w.WriteHeader(404)
		}
	}))
	defer memoryServer.Close()
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tools    []struct{ Function struct{ Name string } }
			Messages []struct{ Role, Content, ToolCallID string }
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if len(body.Tools) != 2 || body.Tools[0].Function.Name != "mem0_add_memory" || body.Tools[1].Function.Name != "mem0_search_memory" {
			t.Error("missing memory tools in model request")
		}
		w.Header().Set("Content-Type", "application/json")
		switch modelRequests.Add(1) {
		case 1:
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"add-1","type":"function","function":{"name":"mem0_add_memory","arguments":"{\"messages\":[{\"role\":\"user\",\"content\":\"likes tea\"}]}"}},{"id":"search-1","type":"function","function":{"name":"mem0_search_memory","arguments":"{\"query\":\"tea\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`)
		default:
			if modelRequests.Load() == 2 && (len(body.Messages) != 5 || body.Messages[3].Role != "tool" || !strings.Contains(body.Messages[3].Content, "saved-1") || body.Messages[4].Role != "tool" || !strings.Contains(body.Messages[4].Content, "likes tea")) {
				t.Error("missing memory results in tool round")
			}
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":2}}`)
		}
	}))
	defer modelServer.Close()
	t.Setenv("OJJ_ENABLE_MEM0", "true")
	t.Setenv("OJJ_MEM0_BASE_URL", memoryServer.URL)
	t.Setenv("OJJ_MEM0_API_KEY", "synthetic-key")
	t.Setenv("OPENAI_BASE_URL", modelServer.URL+"/v1")
	cfg, _, err := mainBrainConfig("unused.db")
	if err != nil || memoryRequests.Load() != 0 {
		t.Fatal("configuration failed or accessed memory at startup")
	}
	agent, err := minipi.New(cfg.Agent)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err = agent.Run(context.Background(), minipi.Context{SystemPrompt: cfg.SystemPrompt}, []minipi.Message{{Role: minipi.RoleUser, Content: "synthetic request"}}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if memoryRequests.Load() != 2 || modelRequests.Load() != 3 {
		t.Fatal("memory should only be accessed for explicit tool calls")
	}
}

func TestPortraitStartupLoading(t *testing.T) {
	brainTestEnv(t)
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("OJJ_PORTRAIT_FILE", "")
	if prompt, err := loadMainSystemPrompt(); err == nil || prompt != "" {
		t.Fatal("missing default portrait accepted")
	}
	if err := os.WriteFile("portrait.md", []byte("\uFEFF \n"+testPortrait+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err := mainBrainConfig("unused.db")
	if err != nil || cfg.SystemPrompt != testSystemPrompt {
		t.Fatal("default portrait or BOM normalization failed", err)
	}
	if err := os.WriteFile("portrait.md", []byte("重启后的人格"), 0600); err != nil {
		t.Fatal(err)
	}
	if cfg.SystemPrompt != testSystemPrompt {
		t.Fatal("startup configuration changed without reload")
	}
	reloaded, _, err := mainBrainConfig("unused.db")
	if err != nil || !strings.HasSuffix(reloaded.SystemPrompt, "\n重启后的人格") {
		t.Fatal("startup did not reload changed portrait", err)
	}
	t.Setenv("OJJ_PORTRAIT_FILE", "custom.txt")
	if err := os.WriteFile("custom.txt", []byte(testPortrait), 0600); err != nil {
		t.Fatal(err)
	}
	if prompt, err := loadMainSystemPrompt(); err != nil || prompt != testSystemPrompt {
		t.Fatal("relative custom portrait failed", err)
	}
}

func TestPortraitValidationBeforeStorage(t *testing.T) {
	for _, tc := range []struct {
		name, content, wantError string
	}{
		{"empty", "", "人格文件不能为空"},
		{"whitespace", "\uFEFF \t\r\n", "人格文件不能为空"},
		{"invalid_utf8", "synthetic-private-body\xff", "人格文件必须是有效 UTF-8 文本"},
		{"size_limit", strings.Repeat("x", maxPortraitBytes), ""},
		{"oversized", strings.Repeat("x", maxPortraitBytes+1), "人格文件不能超过 64 KiB"},
		{"missing", "", "人格文件不可读取或不是普通文件，请检查 OJJ_PORTRAIT_FILE（默认工作目录 portrait.md）"},
		{"directory", "", "人格文件不可读取或不是普通文件，请检查 OJJ_PORTRAIT_FILE（默认工作目录 portrait.md）"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brainTestEnv(t)
			path := filepath.Join(t.TempDir(), "synthetic-private-path")
			if tc.name == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if tc.name != "missing" {
				if err := os.WriteFile(path, []byte(tc.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("OJJ_PORTRAIT_FILE", path)
			prompt, err := loadMainSystemPrompt()
			if tc.wantError == "" {
				if err != nil || prompt != mainSystemPrompt+"\n\n## 人格设定（服从宿主规则）\n"+tc.content {
					t.Fatal("valid portrait rejected or truncated", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantError || prompt != "" {
				t.Fatal("incorrect or unsafe portrait validation", err)
			}
			dbDir := t.TempDir()
			t.Setenv("OJJ_EVENT_DB", filepath.Join(dbDir, "events.db"))
			if err := Run(context.Background()); err == nil || strings.Contains(err.Error(), "synthetic-private") || strings.Contains(err.Error(), path) {
				t.Fatal("invalid portrait accepted or private data leaked")
			}
			entries, err := os.ReadDir(dbDir)
			if err != nil || len(entries) != 0 {
				t.Fatal("opened storage before portrait validation")
			}
		})
	}
}

func TestModelProtocolConfiguration(t *testing.T) {
	for _, protocol := range []string{"", "openai", "anthropic"} {
		t.Run(protocol, func(t *testing.T) {
			brainTestEnv(t)
			t.Setenv("OJJ_MODEL_PROTOCOL", protocol)
			prefix, other := "OPENAI_", "ANTHROPIC_"
			if protocol == "anthropic" {
				prefix, other = other, prefix
			}
			t.Setenv(prefix+"API_KEY", "fake-key")
			t.Setenv(prefix+"MODEL", "test-model")
			t.Setenv(other+"BASE_URL", "invalid-private-url")
			if _, _, err := mainBrainConfig("unused.db"); err != nil {
				t.Fatal(err)
			}
			t.Setenv(other+"BASE_URL", "")
			t.Setenv(other+"API_KEY", "other-fake-key")
			t.Setenv(other+"MODEL", "other-model")
			for _, key := range []string{"API_KEY", "MODEL"} {
				t.Setenv(prefix+key, "")
				if _, _, err := mainBrainConfig("unused.db"); err == nil {
					t.Fatal("cross-protocol credential fallback")
				}
				t.Setenv(prefix+key, "fake-value")
			}
		})
	}
	brainTestEnv(t)
	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		t.Setenv("OPENAI_TOKEN_LIMIT_FIELD", field)
		if _, _, err := mainBrainConfig("unused.db"); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("OJJ_MODEL_PROTOCOL", "private-protocol")
	if _, _, err := mainBrainConfig("unused.db"); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("unsafe protocol validation")
	}
}

func TestInvalidBrainConfigDoesNotOpenDatabases(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"OPENAI_API_KEY", ""}, {"OPENAI_MODEL", ""},
		{"OPENAI_BASE_URL", "ftp://example.com"},
		{"OJJ_MODEL_PROTOCOL", "private-unknown-protocol"},
		{"OPENAI_TOKEN_LIMIT_FIELD", "private-unknown-field"},
		{"OJJ_MODEL_PROTOCOL", "anthropic"},
		{"OJJ_QQ_SEND_ALLOWLIST", "*"}, {"OJJ_QQ_SEND_ALLOWLIST", "private:*"},
		{"OJJ_QQ_SEND_ALLOWLIST", "group:ok,"}, {"OJJ_QQ_SEND_ALLOWLIST", "channel:abc"},
		{"OJJ_ENABLE_COMMAND", "yes"}, {"OJJ_BRAIN_MAX_INPUT_BYTES", "-1"},
		{"OJJ_BRAIN_MAX_CONTEXT_BYTES", "0"}, {"OJJ_BRAIN_MAX_STORED_BYTES", "bad"},
		{"OJJ_BRAIN_MAX_HISTORY_MESSAGES", "0"}, {"OJJ_BRAIN_MAX_HISTORY_MESSAGES", "-1"},
		{"OJJ_BRAIN_MAX_HISTORY_MESSAGES", "bad"}, {"OJJ_BRAIN_MAX_HISTORY_MESSAGES", "99999999999999999999999999999"},
	} {
		t.Run(test.key+test.value, func(t *testing.T) {
			brainTestEnv(t)
			dir := t.TempDir()
			t.Setenv("OJJ_EVENT_DB", filepath.Join(dir, "events.db"))
			t.Setenv(test.key, test.value)
			if err := Run(context.Background()); err == nil {
				t.Fatal("accepted invalid configuration")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("opened storage before validation: %v %v", entries, err)
			}
		})
	}
}

type brainTestSender struct{ calls atomic.Int32 }

func (s *brainTestSender) SendText(_ context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
	if msg.TargetType != "private" || msg.TargetID != "user_1" || msg.MsgID != "original" || msg.MsgSeq != 1 {
		return qq.SentMessage{}, errors.New("unexpected QQ message")
	}
	s.calls.Add(1)
	return qq.SentMessage{ID: "sent-1"}, nil
}

func TestBrainQQEmptyAllowlist(t *testing.T) {
	for _, raw := range []string{"", " \t "} {
		t.Run("empty="+raw, func(t *testing.T) {
			brainTestEnv(t)
			t.Setenv("OJJ_QQ_SEND_ALLOWLIST", raw)
			cfg, allowed, err := mainBrainConfig("unused.db")
			if err != nil || len(allowed) != 0 {
				t.Fatalf("empty config: %v", err)
			}
			var calls int
			sender := checkedQQSender(func(_ context.Context, msg qq.TextMessage) (qq.SentMessage, error) {
				calls++
				return qq.SentMessage{ID: "sent"}, nil
			})
			if err := attachQQTool(&cfg, sender, allowed); err != nil {
				t.Fatal(err)
			}
			// Mutating the source map must not change the attached policy.
			allowed["private:only_this"] = struct{}{}
			tool := cfg.Agent.Tools[0]
			for _, target := range []string{"private", "group"} {
				for _, id := range []string{"user_1", "other"} {
					call := minipi.ToolCall{ID: "send", Name: "qq_send_text", Arguments: []byte(`{"target_type":"` + target + `","target_id":"` + id + `","content":"hello"}`)}
					if _, err := tool.Execute(context.Background(), call, nil); err != nil {
						t.Fatalf("unrestricted %s send: %v", target, err)
					}
				}
			}
			if calls != 4 {
				t.Fatalf("got %d sends, want 4", calls)
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := (authorizedQQSender{sender: sender}).SendText(canceled, qq.TextMessage{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("empty policy ignored cancellation: %v", err)
			}
			if calls != 4 {
				t.Fatal("canceled request reached sender")
			}
		})
	}
}

func TestBrainQQAuthorization(t *testing.T) {
	brainTestEnv(t)
	cfg, _, err := mainBrainConfig("unused.db")
	if err != nil {
		t.Fatal(err)
	}
	sender := &brainTestSender{}
	allowed := map[string]struct{}{"private:user_1": {}}
	if err := attachQQTool(&cfg, sender, allowed); err != nil {
		t.Fatal(err)
	}
	allowed["group:user_1"] = struct{}{}
	tool := cfg.Agent.Tools[0]
	for _, target := range []string{"group", "private"} {
		call := minipi.ToolCall{ID: "send", Name: "qq_send_text", Arguments: []byte(`{"target_type":"` + target + `","target_id":"user_1","content":"hello","msg_id":"original"}`)}
		_, err := tool.Execute(context.Background(), call, nil)
		if (target == "private") != (err == nil) {
			t.Fatalf("policy copy or allowed send failed: %s %v", target, err)
		}
	}
	if sender.calls.Load() != 1 {
		t.Fatal("expected exactly one authorized send")
	}
	sender.calls.Store(0)
	var absent *brainTestSender
	if err := attachQQTool(&cfg, absent, allowed); err == nil {
		t.Fatal("typed nil sender accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (authorizedQQSender{sender: sender, allowed: allowed}).SendText(canceled, qq.TextMessage{TargetType: "private", TargetID: "user_1"}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled send accepted", err)
	}
	guard := authorizedQQSender{sender: sender, allowed: map[string]struct{}{"private:user_1": {}}}
	for _, msg := range []qq.TextMessage{{TargetType: "group", TargetID: "user_1"}, {TargetType: "private", TargetID: "other"}} {
		if _, err := guard.SendText(context.Background(), msg); err == nil {
			t.Fatal("unauthorized send accepted")
		}
	}
	if sender.calls.Load() != 0 {
		t.Fatal("unauthorized request reached sender")
	}
}
