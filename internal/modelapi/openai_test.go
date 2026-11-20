package modelapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"ojj/internal/minipi"
)

const openAIGood = `{"choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":2}}}`

func TestOpenAIConfigAndRequest(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"", "https://api.openai.com/v1/chat/completions"},
		{"https://example.test", "https://example.test/v1/chat/completions"},
		{"http://localhost:1234/prefix/v1/", "http://localhost:1234/prefix/v1/chat/completions"},
	} {
		c, err := New(Config{Protocol: ProtocolOpenAI, APIKey: "fake", Model: "test", BaseURL: tc.base})
		if err != nil || c.endpoint != tc.want {
			t.Fatalf("endpoint: %v", err)
		}
	}
	for _, cfg := range []Config{
		{Protocol: "secret-protocol"},
		{Protocol: ProtocolOpenAI, OpenAITokenLimitField: "secret-field"},
		{Protocol: ProtocolAnthropic, OpenAITokenLimitField: "max_tokens"},
		{Protocol: ProtocolOpenAI, BaseURL: "https://secret@example.test"},
	} {
		cfg.APIKey, cfg.Model = "fake", "test"
		if _, err := New(cfg); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe configuration validation")
		}
	}
	for _, field := range []string{"", "max_tokens", "max_completion_tokens"} {
		t.Run(field, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/prefix/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer fake" || r.Header.Get("x-api-key") != "" || r.Header.Get("anthropic-version") != "" {
					t.Error("wrong endpoint or headers")
				}
				var body openAIRequest
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("invalid request JSON")
					return
				}
				limit := body.MaxTokens
				if field == "max_completion_tokens" {
					limit = body.MaxCompletionTokens
					if body.MaxTokens != 0 {
						t.Error("both token fields present")
					}
				} else if body.MaxCompletionTokens != 0 {
					t.Error("unexpected completion token field")
				}
				if limit != 17 || body.Model != "test" || body.Stream {
					t.Error("wrong limits or model")
				}
				if len(body.Tools) != 1 || body.Tools[0].Type != "function" || body.Tools[0].Function.Name != "work" || body.Tools[0].Function.Description != "desc" || string(body.Tools[0].Function.Parameters) != `{"type":"object"}` {
					t.Error("bad tool schema")
				}
				if len(body.Messages) != 7 {
					t.Error("lost messages")
					return
				}
				m := body.Messages
				if m[0].Role != "system" || m[0].Content == nil || m[0].Content != "system" || m[1].Role != "user" || m[2].Role != "user" {
					t.Error("lost system or adjacent messages")
				}
				if m[3].Role != "assistant" || m[3].Content != nil || len(m[3].ToolCalls) != 2 || m[3].ToolCalls[0].Function.Arguments != `{"n":1}` {
					t.Error("bad tool calls")
				}
				if m[4].Role != "tool" || m[4].ToolCallID != "b" || m[4].Content == nil || m[4].Content != "Tool execution failed:\nfailed" || m[5].ToolCallID != "a" || m[5].Content == nil || m[5].Content != "" {
					t.Error("lost tool results or error status")
				}
				fmt.Fprint(w, openAIGood)
			}))
			defer server.Close()
			c, err := New(Config{Protocol: ProtocolOpenAI, APIKey: "fake", Model: "test", BaseURL: server.URL + "/prefix/v1/", MaxOutputTokens: 50, OpenAITokenLimitField: field})
			if err != nil {
				t.Fatal(err)
			}
			req := minipi.Request{SystemPrompt: "system", MaxTokens: 17, Tools: []minipi.ToolDefinition{{Name: "work", Description: "desc", Parameters: json.RawMessage(`{"type":"object"}`)}}, Messages: []minipi.Message{
				{Role: minipi.RoleUser, Content: "one"}, {Role: minipi.RoleUser, Content: "two"},
				{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "a", Name: "work", Arguments: json.RawMessage(`{"n":1}`)}, {ID: "b", Name: "work", Arguments: json.RawMessage(`{}`)}}},
				{Role: minipi.RoleTool, ToolCallID: "b", Content: "failed", IsError: true},
				{Role: minipi.RoleTool, ToolCallID: "a"}, {Role: minipi.RoleUser, Content: "next"},
			}}
			got, err := c.Generate(context.Background(), req, func(minipi.Message) { t.Error("stream update") })
			if err != nil || got.Message.Content != "hello" || got.StopReason != minipi.StopEnd || got.Usage.InputTokens != 11 || got.Usage.OutputTokens != 3 || calls.Load() != 1 {
				t.Fatalf("response: %+v %v", got, err)
			}
		})
	}
}

func TestOpenAISafety(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect followed") }))
	defer target.Close()
	for _, status := range []int{301, 302, 307, 308, 400, 401, 429, 500} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Location", target.URL+"/?secret=private")
			w.WriteHeader(status)
			fmt.Fprint(w, "private-response")
		}))
		c, err := New(Config{Protocol: ProtocolOpenAI, APIKey: "private-key", Model: "test", BaseURL: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		calls.Store(0)
		_, err = c.Generate(context.Background(), prompt(), nil)
		var he *HTTPError
		if !errors.As(err, &he) || he.StatusCode != status || calls.Load() != 1 || strings.Contains(err.Error(), "private") {
			t.Fatal("retry, redirect or unsafe HTTP error")
		}
		server.Close()
	}
	c, err := New(Config{Protocol: ProtocolOpenAI, APIKey: "fake", Model: "test", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("private-transport")
	})}})
	if err != nil {
		t.Fatal(err)
	}
	calls.Store(0)
	call := minipi.ToolCall{ID: "a", Name: "work", Arguments: json.RawMessage(`{}`)}
	assistant := minipi.Message{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{call}}
	result := minipi.Message{Role: minipi.RoleTool, ToolCallID: "a"}
	for _, req := range []minipi.Request{
		{}, {MaxTokens: -1, Messages: prompt().Messages},
		{Messages: []minipi.Message{{Role: "system", Content: "bad"}}},
		{Messages: []minipi.Message{{Role: minipi.RoleUser}}},
		{Messages: []minipi.Message{assistant}},
		{Messages: []minipi.Message{result}},
		{Messages: []minipi.Message{assistant, prompt().Messages[0], result}},
		{Messages: []minipi.Message{assistant, result, result}},
		{Messages: []minipi.Message{assistant, result, assistant, result}},
		{Messages: []minipi.Message{{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "a", Name: "work", Arguments: json.RawMessage(`[]`)}}}, result}},
		{Messages: prompt().Messages, Tools: []minipi.ToolDefinition{{Name: "work", Parameters: json.RawMessage(`null`)}}},
	} {
		if _, err := c.Generate(context.Background(), req, nil); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Generate(ctx, prompt(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	if _, err := c.Generate(nil, prompt(), nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached transport")
	}
	if _, err := c.Generate(context.Background(), prompt(), nil); err == nil || strings.Contains(err.Error(), "private") || calls.Load() != 1 {
		t.Fatal("unsafe transport error or retry")
	}
}

func TestOpenAIResponses(t *testing.T) {
	call := `{"id":"a","type":"function","function":{"name":"work","arguments":"{\"x\":1}"}}`
	tools := strings.Replace(strings.Replace(openAIGood, `"stop"`, `"tool_calls"`, 1), `"content":"hello"`, `"content":"hello","tool_calls":[`+call+`]`, 1)
	for _, tc := range []struct {
		name, body string
		stop       minipi.StopReason
		calls      int
		bad        bool
	}{
		{"end", openAIGood, minipi.StopEnd, 0, false},
		{"length", strings.Replace(openAIGood, `"stop"`, `"length"`, 1), minipi.StopLength, 0, false},
		{"tools", tools, minipi.StopToolUse, 1, false},
		{"length_tools", strings.Replace(tools, `"finish_reason":"tool_calls"`, `"finish_reason":"length"`, 1), minipi.StopLength, 1, false},
		{"bad_arguments", strings.Replace(tools, `{\"x\":1}`, `secret`, 1), minipi.StopToolUse, 1, true},
		{"object_arguments", strings.Replace(tools, `"arguments":"{\"x\":1}"`, `"arguments":{}`, 1), minipi.StopToolUse, 1, true},
		{"duplicate", strings.Replace(tools, call, call+`,`+call, 1), minipi.StopToolUse, 2, true},
		{"unknown_type", strings.Replace(tools, `"type":"function"`, `"type":"secret"`, 1), minipi.StopToolUse, 1, true},
		{"missing_calls", strings.Replace(openAIGood, `"stop"`, `"tool_calls"`, 1), minipi.StopToolUse, 0, true},
		{"end_with_calls", strings.Replace(tools, `"finish_reason":"tool_calls"`, `"finish_reason":"stop"`, 1), minipi.StopEnd, 1, true},
		{"role", strings.Replace(openAIGood, `"assistant"`, `"user"`, 1), minipi.StopEnd, 0, true},
		{"unknown_finish", strings.Replace(openAIGood, `"stop"`, `"secret"`, 1), "", 0, true},
		{"trailing", openAIGood + ` secret`, minipi.StopEnd, 0, true},
		{"second_json", openAIGood + ` {}`, minipi.StopEnd, 0, true},
		{"legacy", strings.Replace(openAIGood, `"content":"hello"`, `"content":"hello","function_call":{}`, 1), minipi.StopEnd, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeOpenAIResponse([]byte(tc.body))
			if errors.Is(err, minipi.ErrInvalidResponse) != tc.bad || got.StopReason != tc.stop || len(got.Message.ToolCalls) != tc.calls || got.Message.Content != "hello" || got.Usage.InputTokens != 11 {
				t.Fatalf("response: %+v %v", got, err)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("leaked server data")
			}
		})
	}
	for _, body := range []string{`null`, `{}`, `{"choices":[]}`, `{"choices":[{},{}]}`, strings.Replace(openAIGood, `"content":"hello"`, `"content":null`, 1), strings.Replace(openAIGood, `"content":"hello"`, `"content":123`, 1)} {
		if _, err := decodeOpenAIResponse([]byte(body)); !errors.Is(err, minipi.ErrInvalidResponse) {
			t.Fatal("accepted malformed response")
		}
	}
	for _, body := range []string{strings.Replace(tools, `"content":"hello"`, `"content":null`, 1), strings.Replace(openAIGood, `"content":"hello"`, `"content":null,"refusal":"not allowed"`, 1)} {
		if _, err := decodeOpenAIResponse([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, usage := range []string{`null`, `{}`, `{"prompt_tokens":null,"completion_tokens":0}`, `{"prompt_tokens":-1,"completion_tokens":0}`, `{"prompt_tokens":1.5,"completion_tokens":0}`, `{"prompt_tokens":9223372036854775808,"completion_tokens":0}`, `{"prompt_tokens":9223372036854775807,"completion_tokens":1}`, `{"prompt_tokens":0,"completion_tokens":-1}`} {
		body := openAIGood[:strings.Index(openAIGood, `"usage":`)] + `"usage":` + usage + `}`
		got, err := decodeOpenAIResponse([]byte(body))
		if !errors.Is(err, minipi.ErrInvalidResponse) || got.Message.Content != "hello" || got.Usage != (minipi.Usage{}) {
			t.Fatal("bad usage recovery")
		}
	}
}
