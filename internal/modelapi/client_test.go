package modelapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/minipi"
)

const goodResponse = `{"type":"message","role":"assistant","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3,"cache_creation_input_tokens":4,"cache_read_input_tokens":5}}`

func prompt() minipi.Request {
	return minipi.Request{Messages: []minipi.Message{{Role: minipi.RoleUser, Content: "hi"}}}
}

func newTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	c, err := New(Config{APIKey: "test-key-not-real", Model: "test-model", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestConfig(t *testing.T) {
	for _, base := range []string{"", "https://example.test", "https://example.test/v1", "https://example.test/v1/", "http://127.0.0.1:1234", "http://[::1]:1234", "http://localhost:1234", "https://example.test/prefix/v1/", "http://example.test", "http://192.168.1.1", "http://localhost.evil.test", "http://example.test:8080/prefix/v1/", "http://[2001:db8::1]:8080"} {
		t.Run(base, func(t *testing.T) {
			original := &http.Client{}
			c, err := New(Config{APIKey: "fake", Model: "test", BaseURL: base, HTTPClient: original})
			if err != nil {
				t.Fatal(err)
			}
			if c.httpClient.Timeout != defaultTimeout || original.Timeout != 0 || original.CheckRedirect != nil {
				t.Fatal("client snapshot/default failed")
			}
			if !strings.HasSuffix(c.endpoint, "/v1/messages") || strings.Contains(c.endpoint, "/v1/v1/") {
				t.Fatal(c.endpoint)
			}
			if base == "" && c.endpoint != defaultBaseURL+"/v1/messages" {
				t.Fatal(c.endpoint)
			}
		})
	}
	for _, base := range []string{"http://", "http://example.test?key=secret", "http://example.test/#secret", "http://secret@example.test", "ftp://127.0.0.1", "https://example.test?key=secret", "https://example.test/#secret", "https://secret@example.test", "https://example.test/%secret", "https://example.test:secret", "//example.test"} {
		_, err := New(Config{APIKey: "fake", Model: "test", BaseURL: base})
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe URL error for %q: %v", base, err)
		}
	}
	for _, cfg := range []Config{{Model: "x"}, {APIKey: "fake"}, {APIKey: "fake\r\n", Model: "x"}, {APIKey: "fake", Model: "x", MaxOutputTokens: -1}, {APIKey: "fake", Model: "x", HTTPClient: &http.Client{Timeout: -1}}} {
		if _, err := New(cfg); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}

func TestRequestConversion(t *testing.T) {
	requests := make(chan wireRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/prefix/v1/messages" {
			t.Errorf("request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "test-key-not-real" || r.Header.Get("anthropic-version") != "2023-06-01" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("headers missing")
		}
		var captured wireRequest
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Error(err)
		}
		requests <- captured
		fmt.Fprint(w, goodResponse)
	}))
	defer server.Close()
	c, err := New(Config{APIKey: "test-key-not-real", Model: "test-model", BaseURL: server.URL + "/prefix/v1/", MaxOutputTokens: 50})
	if err != nil {
		t.Fatal(err)
	}
	req := minipi.Request{SystemPrompt: "system", MaxTokens: 17, Tools: []minipi.ToolDefinition{{Name: "work", Description: "desc", Parameters: json.RawMessage(`{"type":"object"}`)}}, Messages: []minipi.Message{
		{Role: minipi.RoleUser, Content: "one"}, {Role: minipi.RoleUser, Content: "two"},
		{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "a", Name: "work", Arguments: json.RawMessage(`{"n":1}`)}, {ID: "b", Name: "work", Arguments: json.RawMessage(`{}`)}}},
		{Role: minipi.RoleTool, ToolCallID: "b", Content: "failed", IsError: true},
		{Role: minipi.RoleTool, ToolCallID: "a", Content: ""}, {Role: minipi.RoleUser, Content: "next"},
	}}
	resp, err := c.Generate(context.Background(), req, func(minipi.Message) { t.Error("unexpected streaming update") })
	if err != nil {
		t.Fatal(err)
	}
	captured := <-requests
	if captured.Model != "test-model" || captured.System != "system" || captured.Stream || captured.MaxTokens != 17 {
		t.Fatalf("bad request: %+v", captured)
	}
	if len(captured.Tools) != 1 || captured.Tools[0].Description != "desc" || string(captured.Tools[0].InputSchema) != `{"type":"object"}` {
		t.Fatal("tool definition")
	}
	if len(captured.Messages) != 3 || len(captured.Messages[0].Content) != 2 || len(captured.Messages[1].Content) != 2 || len(captured.Messages[2].Content) != 3 {
		t.Fatalf("merge: %+v", captured.Messages)
	}
	if captured.Messages[1].Content[0].Type != "tool_use" {
		t.Fatal("empty text before tool calls")
	}
	results := captured.Messages[2]
	if results.Role != minipi.RoleUser || results.Content[0].ToolUseID != "b" || !results.Content[0].IsError || results.Content[1].Content == nil || *results.Content[1].Content != "" || results.Content[2].Type != "text" {
		t.Fatalf("results: %+v", results)
	}
	if resp.Message.Content != "hello" || resp.StopReason != minipi.StopEnd || resp.Usage.InputTokens != 11 || resp.Usage.OutputTokens != 3 {
		t.Fatalf("response: %+v", resp)
	}
	for _, budget := range []int64{0, 1, 100} {
		r := prompt()
		r.MaxTokens = budget
		data, err := c.encodeRequest(r)
		if err != nil {
			t.Fatal(err)
		}
		var out wireRequest
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatal(err)
		}
		want := int64(50)
		if budget == 1 {
			want = 1
		}
		if out.MaxTokens != want {
			t.Fatalf("budget %d: %d", budget, out.MaxTokens)
		}
	}
}

func TestResponses(t *testing.T) {
	tool := `{"type":"tool_use","id":"a","name":"work","input":{"x":1}}`
	cases := []struct {
		name, body string
		stop       minipi.StopReason
		calls      int
		bad        bool
	}{
		{"end", goodResponse, minipi.StopEnd, 0, false},
		{"sequence", strings.Replace(goodResponse, "end_turn", "stop_sequence", 1), minipi.StopEnd, 0, false},
		{"length", strings.Replace(goodResponse, "end_turn", "max_tokens", 1), minipi.StopLength, 0, false},
		{"tools", strings.Replace(strings.Replace(goodResponse, `"end_turn"`, `"tool_use"`, 1), `"content":[`, `"content":[`+tool+`,`, 1), minipi.StopToolUse, 1, false},
		{"truncated_tool", strings.Replace(strings.Replace(strings.Replace(goodResponse, `"end_turn"`, `"max_tokens"`, 1), `"content":[`, `"content":[`+tool+`,`, 1), `{"x":1}`, `"{unfinished"`, 1), minipi.StopLength, 1, true},
		{"unknown_block", strings.Replace(goodResponse, `"content":[`, `"content":[{"type":"thinking","thinking":"secret"},`, 1), minipi.StopEnd, 0, true},
		{"wrong_role", strings.Replace(goodResponse, `"assistant"`, `"user"`, 1), minipi.StopEnd, 0, true},
		{"wrong_type", strings.Replace(goodResponse, `"message"`, `"error"`, 1), minipi.StopEnd, 0, true},
		{"unknown_stop", strings.Replace(goodResponse, "end_turn", "pause_turn", 1), "", 0, true},
		{"missing_calls", strings.Replace(goodResponse, "end_turn", "tool_use", 1), minipi.StopToolUse, 0, true},
		{"trailing_json", goodResponse + ` {}`, minipi.StopEnd, 0, true},
		{"trailing_junk", goodResponse + ` secret`, minipi.StopEnd, 0, true},
		{"null_text", strings.Replace(goodResponse, `"content":[`, `"content":[{"type":"text","text":null},`, 1), minipi.StopEnd, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, tc.body) }))
			defer server.Close()
			got, err := newTestClient(t, server).Generate(context.Background(), prompt(), nil)
			if (err != nil) != tc.bad {
				t.Fatalf("error: %v", err)
			}
			if tc.bad && !errors.Is(err, minipi.ErrInvalidResponse) {
				t.Fatalf("error type: %v", err)
			}
			if got.Message.Content != "hello" || got.StopReason != tc.stop || len(got.Message.ToolCalls) != tc.calls || got.Usage.InputTokens != 11 {
				t.Fatalf("lost final response: %+v", got)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("leaked body")
			}
		})
	}
}

func TestUsageValidation(t *testing.T) {
	for _, usage := range []string{`null`, `{}`, `{"input_tokens":-1,"output_tokens":0}`, `{"input_tokens":null,"output_tokens":0}`, `{"input_tokens":1.5,"output_tokens":0}`, `{"input_tokens":9223372036854775808,"output_tokens":0}`, `{"input_tokens":9223372036854775807,"output_tokens":1}`, `{"input_tokens":1,"output_tokens":0,"cache_read_input_tokens":9223372036854775807}`, `{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":-1}`, `{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":9223372036854775807,"cache_read_input_tokens":1}`} {
		body := goodResponse[:strings.Index(goodResponse, `"usage":`)] + `"usage":` + usage + `}`
		got, err := decodeResponse([]byte(body))
		if err == nil || got.Message.Content != "hello" || got.Usage != (minipi.Usage{}) {
			t.Fatalf("usage %s: %+v %v", usage, got, err)
		}
	}
	usage, err := decodeUsage([]byte(`{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":9223372036854775807,"cache_creation":{"ephemeral_5m_input_tokens":100}}`))
	if err != nil || usage.InputTokens != 9223372036854775807 {
		t.Fatalf("boundary: %+v %v", usage, err)
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				close(started)
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
			}))
			defer server.Close()
			c := newTestClient(t, server)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if timeout {
				c.httpClient.Timeout = 40 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { _, err := c.Generate(ctx, prompt(), nil); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("request did not start")
			}
			if !timeout {
				cancel()
			}
			select {
			case err := <-done:
				want := context.Canceled
				if timeout {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatalf("cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("request did not stop")
			}
		})
	}
}

func TestSizeAndTruncation(t *testing.T) {
	for _, tc := range []struct {
		name, body    string
		declared      int
		bad, retained bool
	}{
		{"at_limit", goodResponse + strings.Repeat(" ", MaxResponseBytes-len(goodResponse)), 0, false, true},
		{"over_limit", goodResponse + strings.Repeat(" ", MaxResponseBytes-len(goodResponse)+1), 0, true, true},
		{"partial_json", goodResponse[:len(goodResponse)-5], 0, true, false},
		{"short_read", goodResponse, len(goodResponse) + 100, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.declared > 0 {
					w.Header().Set("Content-Length", fmt.Sprint(tc.declared))
				}
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			got, err := newTestClient(t, server).Generate(context.Background(), prompt(), nil)
			if (err != nil) != tc.bad {
				t.Fatalf("error: %v", err)
			}
			if tc.retained && (got.Message.Content != "hello" || got.Usage.InputTokens != 11) {
				t.Fatalf("lost response: %+v", got)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type logReadFailure struct {
	io.Reader
	err error
}

func (r logReadFailure) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		err = r.err
	}
	return n, err
}

func TestRequestLogsPreserveIOFailureClassification(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	for _, stage := range []string{"transport", "read"} {
		for _, joined := range []bool{false, true} {
			buf.Reset()
			failure := error(context.Canceled)
			if joined {
				failure = errors.Join(failure, errors.New("private-io-error"))
			}
			c, err := New(Config{APIKey: "private-key", Model: "private-model", BaseURL: "https://private-host.test", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if stage == "transport" {
					return nil, failure
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(logReadFailure{strings.NewReader(goodResponse), failure})}, nil
			})}})
			if err != nil {
				t.Fatal("client construction failed")
			}
			ctx := minipi.WithInputID(context.Background(), "0123456789abcdef0123456789abcdef")
			req := prompt()
			req.Messages[0].Content = "private-prompt"
			_, err = c.Generate(ctx, req, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatal("changed cancellation return")
			}
			if strings.Contains(buf.String(), "private-") || strings.Contains(buf.String(), "hello") {
				t.Fatal("sensitive request data logged")
			}
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			var record map[string]any
			if json.Unmarshal([]byte(lines[len(lines)-1]), &record) != nil {
				t.Fatal("invalid request log")
			}
			wantLevel, wantFailure := "DEBUG", "canceled"
			if joined {
				wantLevel, wantFailure = "ERROR", "failure"
			}
			if record["stage"] != stage || record["level"] != wantLevel || record["failure"] != wantFailure {
				t.Fatal("lost raw IO failure classification")
			}
			if record["input_id"] != "0123456789abcdef0123456789abcdef" {
				t.Fatal("lost input correlation")
			}
		}
	}
}

func TestHTTPErrorDatabaseDetail(t *testing.T) {
	const key = "private-api-key"
	for _, protocol := range []Protocol{ProtocolOpenAI, ProtocolAnthropic} {
		for _, tc := range []struct {
			name, body, want string
			readErr          error
		}{
			{"json", `{"error":{"message":"private-diagnostic"}}`, "private-diagnostic", nil},
			{"empty", "", "", nil},
			{"plain", "bad request", "bad request", nil},
			{"exact", strings.Repeat("x", maxErrorBodyBytes), strings.Repeat("x", maxErrorBodyBytes), nil},
			{"large", strings.Repeat("x", maxErrorBodyBytes+100), "[response body truncated]", nil},
			{"utf8", strings.Repeat("中", maxErrorBodyBytes/3-1) + "xx中", "[response body truncated]", nil},
			{"key", "echo " + key, "[redacted]", nil},
			{"key_boundary", strings.Repeat("x", maxErrorBodyBytes-8) + key, "[response body truncated]", nil},
			{"partial", "private-diagnostic", "[response body read incomplete]", errors.New("private-read-error")},
			{"canceled", "private-diagnostic", "[response body read incomplete]", context.Canceled},
			{"timeout", "private-diagnostic", "[response body read incomplete]", context.DeadlineExceeded},
		} {
			t.Run(string(protocol)+"/"+tc.name, func(t *testing.T) {
				var logs bytes.Buffer
				old := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				defer slog.SetDefault(old)
				reader := strings.NewReader(tc.body)
				calls := 0
				c, err := New(Config{Protocol: protocol, APIKey: key, Model: "test", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					var body io.Reader = reader
					if tc.readErr != nil {
						body = logReadFailure{body, tc.readErr}
					}
					return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(body)}, nil
				})}})
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.Generate(context.Background(), prompt(), nil)
				var httpErr *HTTPError
				if !errors.As(err, &httpErr) || httpErr.StatusCode != 400 || calls != 1 {
					t.Fatal("lost HTTP status or retried request")
				}
				detail := httpErr.DatabaseDetail()
				if !strings.Contains(detail, tc.want) || len(detail) > maxErrorBodyBytes+100 || !json.Valid([]byte(fmt.Sprintf("%q", detail))) {
					t.Fatal("invalid bounded diagnostic")
				}
				if tc.name == "exact" && detail != tc.want || tc.name == "empty" && detail != "" {
					t.Fatal("incorrect boundary handling")
				}
				if len(tc.body)-reader.Len() > maxErrorBodyBytes+1 {
					t.Fatal("unbounded response read")
				}
				if strings.Contains(detail, key) || tc.name == "key_boundary" && strings.Contains(detail, key[:8]) {
					t.Fatal("credential echo retained")
				}
				if strings.Contains(err.Error(), "private-") || strings.Contains(logs.String(), "private-") {
					t.Fatal("diagnostic leaked outside database interface")
				}
				if (tc.readErr == context.Canceled || tc.readErr == context.DeadlineExceeded) && !errors.Is(err, tc.readErr) {
					t.Fatal("lost cancellation classification")
				}
			})
		}
	}
}

func TestTransportRedaction(t *testing.T) {
	c, err := New(Config{APIKey: "fake", Model: "test", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("https://secret.test/?key=secret fake")
	})}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Generate(context.Background(), prompt(), nil)
	if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "fake") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestCrossOriginRedirect(t *testing.T) {
	var leaks atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaks.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/?key=secret")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	original := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { t.Error("custom redirect invoked"); return nil }}
	c, err := New(Config{APIKey: "fake", Model: "test", BaseURL: source.URL, HTTPClient: original})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Generate(context.Background(), prompt(), nil)
	if err == nil || leaks.Load() != 0 || original.CheckRedirect == nil {
		t.Fatal("redirect not blocked")
	}
}

func TestInvalidRequestsDoNotSend(t *testing.T) {
	var sent atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		fmt.Fprint(w, goodResponse)
	}))
	defer server.Close()
	c := newTestClient(t, server)
	for _, raw := range []string{"", "null", "[]", "1", `"text"`, `{} {}`, `{"unfinished":`} {
		r := prompt()
		r.Messages = append(r.Messages,
			minipi.Message{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "a", Name: "work", Arguments: json.RawMessage(raw)}}},
			minipi.Message{Role: minipi.RoleTool, ToolCallID: "a", Content: "done"})
		if _, err := c.Generate(context.Background(), r, nil); err == nil {
			t.Fatalf("accepted input %q", raw)
		}
		r = prompt()
		r.Tools = []minipi.ToolDefinition{{Name: "work", Parameters: json.RawMessage(raw)}}
		if _, err := c.Generate(context.Background(), r, nil); err == nil {
			t.Fatalf("accepted schema %q", raw)
		}
	}
	call := minipi.Message{Role: minipi.RoleAssistant, ToolCalls: []minipi.ToolCall{{ID: "a", Name: "work", Arguments: json.RawMessage(`{}`)}}}
	for _, messages := range [][]minipi.Message{
		nil,
		{{Role: "system", Content: "bad"}},
		{{Role: minipi.RoleUser, Content: "hi", IsError: true}},
		{{Role: minipi.RoleAssistant, ToolCallID: "bad", Content: "hi"}},
		{{Role: minipi.RoleTool, ToolCallID: "missing"}},
		{call},
		{call, {Role: minipi.RoleUser, Content: "interrupted"}},
		{call, {Role: minipi.RoleTool, ToolCallID: "wrong"}},
		{call, {Role: minipi.RoleTool, ToolCallID: "a"}, {Role: minipi.RoleTool, ToolCallID: "a"}},
		{call, {Role: minipi.RoleTool, ToolCallID: "a"}, call},
	} {
		if _, err := c.Generate(context.Background(), minipi.Request{Messages: messages}, nil); err == nil {
			t.Fatalf("accepted history %+v", messages)
		}
	}
	r := prompt()
	r.MaxTokens = -1
	if _, err := c.Generate(context.Background(), r, nil); err == nil {
		t.Fatal("accepted negative budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Generate(ctx, prompt(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.Generate(nil, prompt(), nil); err == nil {
		t.Fatal("nil context")
	}
	if sent.Load() != 0 {
		t.Fatalf("invalid request sent: %d", sent.Load())
	}
}

func TestAssistantMergeAndToolInput(t *testing.T) {
	c, err := New(Config{APIKey: "fake", Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	r := prompt()
	r.Messages = append(r.Messages, minipi.Message{Role: minipi.RoleAssistant, Content: "first"}, minipi.Message{Role: minipi.RoleAssistant, Content: "second"})
	data, err := c.encodeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	var out wireRequest
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 2 || len(out.Messages[1].Content) != 2 || out.Messages[1].Content[1].Text != "second" {
		t.Fatalf("assistant merge: %s", data)
	}
	for _, input := range []string{`{}`, `[]`, `null`, `42`, `"unfinished"`} {
		tool := `{"type":"tool_use","id":"a","name":"work","input":` + input + `}`
		body := strings.Replace(strings.Replace(goodResponse, `"end_turn"`, `"tool_use"`, 1), `"content":[`, `"content":[`+tool+`,`, 1)
		got, err := decodeResponse([]byte(body))
		if (err == nil) != (input == `{}`) || len(got.Message.ToolCalls) != 1 || string(got.Message.ToolCalls[0].Arguments) != input || got.Usage.InputTokens != 11 {
			t.Fatalf("tool input %s: %+v %v", input, got, err)
		}
	}
}

func TestHTTPFailures(t *testing.T) {
	for _, status := range []int{301, 302, 307, 308, 401, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Location", "/leak?key=secret")
				w.WriteHeader(status)
				fmt.Fprint(w, "secret test-key-not-real")
			}))
			defer server.Close()
			c := newTestClient(t, server)
			_, err := c.Generate(context.Background(), prompt(), nil)
			var statusErr *HTTPError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != status || requests.Load() != 1 {
				t.Fatalf("status/retry: %v %d", err, requests.Load())
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "test-key") {
				t.Fatal("leaked credentials/body")
			}
		})
	}
}
