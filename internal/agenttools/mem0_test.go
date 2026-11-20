package agenttools

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/minipi"
)

const memoryTestAdd = `{"messages":[{"role":"user","content":"I love fresh vegetable pizza."}]}`
const memoryTestSearch = `{"query":"vegetable"}`

func TestMem0Contract(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.Header.Get("X-API-Key") != "synthetic-key" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect request headers or method")
		}
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/api/memories":
			if string(body) != `{"messages":[{"role":"user","content":"I love fresh vegetable pizza."}],"agent_id":"ojj"}` {
				t.Error("incorrect add body")
			}
			io.WriteString(w, `{"results":[{"id":"new","event":"ADD"}]}`)
		case "/api/search":
			if string(body) != `{"query":"vegetable","filters":{"agent_id":"ojj"}}` {
				t.Error("incorrect search body")
			}
			io.WriteString(w, `{"results":[{"id":"old","memory":"User loves fresh vegetable pizza","hash":"h","metadata":null,"score":0.32,"created_at":"2026-01-01","updated_at":null,"user_id":"alice","attributed_to":"user"}]}`)
		default:
			t.Error("incorrect endpoint")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	tools, err := NewMem0(Mem0Config{BaseURL: server.URL + "/api/", APIKey: "synthetic-key"})
	if err != nil || len(tools) != 2 {
		t.Fatalf("construct tools: %v", err)
	}
	for index, field := range []string{"messages", "query"} {
		var schema struct {
			Properties           map[string]json.RawMessage `json:"properties"`
			Required             []string                   `json:"required"`
			AdditionalProperties bool                       `json:"additionalProperties"`
		}
		if json.Unmarshal(tools[index].Definition.Parameters, &schema) != nil || len(schema.Properties) != 1 || schema.Properties[field] == nil || len(schema.Required) != 1 || schema.Required[0] != field || schema.AdditionalProperties {
			t.Fatal("schema must expose only content arguments")
		}
	}
	for _, tc := range []struct {
		index      int
		args, want string
	}{
		{0, memoryTestAdd, `"event":"ADD"`},
		{1, memoryTestSearch, `"score":0.32`},
		{1, memoryTestSearch, `"metadata":null`},
		{1, memoryTestSearch, `"user_id":"alice"`},
	} {
		tool := tools[tc.index]
		if !json.Valid(tool.Definition.Parameters) || tool.Validate(json.RawMessage(tc.args)) != nil {
			t.Fatal("invalid schema or rejected valid arguments")
		}
		out, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(tc.args)}, nil)
		if err != nil || out.IsError || out.Terminate || !strings.Contains(out.Content, tc.want) {
			t.Fatalf("execute: %+v %v", out, err)
		}
	}
	if calls.Load() != 4 {
		t.Fatal("unexpected request count")
	}
}

func TestMem0ValidationBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	tools, err := NewMem0(Mem0Config{BaseURL: server.URL, APIKey: "synthetic-key"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		index int
		args  string
	}{
		{0, `{}`}, {0, `null`}, {0, `[]`},
		{0, `{"messages":[]}`},
		{0, `{"messages":[{"role":"system","content":"secret"}]}`},
		{0, `{"messages":[{"role":"user","content":" "}]}`},
		{0, `{"messages":[{"role":"user","content":"a","content":"b"}]}`},
		{0, `{"messages":[{"role":"user","content":"a","extra":true}]}`},
		{0, memoryTestAdd + `{}`},
		{0, strings.Replace(memoryTestAdd, "pizza", strings.Repeat("x", 33<<10), 1)},
		{0, strings.Repeat(" ", maxMemoryRequestBytes) + memoryTestAdd},
		{1, `{}`}, {1, `{"query":" "}`}, {1, `{"query":null}`},
		{1, `{"query":"a","Query":"b"}`}, {1, `{"query":"a","query":"b"}`},
		{1, `{"query":"a","filters":null}`}, {1, `{"query":"a","filters":[]}`},
		{1, `{"query":"a","filters":"secret"}`}, {1, `{"query":"a","api_key":"secret"}`},
		{1, `{"query":"` + strings.Repeat("x", 8193) + `"}`},
	} {
		tool := tools[tc.index]
		if tool.Validate(json.RawMessage(tc.args)) == nil {
			t.Fatal("accepted invalid arguments")
		}
		if _, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(tc.args)}, nil); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("execute must validate safely")
		}
	}
	// Models cannot add, replace or remove the host-owned scope, even with its fixed value.
	for index, base := range []string{memoryTestAdd, memoryTestSearch} {
		for _, extra := range []string{
			`"user_id":"alice"`, `"user_id":null`,
			`"agent_id":"other"`, `"agent_id":"ojj"`, `"agent_id":null`,
			`"filters":{}`, `"filters":{"user_id":"alice"}`,
			`"filters":{"agent_id":"other"}`, `"filters":{"agent_id":"ojj"}`,
		} {
			raw := json.RawMessage(strings.TrimSuffix(base, "}") + "," + extra + "}")
			if tools[index].Validate(raw) == nil {
				t.Fatal("model-supplied scope accepted")
			}
			if _, err := tools[index].Execute(context.Background(), minipi.ToolCall{Arguments: raw}, nil); err == nil {
				t.Fatal("execute accepted model-supplied scope")
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid call reached network")
	}
}

func TestMem0Configuration(t *testing.T) {
	for _, cfg := range []Mem0Config{
		{APIKey: "secret"},
		{BaseURL: "ftp://secret", APIKey: "secret"},
		{BaseURL: "http://secret@localhost", APIKey: "secret"},
		{BaseURL: "http://localhost?secret", APIKey: "secret"},
		{BaseURL: "http://localhost#secret", APIKey: "secret"},
		{BaseURL: "http://localhost"},
		{BaseURL: "http://localhost", APIKey: "secret\r\n"},
		{BaseURL: "http://localhost", APIKey: "secret", Timeout: -time.Second},
	} {
		if _, err := NewMem0(cfg); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("configuration validation failed or leaked input")
		}
	}
}

func TestMem0SafeFailuresAndNoRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", 401, "synthetic-private-error"},
		{"server", 500, "synthetic-private-error"},
		{"malformed", 200, "synthetic-private-error"},
		{"null", 200, "null"}, {"array", 200, "[]"},
		{"missing_results", 200, `{}`}, {"null_results", 200, `{"results":null}`},
		{"large", 200, `{"results":[],"extra":"` + strings.Repeat("x", maxMemoryResponseBytes) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			tools, err := NewMem0(Mem0Config{BaseURL: server.URL, APIKey: "synthetic-key"})
			if err != nil {
				t.Fatal(err)
			}
			out, err := tools[1].Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(memoryTestSearch)}, nil)
			if err == nil || !out.IsError || calls.Load() != 1 {
				t.Fatal("expected one failed call")
			}
			for _, secret := range []string{server.URL, "synthetic-key", "synthetic-private-error"} {
				if strings.Contains(out.Content+err.Error(), secret) {
					t.Fatal("failure leaked private data")
				}
			}
		})
	}
}

func TestMem0RedirectBlocked(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	tools, err := NewMem0(Mem0Config{BaseURL: server.URL, APIKey: "synthetic-key"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := tools[0].Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(memoryTestAdd)}, nil)
	if err == nil || !out.IsError || !strings.Contains(out.Content, "不要自动重试") || forwarded.Load() != 0 {
		t.Fatal("redirect was followed or ambiguous write was not reported")
	}
}

func TestMem0TimeoutAndCancellation(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	tools, err := NewMem0(Mem0Config{BaseURL: server.URL, APIKey: "synthetic-key", Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	call := minipi.ToolCall{Arguments: json.RawMessage(memoryTestAdd)}
	out, err := tools[0].Execute(context.Background(), call, nil)
	if err == nil || !out.IsError || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatal("local timeout must be an ordinary tool failure")
	}
	<-entered
	tools, err = NewMem0(Mem0Config{BaseURL: server.URL, APIKey: "synthetic-key"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-entered; cancel() }()
	_, err = tools[0].Execute(ctx, call, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("parent cancellation lost")
	}
	before := calls.Load()
	_, err = tools[0].Execute(ctx, call, nil)
	if !errors.Is(err, context.Canceled) || calls.Load() != before {
		t.Fatal("pre-canceled call reached network")
	}
}
