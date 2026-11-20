package qq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"ojj/internal/event"
)

func sendingClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	c, err := New(Config{AppID: "app", ClientSecret: "secret", APIBaseURL: s.URL, HTTPClient: s.Client()},
		publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSendText(t *testing.T) {
	var tokens, sends atomic.Int32
	c := sendingClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/app/getAppAccessToken" {
			tokens.Add(1)
			fmt.Fprint(w, `{"access_token":"token","expires_in":7200}`)
			return
		}
		sends.Add(1)
		if r.Method != "POST" || r.Header.Get("Authorization") != "QQBot token" {
			t.Error("invalid send request")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["content"] != "你好" || body["msg_type"] != float64(0) {
			t.Error("invalid body", body)
		}
		switch r.URL.Path {
		case "/v2/users/user_1/messages":
			if body["msg_id"] != "incoming" || body["msg_seq"] != float64(1) {
				t.Error("invalid reply", body)
			}
		case "/v2/groups/group_1/messages":
			if _, exists := body["msg_id"]; exists {
				t.Error("active message has msg_id")
			}
		default:
			t.Error("unexpected path", r.URL.Path)
		}
		fmt.Fprint(w, `{"id":"sent","timestamp":"2026-01-01T00:00:00Z"}`)
	})
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			m := TextMessage{TargetType: "private", TargetID: "user_1", Content: "你好", MsgID: "incoming"}
			if i%2 == 0 {
				m.TargetType, m.TargetID, m.MsgID = "group", "group_1", ""
			}
			result, err := c.SendText(context.Background(), m)
			if err != nil || result.ID != "sent" {
				t.Error(result, err)
			}
		})
	}
	wg.Wait()
	if tokens.Load() != 1 || sends.Load() != 20 {
		t.Fatal(tokens.Load(), sends.Load())
	}
}

func TestSendErrorsAndValidation(t *testing.T) {
	var requests atomic.Int32
	c := sendingClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(401)
		fmt.Fprint(w, `{"code":123,"message":"secret"}`)
	})
	m := TextMessage{TargetType: "group", TargetID: "group", Content: "hello"}
	for _, bad := range []TextMessage{
		{}, {TargetType: "channel", TargetID: "group", Content: "hi"},
		{TargetType: "group", TargetID: "../users", Content: "hi"},
		{TargetType: "group", TargetID: "group", Content: " "},
		{TargetType: "group", TargetID: "group", Content: strings.Repeat("x", 65537)},
		{TargetType: "group", TargetID: "group", Content: "hi", MsgSeq: 1},
	} {
		if _, err := c.SendText(context.Background(), bad); err == nil {
			t.Fatal("accepted invalid message", bad)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid arguments made request")
	}
	c.token, c.refreshAt = "old", time.Now().Add(time.Hour)
	_, err := c.SendText(context.Background(), m)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 401 || strings.Contains(err.Error(), "secret") || requests.Load() != 1 || c.token != "" {
		t.Fatal(err, requests.Load(), c.token)
	}
	// A late 401 for an old token must not invalidate a newer cached token.
	c.token = "new"
	_ = c.request(context.Background(), "GET", "/old", "old", nil, &SentMessage{})
	if c.token != "new" {
		t.Fatal("stale request invalidated new token")
	}
}

func TestTokenRefreshWaitCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	c := sendingClient(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		fmt.Fprint(w, `{"access_token":"token","expires_in":7200}`)
	})
	done := make(chan error, 1)
	go func() { _, err := c.accessToken(context.Background()); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.accessToken(ctx)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type sendRoundTripper func(*http.Request) (*http.Response, error)

func (f sendRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type sendErrorBody struct{ err error }

func (b sendErrorBody) Read([]byte) (int, error) { return 0, b.err }
func (b sendErrorBody) Close() error             { return nil }

type sendDeadlineBody struct{ ctx context.Context }

func (b sendDeadlineBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b sendDeadlineBody) Close() error { return nil }

func TestSendUnauthorizedMalformedBody(t *testing.T) {
	for _, body := range []string{"", "broken", strings.Repeat("x", (1<<20)+1), "read-error"} {
		for _, cached := range []string{"old", "new"} {
			calls := 0
			c := &Client{cfg: Config{APIBaseURL: "https://example.invalid"}, token: cached}
			c.http = &http.Client{Transport: sendRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				var reader io.ReadCloser = io.NopCloser(strings.NewReader(body))
				if body == "read-error" {
					reader = sendErrorBody{errors.New("read failed")}
				}
				return &http.Response{StatusCode: 401, Body: reader, Header: make(http.Header)}, nil
			})}
			err := c.request(context.Background(), "POST", "/send", "old", nil, &SentMessage{})
			want := ""
			if cached == "new" {
				want = "new"
			}
			if err == nil || c.token != want || calls != 1 {
				t.Fatalf("malformed 401: err=%v token=%q calls=%d", err, c.token, calls)
			}
		}
	}
}

func TestSendRequestLocalTimeout(t *testing.T) {
	for _, stage := range []string{"transport", "body"} {
		for _, parentTimeout := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parent=%v", stage, parentTimeout), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := &Client{cfg: Config{APIBaseURL: "https://example.invalid"}, token: "token", refreshAt: time.Now().Add(time.Hour)}
					calls := 0
					c.http = &http.Client{Transport: sendRoundTripper(func(r *http.Request) (*http.Response, error) {
						calls++
						if stage == "transport" {
							<-r.Context().Done()
							return nil, r.Context().Err()
						}
						return &http.Response{StatusCode: 200, Body: sendDeadlineBody{r.Context()}, Header: make(http.Header)}, nil
					})}
					ctx := context.Background()
					if parentTimeout {
						var cancel context.CancelFunc
						ctx, cancel = context.WithTimeout(ctx, time.Second)
						defer cancel()
					}
					_, err := c.SendText(ctx, TextMessage{TargetType: "private", TargetID: "user", Content: "hello"})
					if err == nil || calls != 1 || errors.Is(err, context.DeadlineExceeded) != parentTimeout || errors.Is(err, context.Canceled) {
						t.Fatalf("timeout error=%v calls=%d", err, calls)
					}
				})
			})
		}
	}
}

func TestSendMalformedAndCanceled(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"id":123}`, `{"code":40034005}`, `broken`} {
		t.Run(body, func(t *testing.T) {
			c := sendingClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
			c.token, c.refreshAt = "token", time.Now().Add(time.Hour)
			m := TextMessage{TargetType: "private", TargetID: "user", Content: "hello"}
			if _, err := c.SendText(context.Background(), m); err == nil {
				t.Fatal("accepted malformed response")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := c.SendText(ctx, m); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
