package qq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ojj/internal/cancelerr"
	"ojj/internal/event"

	"github.com/gorilla/websocket"
)

type publishFunc func(context.Context, event.Input) (event.PublishResult, error)

func (f publishFunc) Publish(ctx context.Context, in event.Input) (event.PublishResult, error) {
	return f(ctx, in)
}

func testClient(t *testing.T, publisher Publisher, handler func(*websocket.Conn)) *Client {
	t.Helper()
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || body["appId"] != "app" || body["clientSecret"] != "test-secret" {
			t.Error("bad token request")
		}
		fmt.Fprint(w, `{"access_token":"test-token","expires_in":"7200"}`)
	})
	mux.HandleFunc("/gateway/bot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "QQBot test-token" {
			t.Error("bad gateway authorization")
		}
		json.NewEncoder(w).Encode(map[string]any{"url": "wss" + strings.TrimPrefix(server.URL, "https") + "/ws", "shards": 1, "session_start_limit": map[string]int{"remaining": 10, "max_concurrency": 1}})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		handler(conn)
	})
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig
	c, err := New(Config{AppID: "app", ClientSecret: "test-secret", APIBaseURL: server.URL, HTTPClient: server.Client(), Dialer: &dialer, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), RetryInitial: time.Millisecond, RetryMax: 5 * time.Millisecond, MaxReconnectAttempts: 3}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func sendEvent(conn *websocket.Conn, seq int, kind string, data any) error {
	return conn.WriteJSON(map[string]any{"op": 0, "s": seq, "t": kind, "d": data})
}

func TestRunPublishFailureKeepsSessionAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := event.Open(ctx, event.Config{Path: filepath.Join(t.TempDir(), "events.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	producer, err := store.BindProducer("qq.websocket")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	published := make(chan event.PublishResult, 2)
	publisher := publishFunc(func(ctx context.Context, in event.Input) (event.PublishResult, error) {
		if calls.Add(1) == 1 {
			select {
			case <-time.After(130 * time.Millisecond):
				return event.PublishResult{}, event.ErrCapacity
			case <-ctx.Done():
				return event.PublishResult{}, ctx.Err()
			}
		}
		result, err := producer.Publish(ctx, in)
		if err == nil {
			published <- result
		}
		return result, err
	})
	var connections, heartbeats atomic.Int32
	c := testClient(t, publisher, func(conn *websocket.Conn) {
		n := connections.Add(1)
		write(conn, 10, map[string]int{"heartbeat_interval": 40})
		var auth struct {
			Op   int `json:"op"`
			Data struct {
				Token     string `json:"token"`
				Seq       int    `json:"seq"`
				SessionID string `json:"session_id"`
				Intents   int    `json:"intents"`
			} `json:"d"`
		}
		if err := conn.ReadJSON(&auth); err != nil {
			t.Error(err)
			return
		}
		if auth.Data.Token != "QQBot test-token" {
			t.Error("bad identify token")
		}
		if n == 1 {
			if auth.Op != 2 || auth.Data.Intents != 1<<25 {
				t.Error("bad identify")
			}
			sendEvent(conn, 1, "READY", map[string]string{"session_id": "session1"})
		} else {
			if auth.Op != 6 || auth.Data.Seq != 1 || auth.Data.SessionID != "session1" {
				t.Errorf("bad resume: %+v", auth)
			}
		}
		sendEvent(conn, 2, "C2C_MESSAGE_CREATE", json.RawMessage(privateMessage))
		if n > 1 {
			sendEvent(conn, 3, "C2C_MESSAGE_CREATE", json.RawMessage(privateMessage))
			sendEvent(conn, 4, "RESUMED", "")
		}
		for {
			var p payload
			if err := conn.ReadJSON(&p); err != nil {
				return
			}
			if p.Op != nil && *p.Op == 1 {
				heartbeats.Add(1)
				write(conn, 11, nil)
			}
		}
	})
	if err := c.Run(ctx); !errors.Is(err, ErrPublish) {
		t.Fatalf("publish failure: %v", err)
	}
	if c.state.id != "session1" || c.state.committed == nil || *c.state.committed != 1 {
		t.Fatalf("lost cursor: %+v", c.state)
	}
	if heartbeats.Load() < 2 {
		t.Fatal("slow publish blocked heartbeats")
	}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var results []event.PublishResult
	for len(results) < 2 {
		select {
		case result := <-published:
			results = append(results, result)
		case <-ctx.Done():
			t.Fatal("resume did not persist messages")
		}
	}
	if !results[1].Duplicate || results[0].Event.ID != results[1].Event.ID {
		t.Fatal("replay not deduplicated")
	}
	if err := c.Run(ctx); !errors.Is(err, ErrRunning) {
		t.Fatalf("concurrent run: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReadyImmediatelyDisconnectedStillResumes(t *testing.T) {
	var connections atomic.Int32
	c := testClient(t, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }), func(conn *websocket.Conn) {
		n := connections.Add(1)
		write(conn, 10, map[string]int{"heartbeat_interval": 1000})
		var p struct {
			Op   int `json:"op"`
			Data struct {
				Seq int `json:"seq"`
			} `json:"d"`
		}
		if err := conn.ReadJSON(&p); err != nil {
			t.Error(err)
			return
		}
		if n == 1 {
			sendEvent(conn, 1, "READY", map[string]string{"session_id": "ready-session"})
		} else {
			if p.Op != 6 || p.Data.Seq != 1 {
				t.Errorf("must resume READY cursor: %+v", p)
			}
			conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4014, "secret must not leak"), time.Now().Add(time.Second))
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := c.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "4014") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("permanent close: %v", err)
	}
	if connections.Load() != 2 {
		t.Fatal(connections.Load())
	}
}

func TestMissingACKAndQuota(t *testing.T) {
	c := testClient(t, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }), func(conn *websocket.Conn) {
		write(conn, 10, map[string]int{"heartbeat_interval": 20})
		var p payload
		if conn.ReadJSON(&p) != nil {
			return
		}
		if p.Op != nil && *p.Op == 6 {
			sendEvent(conn, 2, "RESUMED", "")
		} else {
			sendEvent(conn, 1, "READY", map[string]string{"session_id": "session"})
		}
		for conn.ReadJSON(&p) == nil {
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "attempts exhausted") {
		t.Fatalf("missing ACK: %v", err)
	}
}

func TestRefreshConnectionPreservesSession(t *testing.T) {
	c := testClient(t, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }), func(conn *websocket.Conn) {
		write(conn, 10, map[string]int{"heartbeat_interval": 30})
		var p payload
		if conn.ReadJSON(&p) != nil {
			return
		}
		sendEvent(conn, 1, "READY", map[string]string{"session_id": "refresh-session"})
		for conn.ReadJSON(&p) == nil {
			write(conn, 11, nil)
		}
	})
	c.token, c.refreshAt = "test-token", time.Now().Add(150*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := c.connect(ctx, &c.state)
	if err == nil || !strings.Contains(err.Error(), "refresh token") {
		t.Fatalf("refresh: %v", err)
	}
	if c.state.committed == nil || *c.state.committed != 1 {
		t.Fatal("refresh lost session")
	}
}

func TestResumeReplayCanExceedHandshakeDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("long replay regression")
	}
	c := testClient(t, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil }), func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		write(conn, 10, map[string]int{"heartbeat_interval": 100})
		var p payload
		if conn.ReadJSON(&p) != nil {
			return
		}
		sendEvent(conn, 2, "IGNORED_EVENT", map[string]any{})
		start := time.Now()
		for conn.ReadJSON(&p) == nil {
			if time.Since(start) >= 16*time.Second {
				sendEvent(conn, 3, "RESUMED", "")
				conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4014, ""), time.Now().Add(time.Second))
				return
			}
			write(conn, 11, nil)
		}
	})
	seq := int64(1)
	c.state = session{id: "replay-session", committed: &seq}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := c.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "4014") {
		t.Fatalf("replay prematurely interrupted: %v", err)
	}
}

func TestRunPublishCancellationBoundary(t *testing.T) {
	secret := errors.New("private database rollback detail")
	for _, tc := range []struct {
		name       string
		cancel     bool
		deadline   bool
		publishErr func(context.Context) error
		want       error
	}{
		{"failure during cancellation", true, false, func(context.Context) error { return secret }, ErrPublish},
		{"cancel and rollback failure", true, false, func(ctx context.Context) error { return errors.Join(ctx.Err(), secret) }, ErrPublish},
		{"wrapped rollback failure", true, false, func(ctx context.Context) error { return fmt.Errorf("publish: %w", errors.Join(ctx.Err(), secret)) }, ErrPublish},
		{"normal cancellation", true, false, func(ctx context.Context) error { return ctx.Err() }, context.Canceled},
		{"wrapped joined cancellation", true, false, func(ctx context.Context) error { return errors.Join(ctx.Err(), fmt.Errorf("publish: %w", ctx.Err())) }, context.Canceled},
		{"lifecycle deadline", false, true, func(ctx context.Context) error { return ctx.Err() }, context.DeadlineExceeded},
		{"publish deadline with active lifecycle", false, false, func(context.Context) error { return context.DeadlineExceeded }, ErrPublish},
		{"publish cancellation with active lifecycle", false, false, func(context.Context) error { return context.Canceled }, ErrPublish},
		{"publish deadline versus lifecycle cancellation", true, false, func(context.Context) error { return context.DeadlineExceeded }, ErrPublish},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeout := 3 * time.Second
			if tc.deadline {
				timeout = 300 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var calls atomic.Int32
			publisher := publishFunc(func(publishCtx context.Context, _ event.Input) (event.PublishResult, error) {
				calls.Add(1)
				if tc.cancel {
					cancel()
				}
				if tc.cancel || tc.deadline {
					<-publishCtx.Done()
				}
				return event.PublishResult{}, tc.publishErr(publishCtx)
			})
			c := testClient(t, publisher, func(conn *websocket.Conn) {
				write(conn, 10, map[string]int{"heartbeat_interval": 1000})
				var p payload
				if conn.ReadJSON(&p) != nil {
					return
				}
				sendEvent(conn, 1, "READY", map[string]string{"session_id": "cancel-session"})
				sendEvent(conn, 2, "C2C_MESSAGE_CREATE", json.RawMessage(privateMessage))
				for conn.ReadJSON(&p) == nil {
					write(conn, 11, nil)
				}
			})
			err := c.Run(ctx)
			if err != tc.want {
				t.Fatalf("Run() = %v, want %v", err, tc.want)
			}
			if calls.Load() != 1 {
				t.Fatalf("publish calls = %d, want 1", calls.Load())
			}
			if strings.Contains(err.Error(), secret.Error()) || errors.Is(err, secret) {
				t.Fatal("publisher detail leaked")
			}
			if c.state.committed == nil || *c.state.committed != 1 {
				t.Fatalf("failed or canceled publication advanced cursor: %+v", c.state)
			}
		})
	}
}

type sessionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f sessionRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type sessionReadFunc func([]byte) (int, error)

func (f sessionReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestRunTransportCancellationBoundary(t *testing.T) {
	for _, stage := range []string{"http", "body", "dial"} {
		for _, joined := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/joined=%v", stage, joined), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				failure := errors.New("private transport detail")
				fail := func() error {
					cancel()
					if joined {
						return errors.Join(ctx.Err(), failure)
					}
					return fmt.Errorf("transport: %w", ctx.Err())
				}
				c := testClient(t, publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
					t.Error("unexpected publication")
					return event.PublishResult{}, nil
				}), func(*websocket.Conn) { t.Error("unexpected websocket session") })
				switch stage {
				case "http":
					c.http = &http.Client{Transport: sessionRoundTripFunc(func(*http.Request) (*http.Response, error) {
						return nil, fail()
					})}
				case "body":
					c.http = &http.Client{Transport: sessionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
							Body: io.NopCloser(sessionReadFunc(func([]byte) (int, error) { return 0, fail() }))}, nil
					})}
				case "dial":
					c.dialer.NetDialContext = func(context.Context, string, string) (net.Conn, error) { return nil, fail() }
				}
				err := c.Run(ctx)
				if !joined && err != context.Canceled {
					t.Fatalf("normal shutdown reported failure: %v", err)
				}
				if joined && (err == nil || cancelerr.Only(err, context.Canceled)) {
					t.Fatalf("concurrent failure lost: %v", err)
				}
				if err != nil && (strings.Contains(err.Error(), failure.Error()) || errors.Is(err, failure)) {
					t.Fatal("transport detail leaked")
				}
			})
		}
	}
}

func TestRunAPIFailureConcurrentCancellation(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &http.Client{Transport: sessionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				cancel()
				return &http.Response{
					StatusCode: status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"message":"private server detail"}`)),
					Request:    r,
				}, nil
			})}
			c, err := New(Config{AppID: "app", ClientSecret: "secret", APIBaseURL: "https://example.invalid", HTTPClient: client},
				publishFunc(func(context.Context, event.Input) (event.PublishResult, error) {
					t.Error("unexpected publish")
					return event.PublishResult{}, nil
				}))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(ctx)
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Status != status {
				t.Fatalf("API failure lost during cancellation: %v", err)
			}
			if status == http.StatusForbidden && errors.Is(err, context.Canceled) {
				t.Fatalf("permanent error must take priority: %v", err)
			}
			if status == http.StatusServiceUnavailable && !errors.Is(err, context.Canceled) {
				t.Fatalf("non-permanent failure must retain cancellation: %v", err)
			}
			if strings.Contains(err.Error(), "private server detail") {
				t.Fatal("API response detail leaked")
			}
		})
	}
}

func TestTokenRefreshAndAPIValidation(t *testing.T) {
	var requests atomic.Int32
	response := `{"access_token":"token1","expires_in":120}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, response) }))
	defer server.Close()
	publisher := publishFunc(func(context.Context, event.Input) (event.PublishResult, error) { return event.PublishResult{}, nil })
	c, err := New(Config{AppID: "app", ClientSecret: "secret", APIBaseURL: server.URL, HTTPClient: server.Client()}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := c.accessToken(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 1 {
		t.Fatal("token not cached")
	}
	c.refreshAt = time.Now().Add(-time.Second)
	response = `{"access_token":"token2","expires_in":"7200"}`
	if token, err := c.accessToken(ctx); err != nil || token != "token2" {
		t.Fatal(token, err)
	}
	response = `{"code":100007,"message":"secret"}`
	c.token = ""
	_, err = c.accessToken(ctx)
	var fatal *permanentError
	if !errors.As(err, &fatal) || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	response = `{"url":"wss://example.com","shards":1,"session_start_limit":{"remaining":0}}`
	_, err = c.gateway(ctx, "token", true)
	if !errors.As(err, &fatal) {
		t.Fatalf("quota: %v", err)
	}
	if _, err = c.gateway(ctx, "token", false); err != nil {
		t.Fatalf("resume does not consume identify quota: %v", err)
	}
	if _, err = New(Config{AppID: "app", ClientSecret: "secret", APIBaseURL: "http://example.com"}, publisher); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}
