// Package qq 将 QQ 官方 WebSocket 消息转为可靠事件。
// 会话游标仅驻留内存；进程重启或会话失效可能产生接收缺口。
package qq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"ojj/internal/cancelerr"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrConfig   = errors.New("qq: invalid configuration")
	ErrProtocol = errors.New("qq: invalid gateway protocol")
	ErrRunning  = errors.New("qq: client already running")
	ErrPublish  = errors.New("qq: message could not be persisted")
)

type permanentError struct{ error }

func permanent(err error) error { return &permanentError{err} }

// Config 不保存到磁盘，禁止打印整个配置。HTTPClient/Dialer 可用于代理或测试。
// 不允许明文 HTTP/WS；跟随重定向可能泄露密钥，因此 API 禁止重定向。
type Config struct {
	AppID                string
	ClientSecret         string
	APIBaseURL           string
	HTTPClient           *http.Client
	Dialer               *websocket.Dialer
	Logger               *slog.Logger
	RetryInitial         time.Duration
	RetryMax             time.Duration
	MaxReconnectAttempts int
}

type Client struct {
	cfg           Config
	publisher     Publisher
	http          *http.Client
	dialer        websocket.Dialer
	running       atomic.Bool
	tokenMu       sync.Mutex
	refreshing    chan struct{}
	token         string
	refreshAt     time.Time
	identifyAfter time.Time
	state         session // 由 Run 独占，停止后保留供同一客户端再次恢复。
}

func New(cfg Config, publisher Publisher) (*Client, error) {
	if !validID(cfg.AppID) || strings.TrimSpace(cfg.ClientSecret) == "" || publisher == nil {
		return nil, ErrConfig
	}
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = "https://api.bot.qq.com"
	}
	u, err := url.Parse(cfg.APIBaseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, ErrConfig
	}
	cfg.APIBaseURL = strings.TrimRight(cfg.APIBaseURL, "/")
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.Logger = cfg.Logger.With("component", "qq")
	if cfg.RetryInitial == 0 {
		cfg.RetryInitial = time.Second
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = time.Minute
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = 10
	}
	if cfg.RetryInitial < 0 || cfg.RetryMax < cfg.RetryInitial || cfg.MaxReconnectAttempts < 1 {
		return nil, ErrConfig
	}
	h := http.Client{Timeout: 15 * time.Second}
	if cfg.HTTPClient != nil {
		h = *cfg.HTTPClient
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	d := *websocket.DefaultDialer
	if cfg.Dialer != nil {
		d = *cfg.Dialer
	}
	d.HandshakeTimeout = 15 * time.Second
	return &Client{cfg: cfg, publisher: publisher, http: &h, dialer: d}, nil
}

// APIError 只保留状态码和业务码，不泄露响应体、URL 或凭证。
type APIError struct {
	Status int
	Code   int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("qq: API failure (http=%d code=%d)", e.Status, e.Code)
}

func (c *Client) request(ctx context.Context, method, path, token string, body io.Reader, result any) (retErr error) {
	started := time.Now()
	statusCode := 0
	operation := "api_request"
	switch path {
	case "/app/getAppAccessToken":
		operation = "auth_request"
	case "/gateway/bot":
		operation = "gateway_request"
	}
	defer func() {
		if retErr != nil {
			logFailure(c.logger(), slog.LevelWarn, operation, retErr, "http_status", statusCode, "duration", time.Since(started))
		} else {
			c.logger().Debug("QQ HTTP request completed", "stage", operation, "http_status", statusCode, "duration", time.Since(started))
		}
	}()
	parent := ctx
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, parent.Err())
		}
	}()
	requestCanceled := func() error {
		if parent.Err() != nil {
			return parent.Err()
		}
		// A request-local deadline is not cancellation of the calling agent.
		return errors.New("qq: API request timed out")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.APIBaseURL+path, body)
	if err != nil {
		return permanent(ErrConfig)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "QQBot "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if cancelerr.Only(err, ctx.Err()) {
			return requestCanceled()
		}
		return errors.New("qq: API transport failure")
	}
	statusCode = resp.StatusCode
	defer resp.Body.Close()
	// Invalidate before reading the body: malformed/empty/error bodies on 401
	// must not leave a rejected token cached. Never invalidate a newer token.
	if resp.StatusCode == http.StatusUnauthorized && token != "" {
		c.tokenMu.Lock()
		if c.token == token {
			c.token = ""
		}
		c.tokenMu.Unlock()
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		if cancelerr.Only(err, ctx.Err()) {
			return requestCanceled()
		}
		return ErrProtocol
	}
	if len(raw) > 1<<20 {
		return ErrProtocol
	}
	var status struct {
		Code    int `json:"code"`
		ErrCode int `json:"err_code"`
	}
	if json.Unmarshal(raw, &status) != nil {
		return ErrProtocol
	}
	code := status.Code
	if code == 0 {
		code = status.ErrCode
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || code != 0 {
		e := &APIError{Status: resp.StatusCode, Code: code}
		if code == 100007 || code == 100016 || code == 10004 || resp.StatusCode == 403 {
			return permanent(e)
		}
		return e
	}
	if json.Unmarshal(raw, result) != nil {
		return ErrProtocol
	}
	return nil
}

func (c *Client) logger() *slog.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return slog.Default().With("component", "qq")
}

func logFailure(logger *slog.Logger, level slog.Level, operation string, err error, attrs ...any) {
	category := "operation_failure"
	switch {
	case cancelerr.Only(err, context.Canceled):
		level, category = slog.LevelDebug, "canceled"
	case cancelerr.Only(err, context.DeadlineExceeded):
		level, category = slog.LevelWarn, "timeout"
	case errors.Is(err, ErrConfig):
		category = "configuration"
	case errors.Is(err, ErrProtocol):
		category = "protocol"
	case errors.Is(err, ErrPublish):
		category = "publish"
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		category = "api_failure"
		attrs = append(attrs, "api_status", apiErr.Status, "api_code", apiErr.Code)
	}
	attrs = append(attrs, "stage", operation, "failure", category)
	logger.Log(context.Background(), level, "QQ operation failed", attrs...)
}

func (c *Client) accessToken(ctx context.Context) (_ string, retErr error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		c.tokenMu.Lock()
		if c.token != "" && time.Now().Before(c.refreshAt) {
			token := c.token
			c.tokenMu.Unlock()
			return token, nil
		}
		if done := c.refreshing; done != nil {
			c.tokenMu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				continue
			}
		}
		c.refreshing = make(chan struct{})
		c.tokenMu.Unlock()
		defer func() {
			c.tokenMu.Lock()
			close(c.refreshing)
			c.refreshing = nil
			c.tokenMu.Unlock()
		}()
		break
	}
	started := time.Now()
	c.logger().Debug("QQ authentication refresh started")
	defer func() {
		if retErr != nil {
			logFailure(c.logger(), slog.LevelWarn, "auth_refresh", retErr, "duration", time.Since(started))
		} else {
			c.logger().Info("QQ authentication refreshed", "duration", time.Since(started))
		}
	}()
	body, _ := json.Marshal(struct {
		AppID  string `json:"appId"`
		Secret string `json:"clientSecret"`
	}{c.cfg.AppID, c.cfg.ClientSecret})
	var response struct {
		Token   string          `json:"access_token"`
		Expires json.RawMessage `json:"expires_in"`
	}
	if err := c.request(ctx, http.MethodPost, "/app/getAppAccessToken", "", strings.NewReader(string(body)), &response); err != nil {
		return "", err
	}
	expires := strings.Trim(string(response.Expires), "\"")
	seconds, err := strconv.ParseInt(expires, 10, 64)
	if err != nil || seconds < 1 || seconds > 86400 || response.Token == "" {
		return "", ErrProtocol
	}
	lifetime := time.Duration(seconds) * time.Second
	margin := time.Minute
	if lifetime <= margin {
		margin = lifetime / 2
	}
	c.tokenMu.Lock()
	c.token, c.refreshAt = response.Token, time.Now().Add(lifetime-margin)
	c.tokenMu.Unlock()
	return response.Token, nil
}

func (c *Client) gateway(ctx context.Context, token string, identify bool) (string, error) {
	var response struct {
		URL    string `json:"url"`
		Shards int    `json:"shards"`
		Limit  *struct {
			Remaining      int   `json:"remaining"`
			ResetAfter     int64 `json:"reset_after"`
			MaxConcurrency int   `json:"max_concurrency"`
		} `json:"session_start_limit"`
	}
	if err := c.request(ctx, http.MethodGet, "/gateway/bot", token, nil, &response); err != nil {
		return "", err
	}
	u, err := url.Parse(response.URL)
	if err != nil || u.Scheme != "wss" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return "", permanent(ErrProtocol)
	}
	// 首版仅支持单分片。服务器要求多分片时停止，不悄悄遗漏其余分片。
	if response.Shards > 1 {
		return "", permanent(errors.New("qq: gateway requires multiple shards"))
	}
	if identify && response.Limit != nil && response.Limit.Remaining <= 0 {
		return "", permanent(errors.New("qq: session start quota exhausted"))
	}
	return response.URL, nil
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
