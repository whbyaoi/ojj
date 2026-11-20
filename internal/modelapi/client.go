// Package modelapi adapts non-streaming Anthropic Messages and OpenAI Chat Completions to minipi.
package modelapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"ojj/internal/imagecache"
	"ojj/internal/minipi"
)

type Protocol string

const (
	ProtocolAnthropic Protocol = "anthropic"
	ProtocolOpenAI    Protocol = "openai"
)

const (
	defaultBaseURL         = "https://api.anthropic.com"
	defaultMaxOutputTokens = 4096
	defaultTimeout         = 2 * time.Minute
	// MaxResponseBytes bounds successful response bodies, including whitespace.
	MaxResponseBytes = 8 << 20
)

// Config is snapshotted by New. A custom HTTPClient's Transport is trusted and
// must not forward credentials elsewhere. Its redirect policy is overridden;
// its positive timeout is kept, otherwise a two-minute timeout is installed.
// BaseURL accepts a path prefix, optionally ending in /v1 (with a trailing slash).
// HTTP and HTTPS are supported; HTTP sends credentials and messages in plaintext.
// Queries, fragments, and userinfo are rejected.
// MaxOutputTokens defaults to 4096. APIKey and Model are mandatory.
type Config struct {
	// ImageCache resolves attachments locally; nil degrades images to text.
	ImageCache imagecache.Resolver
	// Protocol defaults to Anthropic for existing direct callers; the app selects OpenAI.
	Protocol Protocol
	// OpenAITokenLimitField accepts max_tokens (default) or max_completion_tokens.
	// Some reasoning models require the latter; no automatic retries are made.
	OpenAITokenLimitField string
	APIKey                string
	Model                 string
	BaseURL               string
	MaxOutputTokens       int
	HTTPClient            *http.Client
}

// Client is safe for concurrent use provided its custom Transport is also safe.
type Client struct {
	imageCache            imagecache.Resolver
	protocol              Protocol
	openAITokenLimitField string
	apiKey                string
	model                 string
	endpoint              string
	maxOutputTokens       int
	httpClient            *http.Client
}

var _ minipi.Model = (*Client)(nil)

// HTTPError keeps bounded, untrusted response details separate from Error().
// Only DatabaseDetail should be used to persist the body; never log the struct.
type HTTPError struct {
	StatusCode int
	detail     string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("modelapi: HTTP status %d", e.StatusCode)
}

// DatabaseDetail is sensitive diagnostic metadata, not model input or log text.
func (e *HTTPError) DatabaseDetail() string { return e.detail }

const maxErrorBodyBytes = 3072

func (c *Client) readHTTPError(res *http.Response) (*HTTPError, error) {
	data, readErr := io.ReadAll(io.LimitReader(res.Body, maxErrorBodyBytes+1))
	truncated := len(data) > maxErrorBodyBytes
	if truncated {
		data = data[:maxErrorBodyBytes]
		// Do not retain a credential prefix cut off by the read limit.
		for n := min(len(c.apiKey)-1, len(data)); n > 0; n-- {
			if bytes.HasSuffix(data, []byte(c.apiKey[:n])) {
				data = data[:len(data)-n]
				break
			}
		}
	}
	detail := strings.ReplaceAll(string(data), c.apiKey, "[redacted]")
	// Replacement may grow the body. Keep a byte bound and valid UTF-8.
	if len(detail) > maxErrorBodyBytes {
		detail, truncated = detail[:maxErrorBodyBytes], true
	}
	detail = strings.ToValidUTF8(detail, "")
	if truncated {
		detail += " [response body truncated]"
	}
	if readErr != nil {
		detail += " [response body read incomplete]"
	}
	return &HTTPError{StatusCode: res.StatusCode, detail: detail}, readErr
}

// New validates configuration without contacting the server.
func New(config Config) (*Client, error) {
	if strings.TrimSpace(config.APIKey) == "" || strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("modelapi: API key and model are required")
	}
	for _, ch := range config.APIKey {
		if ch < 32 || ch > 126 {
			return nil, errors.New("modelapi: invalid API key header")
		}
	}
	if config.MaxOutputTokens < 0 {
		return nil, errors.New("modelapi: negative output limit")
	}
	if config.MaxOutputTokens == 0 {
		config.MaxOutputTokens = defaultMaxOutputTokens
	}
	if config.Protocol == "" {
		config.Protocol = ProtocolAnthropic
	}
	base, suffix := config.BaseURL, "/messages"
	switch config.Protocol {
	case ProtocolAnthropic:
		if config.OpenAITokenLimitField != "" {
			return nil, errors.New("modelapi: OpenAI token field requires OpenAI protocol")
		}
		if base == "" {
			base = defaultBaseURL
		}
	case ProtocolOpenAI:
		if base == "" {
			base = "https://api.openai.com"
		}
		suffix = "/chat/completions"
		if config.OpenAITokenLimitField == "" {
			config.OpenAITokenLimitField = "max_tokens"
		}
		if config.OpenAITokenLimitField != "max_tokens" && config.OpenAITokenLimitField != "max_completion_tokens" {
			return nil, errors.New("modelapi: invalid OpenAI token limit field")
		}
	default:
		return nil, errors.New("modelapi: unsupported protocol")
	}
	endpoint, err := apiURL(base, suffix)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{}
	if config.HTTPClient != nil {
		*hc = *config.HTTPClient
	}
	if hc.Timeout < 0 {
		return nil, errors.New("modelapi: negative HTTP timeout")
	}
	if hc.Timeout == 0 {
		hc.Timeout = defaultTimeout
	}
	// Never follow even same-origin redirects: a redirect target may contain
	// query secrets, and custom redirect policies must not leak the API key.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{imageCache: config.ImageCache, protocol: config.Protocol, openAITokenLimitField: config.OpenAITokenLimitField,
		apiKey: config.APIKey, model: config.Model, endpoint: endpoint,
		maxOutputTokens: config.MaxOutputTokens, httpClient: hc}, nil
}

func apiURL(base, suffix string) (string, error) {
	bad := errors.New("modelapi: invalid or insecure base URL")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", bad
	}
	if u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") {
		return "", bad
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", bad
	}
	path := strings.TrimRight(u.EscapedPath(), "/")
	if !strings.HasSuffix(path, "/v1") {
		path += "/v1"
	}
	path += suffix
	decoded, err := url.PathUnescape(path)
	if err != nil {
		return "", bad
	}
	u.Path, u.RawPath = decoded, path
	return u.String(), nil
}

// Generate makes exactly one non-streaming request, with no retries or updates.
// Positive request.MaxTokens caps output by the remaining combined token budget;
// zero means no additional cap. This cannot predict or bound input-token costs.
// On protocol/read errors, any fully decoded final message and valid usage are
// returned alongside the error. Malformed/truncated JSON cannot be recovered.
func (c *Client) Generate(ctx context.Context, request minipi.Request, _ func(minipi.Message)) (response minipi.Response, err error) {
	started := time.Now()
	stage, status := "validate", 0
	var ioErr error
	defer func() {
		minipi.LogOperation(ctx, "modelapi", stage, started, errors.Join(err, ioErr), slog.LevelInfo,
			slog.Int("http_status", status), slog.Int64("input_tokens", response.Usage.InputTokens), slog.Int64("output_tokens", response.Usage.OutputTokens))
	}()
	minipi.LogOperation(ctx, "modelapi", "request_start", started, nil, slog.LevelDebug)
	var zero minipi.Response
	if ctx == nil {
		return zero, errors.New("modelapi: nil context")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	stage = "encode"
	// Validate the complete history before performing any attachment downloads.
	if _, err := c.encodeRequest(request); err != nil {
		return zero, err
	}
	stage = "images"
	images, err := c.prepareImages(ctx, request)
	if err != nil {
		return zero, err
	}
	stage = "encode"
	body, err := c.encodePrepared(request, images)
	if err != nil {
		return zero, err
	}
	stage = "construct"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return zero, errors.New("modelapi: cannot construct HTTP request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.protocol == ProtocolOpenAI {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	} else {
		req.Header.Set("x-api-key", c.apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	stage = "transport"
	res, err := c.httpClient.Do(req)
	if err != nil {
		ioErr = err
		return zero, safeIOError(ctx, err)
	}
	defer res.Body.Close()
	stage, status = "http_status", res.StatusCode
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		httpErr, readErr := c.readHTTPError(res)
		if readErr != nil {
			ioErr = readErr
			return zero, errors.Join(httpErr, safeIOError(ctx, readErr))
		}
		return zero, httpErr
	}
	stage = "decode"
	data, readErr := io.ReadAll(io.LimitReader(res.Body, MaxResponseBytes+1))
	var sizeErr error
	if len(data) > MaxResponseBytes {
		data = data[:MaxResponseBytes]
		sizeErr = errors.New("modelapi: response exceeds size limit")
	}
	decode := decodeResponse
	if c.protocol == ProtocolOpenAI {
		decode = decodeOpenAIResponse
	}
	response, decodeErr := decode(data)
	if readErr != nil {
		stage, ioErr = "read", readErr
		readErr = safeIOError(ctx, readErr)
	}
	if sizeErr != nil {
		stage = "response_limit"
	}
	if decodeErr == nil && sizeErr == nil && readErr == nil {
		stage = "request_end"
	}
	return response, errors.Join(decodeErr, sizeErr, readErr, ctx.Err())
}

// Do not wrap arbitrary transport/url errors: they can contain request URLs,
// proxy credentials, response snippets or the API key itself.
func safeIOError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return fmt.Errorf("modelapi: HTTP timeout: %w", context.DeadlineExceeded)
	}
	return errors.New("modelapi: HTTP transport or body read failed")
}

func jsonObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{' && json.Valid(raw)
}
