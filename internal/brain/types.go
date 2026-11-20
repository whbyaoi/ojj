// Package brain provides a durable, single-owner region and serial agent queue.
package brain

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"ojj/internal/minipi"
)

var (
	ErrInvalid         = errors.New("brain: invalid configuration or input")
	ErrOwned           = errors.New("brain: database already owned")
	ErrIdentity        = errors.New("brain: database identity or schema mismatch")
	ErrClosed          = errors.New("brain: closed")
	ErrRunning         = errors.New("brain: Run already active")
	ErrConflict        = errors.New("brain: input ID conflicts with stored text")
	ErrNotFound        = errors.New("brain: input not found")
	ErrCapacity        = errors.New("brain: storage capacity exceeded")
	ErrContextCapacity = errors.New("brain: context capacity exceeded")
	ErrPanic           = errors.New("brain: external callback panicked")
)

type modelSkippedKey struct{}

// WithModelSkipped records a host decision for this input, not an error. A skipped
// input succeeds without running the model/tools and remains available in history.
// InputContext must set false for runnable inputs to mask inherited decisions.
func WithModelSkipped(ctx context.Context, skipped bool) context.Context {
	return context.WithValue(ctx, modelSkippedKey{}, skipped)
}

// Config requires Path, ID and Agent.Model. Zero limits use DefaultConfig.
// Byte limits are logical sizes, not SQLite/WAL disk quotas. Logs are never evicted.
// MaxInputBytes counts input ID/text plus nonempty image slice JSON.
// MaxStoredBytes counts input ID/text plus message JSON; metadata is excluded.
// Image URLs/labels count toward these limits; remote bytes are never cached.
// Stored images may expire or become inaccessible to the model provider.
// MaxContextBytes counts SystemPrompt plus canonical message JSON at every model
// request; exceeding it fails rather than truncating messages. Only the current
// input's images are loaded. Historical images, tool calls and tool results are
// omitted before counting bytes; visible assistant prose is retained unless not sent.
// MaxHistoryMessages bounds initial history loading, including the current input.
// Only the newest complete succeeded tasks fitting this limit are retained;
// this record count is applied before historical content filtering.
// Messages generated during the current task are not subject to this count limit.
// Open's context bounds opening only; Run's context controls scheduling.
type Config struct {
	Path         string
	ID           string
	SystemPrompt string
	Agent        minipi.Config
	// InputContext restores trusted values for each durable input using its real ID.
	// It runs before the agent; errors, nil contexts and panics stop Run as storage
	// failures. A nil callback leaves the context unchanged. Callbacks must honor
	// cancellation; returned values live only for the current input's execution.
	InputContext       func(context.Context, string) (context.Context, error)
	PollInterval       time.Duration
	OperationTimeout   time.Duration
	MaxInputBytes      int
	MaxStoredBytes     int64
	MaxContextBytes    int
	MaxHistoryMessages int
}

func DefaultConfig() Config {
	return Config{PollInterval: 100 * time.Millisecond, OperationTimeout: 5 * time.Second,
		MaxInputBytes: 1 << 20, MaxStoredBytes: 1 << 30, MaxContextBytes: 8 << 20, MaxHistoryMessages: 1000}
}

func (c Config) normalized() (Config, error) {
	d := DefaultConfig()
	if c.PollInterval == 0 {
		c.PollInterval = d.PollInterval
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = d.OperationTimeout
	}
	if c.MaxInputBytes == 0 {
		c.MaxInputBytes = d.MaxInputBytes
	}
	if c.MaxStoredBytes == 0 {
		c.MaxStoredBytes = d.MaxStoredBytes
	}
	if c.MaxContextBytes == 0 {
		c.MaxContextBytes = d.MaxContextBytes
	}
	if c.MaxHistoryMessages == 0 {
		c.MaxHistoryMessages = d.MaxHistoryMessages
	}
	if strings.TrimSpace(c.ID) == "" || !utf8.ValidString(c.ID) || c.Path == "" ||
		c.PollInterval < 0 || c.OperationTimeout < 0 || c.MaxInputBytes < 1 || c.MaxStoredBytes < 1 || c.MaxContextBytes < 1 || c.MaxHistoryMessages < 1 {
		return c, ErrInvalid
	}
	if _, err := minipi.New(c.Agent); err != nil {
		return c, err
	}
	c.Agent.Tools = append([]minipi.Tool(nil), c.Agent.Tools...)
	for i := range c.Agent.Tools {
		c.Agent.Tools[i].Definition.Parameters = append(json.RawMessage(nil), c.Agent.Tools[i].Definition.Parameters...)
	}
	return c, nil
}

type Status string

const (
	StatusPending     Status = "pending"
	StatusProcessing  Status = "processing"
	StatusSucceeded   Status = "succeeded"
	StatusFailed      Status = "failed"
	StatusInterrupted Status = "interrupted"
)

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type Input struct {
	Sequence    int64             `json:"sequence"`
	ID          string            `json:"id"`
	Text        string            `json:"text"`
	Status      Status            `json:"status"`
	ReceivedAt  time.Time         `json:"received_at"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	LastError   string            `json:"last_error"`
	Turns       int               `json:"turns"`
	Usage       Usage             `json:"usage"`
	StopReason  minipi.StopReason `json:"stop_reason"`
}

// Block stores raw argument bytes (base64 JSON) even for malformed JSON/UTF-8.
// Input is present only for valid JSON; Diagnostic explains invalid arguments.
type Block struct {
	Type         string          `json:"type"`
	Text         string          `json:"text,omitempty"`
	ID           string          `json:"id,omitempty"`
	Name         string          `json:"name,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	ArgumentsRaw []byte          `json:"arguments_raw,omitempty"`
	ToolUseID    string          `json:"tool_use_id,omitempty"`
	IsError      bool            `json:"is_error,omitempty"`
	Diagnostic   string          `json:"diagnostic,omitempty"`
	Image        *minipi.Image   `json:"image,omitempty"`
}

type Message struct {
	Role       string  `json:"role"`
	Content    []Block `json:"content"`
	ToolCallID string  `json:"tool_call_id,omitempty"`
	IsError    bool    `json:"is_error,omitempty"`
}

type StoredMessage struct {
	Sequence   int64             `json:"sequence"`
	InputID    string            `json:"input_id"`
	Message    Message           `json:"message"`
	Turn       int               `json:"turn"`
	Usage      Usage             `json:"usage"`
	StopReason minipi.StopReason `json:"stop_reason"`
	Error      string            `json:"error,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
}

func encodeMessage(m minipi.Message) Message {
	out := Message{Role: string(m.Role), Content: []Block{{Type: "text", Text: m.Content}}, ToolCallID: m.ToolCallID, IsError: m.IsError}
	if m.Role == minipi.RoleTool {
		out.Role = "toolResult"
		out.Content = []Block{{Type: "tool_result", Text: m.Content, ToolUseID: m.ToolCallID, IsError: m.IsError}}
	}
	for _, image := range m.Images {
		out.Content = append(out.Content, Block{Type: "image", Image: &image})
	}
	for _, call := range m.ToolCalls {
		b := Block{Type: "tool_use", ID: call.ID, Name: call.Name, ArgumentsRaw: append([]byte(nil), call.Arguments...)}
		if json.Valid(call.Arguments) && utf8.Valid(call.Arguments) {
			b.Input = append(json.RawMessage(nil), call.Arguments...)
		}
		raw := strings.TrimSpace(string(call.Arguments))
		if len(b.Input) == 0 || !strings.HasPrefix(raw, "{") {
			b.Diagnostic = "arguments must be a complete UTF-8 JSON object"
		}
		out.Content = append(out.Content, b)
	}
	return out
}

func decodeMessage(m Message) minipi.Message {
	out := minipi.Message{Role: minipi.Role(m.Role), ToolCallID: m.ToolCallID, IsError: m.IsError}
	if m.Role == "toolResult" {
		out.Role = minipi.RoleTool
	}
	for _, b := range m.Content {
		switch b.Type {
		case "text", "tool_result":
			out.Content = b.Text
		case "image":
			if b.Image != nil {
				out.Images = append(out.Images, *b.Image)
			}
		case "tool_use":
			out.ToolCalls = append(out.ToolCalls, minipi.ToolCall{ID: b.ID, Name: b.Name, Arguments: append(json.RawMessage(nil), b.ArgumentsRaw...)})
		}
	}
	return out
}
