// Package minipi implements an independent Go agent loop inspired by
// https://github.com/earendil-works/pi/tree/main/packages/agent.
// It is not a complete TypeScript-compatible port. It neither persists sessions,
// retries model requests automatically, nor connects external messages or queues.
//
// Supply a Model adapter and validated Tools to New, then use Run with a Context
// and user prompts. Result.Messages is the full history to pass to the next Run;
// Continue instead resumes a history ending in a user message or tool results.
// Each turn generates an assistant response, executes its tool calls serially,
// and feeds results into the next turn. Truncated calls are skipped and paired
// with error results; a response without calls ends the run. There is no mid-run
// message injection, parallel tool execution, or automatic context trimming.
package minipi

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrRunning         = errors.New("minipi: agent already running")
	ErrMaxTurns        = errors.New("minipi: turn budget exhausted")
	ErrMaxTokens       = errors.New("minipi: token budget exhausted")
	ErrInvalidConfig   = errors.New("minipi: invalid configuration")
	ErrInvalidContext  = errors.New("minipi: invalid context")
	ErrInvalidResponse = errors.New("minipi: invalid model response")
)

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message uses ToolCallID and IsError only for tool results. System instructions
// live in Context.SystemPrompt, not in the message history.
type Message struct {
	Role       Role
	Content    string
	Images     []Image // User input only; URLs are fetched by the model provider, not this process.
	ToolCalls  []ToolCall
	ToolCallID string
	IsError    bool
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

type ToolDefinition struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

type ToolResult struct {
	Content   string
	IsError   bool
	Terminate bool
}

// Tool.Validate is mandatory and checks the declared schema; the core checks
// that arguments are a complete JSON object first. Execute must honor ctx.
// Updates are snapshots. Only the returned result can terminate execution.
// Terminate stops immediately and pairs remaining calls with skipped results.
// Ordinary execution errors are fed back to the model; errors accompanying
// Terminate instead fail the run because no subsequent model turn can handle them.
// Cancellation stops the run and remains joined with any real execution error.
// Function closures and external state remain the implementer's responsibility.
type Tool struct {
	Definition ToolDefinition
	Validate   func(json.RawMessage) error
	Execute    func(context.Context, ToolCall, func(ToolResult)) (ToolResult, error)
}

// Model must honor ctx; the core does not forcibly detach blocked calls.
// update supplies a complete assistant snapshot (possibly unfinished arguments),
// not a delta. It is optional; Response.Message is the authoritative final value.
// Callbacks may be concurrent, but must not mutate a snapshot during delivery.
type Model interface {
	Generate(ctx context.Context, request Request, update func(Message)) (Response, error)
}

type Request struct {
	SystemPrompt string
	Messages     []Message
	Tools        []ToolDefinition
	// MaxTokens is the remaining combined input/output budget. Adapters should
	// limit output accordingly. Reported usage is checked after Generate; this
	// is not a precise billing hard cap and cannot stop an in-flight request.
	MaxTokens int64
}

type StopReason string

const (
	// Models may return only these three reasons.
	StopEnd     StopReason = "end"
	StopToolUse StopReason = "tool_use"
	StopLength  StopReason = "length"

	// These reasons are produced by the core, not by model adapters.
	StopTerminate StopReason = "terminate"
	StopCanceled  StopReason = "canceled"
	StopError     StopReason = "error"
	StopMaxTurns  StopReason = "max_turns"
	StopMaxTokens StopReason = "max_tokens"
)

// Usage counts nonnegative input and output tokens, including any cached tokens
// that the adapter elects to account for. Their sum must fit in int64.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

type Response struct {
	Message    Message
	StopReason StopReason
	Usage      Usage
}

// Context is a caller-owned session snapshot. History must have valid roles and
// paired tool calls/results. Failed/truncated arguments may remain in history
// provided their tool results are paired. Tool-call IDs must be globally unique.
type Context struct {
	SystemPrompt string
	Messages     []Message
}

// Config defaults are 32 turns, 100000 tokens and two minutes. Negative limits
// are rejected. A turn includes one model request and its serial tool batch.
type Config struct {
	Model     Model
	Tools     []Tool
	MaxTurns  int
	MaxTokens int64
	Timeout   time.Duration
}

// Agent owns immutable configuration, not conversation state. Use New to create
// it and do not copy it. A single Agent permits only one Run/Continue at a time.
// Model and tool implementations are not cloned and must protect external state.
type Agent struct {
	config  Config
	tools   map[string]Tool
	running atomic.Bool
}

// Running can be called from event observers without acquiring a state mutex.
func (a *Agent) Running() bool { return a.running.Load() }

// Result includes the full transcript even on failure. Invalid model responses
// are retained for diagnosis; such a transcript may require repair before reuse.
type Result struct {
	Messages   []Message
	Usage      Usage
	Turns      int
	StopReason StopReason
}

type EventType string

const (
	EventAgentStart          EventType = "agent_start"
	EventAgentEnd            EventType = "agent_end"
	EventTurnStart           EventType = "turn_start"
	EventTurnEnd             EventType = "turn_end"
	EventMessageStart        EventType = "message_start"
	EventMessageUpdate       EventType = "message_update"
	EventMessageEnd          EventType = "message_end"
	EventToolExecutionStart  EventType = "tool_execution_start"
	EventToolExecutionUpdate EventType = "tool_execution_update"
	EventToolExecutionEnd    EventType = "tool_execution_end"
)

// Event payloads are detached snapshots. Turn is one-based (zero before a turn).
// Err and StopReason describe end events; Result is populated on agent_end.
// Observers run synchronously and must not panic or wait indefinitely. Prefer
// quick returns; durable hosts may use explicitly bounded synchronous persistence
// at message_end, canceling the run on failure before subsequent tool side effects.
// Observer time counts toward the run timeout; cancellation cannot forcibly detach it.
// Delivery is serialized, including parallel model/tool updates. Updates arriving
// after their originating call has closed are ignored. No state mutex is held.
type Event struct {
	Type       EventType
	Turn       int
	Message    Message
	ToolCall   ToolCall
	ToolResult ToolResult
	Usage      Usage
	StopReason StopReason
	Err        error
	Result     Result
}
