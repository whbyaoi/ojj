package minipi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// New validates and snapshots configuration. JSON schemas must be objects;
// schema semantics are enforced by each tool's mandatory Validate function.
func New(config Config) (*Agent, error) {
	if config.Model == nil || nilModel(config.Model) {
		return nil, fmt.Errorf("%w: model is required", ErrInvalidConfig)
	}
	if config.MaxTurns < 0 || config.MaxTokens < 0 || config.Timeout < 0 {
		return nil, fmt.Errorf("%w: negative limit", ErrInvalidConfig)
	}
	if config.MaxTurns == 0 {
		config.MaxTurns = 32
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = 100000
	}
	if config.Timeout == 0 {
		config.Timeout = 2 * time.Minute
	}
	config.Tools = append([]Tool(nil), config.Tools...)
	tools := make(map[string]Tool, len(config.Tools))
	for i := range config.Tools {
		t := &config.Tools[i]
		t.Definition = cloneDefinition(t.Definition)
		name := t.Definition.Name
		if strings.TrimSpace(name) == "" || t.Validate == nil || t.Execute == nil {
			return nil, fmt.Errorf("%w: tool %q needs name, validator and executor", ErrInvalidConfig, name)
		}
		if _, exists := tools[name]; exists {
			return nil, fmt.Errorf("%w: duplicate tool %q", ErrInvalidConfig, name)
		}
		if !jsonObject(t.Definition.Parameters) {
			return nil, fmt.Errorf("%w: tool %q schema must be a JSON object", ErrInvalidConfig, name)
		}
		tools[name] = *t
	}
	return &Agent{config: config, tools: tools}, nil
}

func nilModel(model Model) bool {
	v := reflect.ValueOf(model)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func jsonObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

func cloneCall(call ToolCall) ToolCall {
	call.Arguments = append(json.RawMessage(nil), call.Arguments...)
	return call
}

func cloneMessage(message Message) Message {
	message.Images = append([]Image(nil), message.Images...)
	calls := message.ToolCalls
	if calls != nil {
		message.ToolCalls = make([]ToolCall, len(calls))
		for i, call := range calls {
			message.ToolCalls[i] = cloneCall(call)
		}
	}
	return message
}

func cloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	out := make([]Message, len(messages))
	for i, message := range messages {
		out[i] = cloneMessage(message)
	}
	return out
}

func cloneDefinition(def ToolDefinition) ToolDefinition {
	def.Parameters = append(json.RawMessage(nil), def.Parameters...)
	return def
}

func cloneResult(result Result) Result {
	result.Messages = cloneMessages(result.Messages)
	return result
}

func messageShape(message Message) error {
	if err := ValidateImages(message.Role, message.Images); err != nil {
		return err
	}
	switch message.Role {
	case RoleUser:
		if len(message.ToolCalls) != 0 || message.ToolCallID != "" || message.IsError {
			return fmt.Errorf("user message contains tool fields")
		}
	case RoleAssistant:
		if message.ToolCallID != "" || message.IsError {
			return fmt.Errorf("assistant message contains tool-result fields")
		}
	case RoleTool:
		if len(message.ToolCalls) != 0 || strings.TrimSpace(message.ToolCallID) == "" {
			return fmt.Errorf("tool message needs a call ID and no calls")
		}
	default:
		return fmt.Errorf("unknown role %q", message.Role)
	}
	return nil
}

func validateCalls(calls []ToolCall, seen map[string]bool) error {
	for _, call := range calls {
		if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" {
			return fmt.Errorf("tool call needs a nonempty ID and name")
		}
		if seen[call.ID] {
			return fmt.Errorf("duplicate tool call ID %q", call.ID)
		}
		seen[call.ID] = true
	}
	return nil
}

func validateHistory(messages []Message) error {
	pending := make(map[string]bool)
	seen := make(map[string]bool)
	for i, message := range messages {
		if err := messageShape(message); err != nil {
			return fmt.Errorf("message %d: %w", i, err)
		}
		if len(pending) > 0 && message.Role != RoleTool {
			return fmt.Errorf("message %d interrupts pending tool results", i)
		}
		switch message.Role {
		case RoleAssistant:
			if err := validateCalls(message.ToolCalls, seen); err != nil {
				return fmt.Errorf("message %d: %w", i, err)
			}
			for _, call := range message.ToolCalls {
				pending[call.ID] = true
			}
		case RoleTool:
			if !pending[message.ToolCallID] {
				return fmt.Errorf("message %d has unmatched tool result %q", i, message.ToolCallID)
			}
			delete(pending, message.ToolCallID)
		}
	}
	if len(pending) > 0 {
		return fmt.Errorf("history has pending tool results")
	}
	return nil
}

func validateResponse(response Response, history []Message) error {
	if response.Message.Role != RoleAssistant {
		return fmt.Errorf("%w: expected assistant role", ErrInvalidResponse)
	}
	if err := messageShape(response.Message); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	seen := make(map[string]bool)
	for _, message := range history {
		for _, call := range message.ToolCalls {
			seen[call.ID] = true
		}
	}
	if err := validateCalls(response.Message.ToolCalls, seen); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}
	switch response.StopReason {
	case StopEnd:
		if len(response.Message.ToolCalls) > 0 {
			return fmt.Errorf("%w: end reason contains tool calls", ErrInvalidResponse)
		}
	case StopToolUse:
		if len(response.Message.ToolCalls) == 0 {
			return fmt.Errorf("%w: tool_use without calls", ErrInvalidResponse)
		}
	case StopLength:
	default:
		return fmt.Errorf("%w: unknown stop reason %q", ErrInvalidResponse, response.StopReason)
	}
	return nil
}

func addUsage(total *Usage, next Usage) error {
	if next.InputTokens < 0 || next.OutputTokens < 0 ||
		next.InputTokens > math.MaxInt64-next.OutputTokens {
		return fmt.Errorf("%w: negative or overflowing usage", ErrInvalidResponse)
	}
	used := total.InputTokens + total.OutputTokens
	if next.InputTokens+next.OutputTokens > math.MaxInt64-used {
		return fmt.Errorf("%w: accumulated usage overflows", ErrInvalidResponse)
	}
	total.InputTokens += next.InputTokens
	total.OutputTokens += next.OutputTokens
	return nil
}
