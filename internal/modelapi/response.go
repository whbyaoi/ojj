package modelapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"ojj/internal/minipi"
)

func invalidResponse(reason string) error {
	return fmt.Errorf("modelapi: %w: %s", minipi.ErrInvalidResponse, reason)
}

// Decode independent raw fields so a bad block, stop reason or usage field
// does not erase a recoverable final message or otherwise valid accounting.
func decodeResponse(data []byte) (minipi.Response, error) {
	var out minipi.Response
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return out, invalidResponse("invalid JSON message")
	}
	var resultErr error
	add := func(reason string) { resultErr = errors.Join(resultErr, invalidResponse(reason)) }
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		add("expected one complete JSON value")
	}
	var typ, role, stop string
	if json.Unmarshal(fields["type"], &typ) != nil || typ != "message" {
		add("invalid message type")
	}
	if json.Unmarshal(fields["role"], &role) != nil || role != "assistant" {
		add("expected assistant role")
	}
	out.Message.Role = minipi.Role(role)
	if json.Unmarshal(fields["stop_reason"], &stop) != nil {
		add("invalid stop reason")
	}
	switch stop {
	case "end_turn", "stop_sequence":
		out.StopReason = minipi.StopEnd
	case "tool_use":
		out.StopReason = minipi.StopToolUse
	case "max_tokens":
		out.StopReason = minipi.StopLength
	default:
		add("unsupported stop reason")
	}
	var blocks []json.RawMessage
	if json.Unmarshal(fields["content"], &blocks) != nil || len(blocks) == 0 {
		add("missing or invalid content")
	}
	seen := make(map[string]bool)
	var text strings.Builder
	for _, raw := range blocks {
		var block map[string]json.RawMessage
		if json.Unmarshal(raw, &block) != nil || block == nil {
			add("invalid content block")
			continue
		}
		var kind string
		if json.Unmarshal(block["type"], &kind) != nil {
			add("invalid block type")
			continue
		}
		switch kind {
		case "text":
			var value *string
			if json.Unmarshal(block["text"], &value) != nil || value == nil {
				add("invalid text block")
				continue
			}
			text.WriteString(*value)
		case "tool_use":
			var call minipi.ToolCall
			idErr := json.Unmarshal(block["id"], &call.ID)
			nameErr := json.Unmarshal(block["name"], &call.Name)
			call.Arguments = append(json.RawMessage(nil), block["input"]...)
			out.Message.ToolCalls = append(out.Message.ToolCalls, call)
			if idErr != nil || nameErr != nil || strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" || seen[call.ID] || !jsonObject(call.Arguments) {
				add("invalid tool call")
			}
			seen[call.ID] = true
		default:
			add("unsupported content block")
		}
	}
	out.Message.Content = text.String()
	if out.StopReason == minipi.StopEnd && len(out.Message.ToolCalls) > 0 || out.StopReason == minipi.StopToolUse && len(out.Message.ToolCalls) == 0 {
		add("stop reason conflicts with tool calls")
	}
	usage, err := decodeUsage(fields["usage"])
	out.Usage = usage
	return out, errors.Join(resultErr, err)
}

// Anthropic input_tokens excludes both cache counters. Count each once, not
// the nested cache_creation breakdown, and check every addition before adding.
func decodeUsage(raw json.RawMessage) (minipi.Usage, error) {
	var fields map[string]json.RawMessage
	bad := invalidResponse("missing, negative or overflowing token usage")
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return minipi.Usage{}, bad
	}
	read := func(key string, required bool) (int64, bool) {
		value, exists := fields[key]
		if !exists {
			return 0, !required
		}
		var n *int64
		if json.Unmarshal(value, &n) != nil || n == nil || *n < 0 {
			return 0, false
		}
		return *n, true
	}
	input, ok1 := read("input_tokens", true)
	output, ok2 := read("output_tokens", true)
	creation, ok3 := read("cache_creation_input_tokens", false)
	cached, ok4 := read("cache_read_input_tokens", false)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return minipi.Usage{}, bad
	}
	for _, n := range []int64{creation, cached} {
		if input > math.MaxInt64-n {
			return minipi.Usage{}, bad
		}
		input += n
	}
	if input > math.MaxInt64-output {
		return minipi.Usage{}, bad
	}
	return minipi.Usage{InputTokens: input, OutputTokens: output}, nil
}
