package modelapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"

	"ojj/internal/minipi"
)

type openAIFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAICall struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIMessage struct {
	Role       string       `json:"role"`
	Content    any          `json:"content"`
	ToolCalls  []openAICall `json:"tool_calls,omitempty"`
	ToolCallID string       `json:"tool_call_id,omitempty"`
}

type openAIContentPart struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ImageURL *openAIImageURL `json:"image_url,omitempty"`
}

type openAIImageURL struct {
	URL string `json:"url"`
}

type openAITool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

type openAIRequest struct {
	Model               string          `json:"model"`
	Messages            []openAIMessage `json:"messages"`
	Tools               []openAITool    `json:"tools,omitempty"`
	MaxTokens           int64           `json:"max_tokens,omitempty"`
	MaxCompletionTokens int64           `json:"max_completion_tokens,omitempty"`
	Stream              bool            `json:"stream"`
}

func (c *Client) encodeOpenAIRequest(in minipi.Request, images *preparedImages) ([]byte, error) {
	bad := errors.New("modelapi: invalid message or tool history")
	if in.MaxTokens < 0 {
		return nil, errors.New("modelapi: negative token budget")
	}
	if len(in.Messages) == 0 {
		return nil, bad
	}
	limit := int64(c.maxOutputTokens)
	if in.MaxTokens > 0 && in.MaxTokens < limit {
		limit = in.MaxTokens
	}
	out := openAIRequest{Model: c.model}
	if c.openAITokenLimitField == "max_completion_tokens" {
		out.MaxCompletionTokens = limit
	} else {
		out.MaxTokens = limit
	}
	if in.SystemPrompt != "" {
		out.Messages = append(out.Messages, openAIMessage{Role: "system", Content: &in.SystemPrompt})
	}
	seen, pending := make(map[string]bool), make(map[string]bool)
	for messageIndex, m := range in.Messages {
		if minipi.ValidateImages(m.Role, m.Images) != nil {
			return nil, bad
		}
		if len(pending) > 0 && m.Role != minipi.RoleTool {
			return nil, bad
		}
		content := m.Content
		msg := openAIMessage{Role: string(m.Role), Content: &content}
		switch m.Role {
		case minipi.RoleUser, minipi.RoleAssistant:
			if m.ToolCallID != "" || m.IsError || m.Role == minipi.RoleUser && len(m.ToolCalls) > 0 {
				return nil, bad
			}
			if content == "" && len(m.ToolCalls) == 0 && len(m.Images) == 0 {
				return nil, bad
			}
			for _, call := range m.ToolCalls {
				if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" || seen[call.ID] || !jsonObject(call.Arguments) {
					return nil, bad
				}
				seen[call.ID], pending[call.ID] = true, true
				msg.ToolCalls = append(msg.ToolCalls, openAICall{ID: call.ID, Type: "function", Function: openAIFunction{Name: call.Name, Arguments: string(call.Arguments)}})
			}
			if content == "" {
				msg.Content = nil
			}
			if len(m.Images) > 0 {
				var parts []openAIContentPart
				if content != "" {
					parts = append(parts, openAIContentPart{Type: "text", Text: content})
				}
				for imageIndex, image := range m.Images {
					if image.Label != "" {
						parts = append(parts, openAIContentPart{Type: "text", Text: image.Label})
					}
					if source := images.source(messageIndex, imageIndex); source != nil {
						parts = append(parts, openAIContentPart{Type: "image_url", ImageURL: &openAIImageURL{URL: "data:" + source.MediaType + ";base64," + source.Data}})
					} else {
						parts = append(parts, openAIContentPart{Type: "text", Text: unavailableImage})
					}
				}
				msg.Content = parts
			}
		case minipi.RoleTool:
			if len(m.ToolCalls) > 0 || !pending[m.ToolCallID] {
				return nil, bad
			}
			delete(pending, m.ToolCallID)
			msg.ToolCallID = m.ToolCallID
			// Chat Completions has no is_error field; retain the status in content.
			if m.IsError {
				content = "Tool execution failed:\n" + content
			}
		default:
			return nil, bad
		}
		out.Messages = append(out.Messages, msg)
	}
	if len(pending) != 0 {
		return nil, bad
	}
	names := make(map[string]bool)
	for _, tool := range in.Tools {
		if strings.TrimSpace(tool.Name) == "" || names[tool.Name] || !jsonObject(tool.Parameters) {
			return nil, bad
		}
		names[tool.Name] = true
		def := openAITool{Type: "function"}
		def.Function.Name, def.Function.Description, def.Function.Parameters = tool.Name, tool.Description, tool.Parameters
		out.Tools = append(out.Tools, def)
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, bad
	}
	return data, nil
}

func decodeOpenAIResponse(data []byte) (minipi.Response, error) {
	var out minipi.Response
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return out, invalidResponse("invalid JSON completion")
	}
	var resultErr error
	add := func(reason string) { resultErr = errors.Join(resultErr, invalidResponse(reason)) }
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		add("expected one complete JSON value")
	}
	var choices []map[string]json.RawMessage
	if json.Unmarshal(fields["choices"], &choices) != nil || len(choices) != 1 || choices[0] == nil {
		add("expected exactly one completion choice")
	} else {
		choice := choices[0]
		var stop string
		if json.Unmarshal(choice["finish_reason"], &stop) != nil {
			add("invalid finish reason")
		}
		switch stop {
		case "stop":
			out.StopReason = minipi.StopEnd
		case "tool_calls":
			out.StopReason = minipi.StopToolUse
		case "length":
			out.StopReason = minipi.StopLength
		default:
			add("unsupported finish reason")
		}
		var message map[string]json.RawMessage
		if json.Unmarshal(choice["message"], &message) != nil || message == nil {
			add("missing assistant message")
		} else {
			var role string
			if json.Unmarshal(message["role"], &role) != nil || role != "assistant" {
				add("expected assistant role")
			}
			out.Message.Role = minipi.Role(role)
			var content *string
			if raw, ok := message["content"]; ok && json.Unmarshal(raw, &content) != nil {
				add("invalid text content")
			}
			if content != nil {
				out.Message.Content = *content
			}
			// Refusals can carry text instead of content. Never execute legacy calls.
			if raw, ok := message["refusal"]; ok {
				var refusal *string
				if json.Unmarshal(raw, &refusal) != nil {
					add("invalid refusal")
				} else if refusal != nil && out.Message.Content == "" {
					out.Message.Content = *refusal
				}
			}
			if raw, ok := message["function_call"]; ok && string(bytes.TrimSpace(raw)) != "null" {
				add("legacy function calls are unsupported")
			}
			var calls []json.RawMessage
			if raw, ok := message["tool_calls"]; ok && json.Unmarshal(raw, &calls) != nil {
				add("invalid tool calls")
			}
			seen := make(map[string]bool)
			for _, raw := range calls {
				var call openAICall
				err := json.Unmarshal(raw, &call)
				out.Message.ToolCalls = append(out.Message.ToolCalls, minipi.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: json.RawMessage(call.Function.Arguments)})
				if err != nil || call.Type != "function" || strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Function.Name) == "" || seen[call.ID] || !jsonObject(json.RawMessage(call.Function.Arguments)) {
					add("invalid tool call")
				}
				seen[call.ID] = true
			}
			if content == nil && out.Message.Content == "" && len(calls) == 0 {
				add("missing text or tool calls")
			}
		}
	}
	if out.StopReason == minipi.StopEnd && len(out.Message.ToolCalls) > 0 || out.StopReason == minipi.StopToolUse && len(out.Message.ToolCalls) == 0 {
		add("finish reason conflicts with tool calls")
	}
	usage, err := decodeOpenAIUsage(fields["usage"])
	out.Usage = usage
	return out, errors.Join(resultErr, err)
}

func decodeOpenAIUsage(raw json.RawMessage) (minipi.Usage, error) {
	var usage struct {
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
	}
	if json.Unmarshal(raw, &usage) != nil || usage.Prompt == nil || usage.Completion == nil || *usage.Prompt < 0 || *usage.Completion < 0 || *usage.Prompt > math.MaxInt64-*usage.Completion {
		return minipi.Usage{}, invalidResponse("missing, negative or overflowing token usage")
	}
	// Prompt/completion counters already include cached/reasoning tokens.
	return minipi.Usage{InputTokens: *usage.Prompt, OutputTokens: *usage.Completion}, nil
}
