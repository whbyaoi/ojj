package modelapi

import (
	"encoding/json"
	"errors"
	"strings"

	"ojj/internal/minipi"
)

type imageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

type requestBlock struct {
	Source    *imageSource    `json:"source,omitempty"`
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   *string         `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}

type requestMessage struct {
	Role    minipi.Role    `json:"role"`
	Content []requestBlock `json:"content"`
}

type requestTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type wireRequest struct {
	Model     string           `json:"model"`
	System    string           `json:"system,omitempty"`
	MaxTokens int64            `json:"max_tokens"`
	Stream    bool             `json:"stream"`
	Messages  []requestMessage `json:"messages"`
	Tools     []requestTool    `json:"tools,omitempty"`
}

func (c *Client) encodeRequest(in minipi.Request) ([]byte, error) {
	return c.encodePrepared(in, nil)
}

func (c *Client) encodePrepared(in minipi.Request, images *preparedImages) ([]byte, error) {
	if c.protocol == ProtocolOpenAI {
		return c.encodeOpenAIRequest(in, images)
	}
	bad := errors.New("modelapi: invalid message or tool history")
	if in.MaxTokens < 0 {
		return nil, errors.New("modelapi: negative token budget")
	}
	out := wireRequest{Model: c.model, System: in.SystemPrompt, MaxTokens: int64(c.maxOutputTokens)}
	if in.MaxTokens > 0 && in.MaxTokens < out.MaxTokens {
		out.MaxTokens = in.MaxTokens
	}
	if len(in.Messages) == 0 {
		return nil, bad
	}
	seen := make(map[string]bool)
	pending := make(map[string]bool)
	for messageIndex, m := range in.Messages {
		if minipi.ValidateImages(m.Role, m.Images) != nil {
			return nil, bad
		}
		role := m.Role
		var blocks []requestBlock
		if len(pending) > 0 && role != minipi.RoleTool {
			return nil, bad
		}
		switch role {
		case minipi.RoleUser, minipi.RoleAssistant:
			if m.ToolCallID != "" || m.IsError || role == minipi.RoleUser && len(m.ToolCalls) > 0 {
				return nil, bad
			}
			if m.Content != "" {
				blocks = append(blocks, requestBlock{Type: "text", Text: m.Content})
			}
			for imageIndex, image := range m.Images {
				if image.Label != "" {
					blocks = append(blocks, requestBlock{Type: "text", Text: image.Label})
				}
				if source := images.source(messageIndex, imageIndex); source != nil {
					blocks = append(blocks, requestBlock{Type: "image", Source: source})
				} else {
					blocks = append(blocks, requestBlock{Type: "text", Text: unavailableImage})
				}
			}
			for _, call := range m.ToolCalls {
				if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Name) == "" || seen[call.ID] || !jsonObject(call.Arguments) {
					return nil, bad
				}
				seen[call.ID], pending[call.ID] = true, true
				blocks = append(blocks, requestBlock{Type: "tool_use", ID: call.ID, Name: call.Name, Input: call.Arguments})
			}
			if len(blocks) == 0 {
				return nil, bad
			}
		case minipi.RoleTool:
			if len(m.ToolCalls) > 0 || !pending[m.ToolCallID] {
				return nil, bad
			}
			delete(pending, m.ToolCallID)
			role = minipi.RoleUser
			content := m.Content
			blocks = append(blocks, requestBlock{Type: "tool_result", ToolUseID: m.ToolCallID, Content: &content, IsError: m.IsError})
		default:
			return nil, bad
		}
		last := len(out.Messages) - 1
		if last >= 0 && out.Messages[last].Role == role {
			out.Messages[last].Content = append(out.Messages[last].Content, blocks...)
		} else {
			out.Messages = append(out.Messages, requestMessage{Role: role, Content: blocks})
		}
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
		out.Tools = append(out.Tools, requestTool{Name: tool.Name, Description: tool.Description, InputSchema: tool.Parameters})
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, bad
	}
	return data, nil
}
