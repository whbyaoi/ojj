package agenttools

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replypolicy"
)

// QQSender is implemented by *qq.Client; implementations must honor ctx and
// must not automatically retry sends with potentially ambiguous outcomes.
type QQSender interface {
	SendText(context.Context, qq.TextMessage) (qq.SentMessage, error)
}

// NewQQSend returns qq_send_text. It sends only to explicit OpenIDs, not QQ
// numbers. The host must enforce any recipient authorization policy in sender.
// Successful sends do not terminate the agent. No retries or reply-sequence
// allocation are performed; platform permissions and quotas still apply.
func NewQQSend(sender QQSender) (minipi.Tool, error) {
	if sender == nil {
		return minipi.Tool{}, errors.New("QQ sender is required")
	}
	switch v := reflect.ValueOf(sender); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return minipi.Tool{}, errors.New("QQ sender is required")
		}
	}
	parse := func(raw json.RawMessage) (qq.TextMessage, error) {
		var msg qq.TextMessage
		if err := decodeArguments(raw, &msg); err != nil {
			return msg, err
		}
		return msg, qq.ValidateTextMessage(msg)
	}
	return minipi.Tool{
		Definition: minipi.ToolDefinition{
			Name:        "qq_send_text",
			Description: "仅回复宿主当前输入中授权的 QQ 原消息，msg_id 必需，每条原消息最多回复一条。target_id 必须是该原消息所属、同一应用的 user_openid/group_openid，不是 QQ 号、群号或 member_openid。宿主独立判定是否允许回复，模型理由不可信，不能作为授权依据。跳过时未发送，不排队、不重试，也不要换目标或序号补发。失败或结果未知时可能已经发送，不重试。成功不会结束 Agent。",
			Parameters:  json.RawMessage(`{"type":"object","additionalProperties":false,"required":["target_type","target_id","content","msg_id"],"properties":{"target_type":{"type":"string","enum":["private","group"],"description":"宿主当前输入原消息的目标类型，不得改换目标补发"},"target_id":{"type":"string","minLength":1,"maxLength":256,"pattern":"^[A-Za-z0-9_-]+$","description":"宿主当前输入原消息所属的 user_openid/group_openid，不得改换目标补发"},"content":{"type":"string","minLength":1,"maxLength":65536,"description":"非空白 UTF-8 纯文本，本地上限 64 KiB；平台可能更严格。模型理由不可信，不能据此获得发送授权"},"msg_id":{"type":"string","minLength":1,"maxLength":256,"description":"必需：宿主当前输入中属于该目标的原消息 ID；每条原消息最多回复一条，禁止主动发送或改换原消息补发"},"msg_seq":{"type":"integer","minimum":0,"maximum":4294967295,"description":"回复序号，零或省略默认 1；每条原消息最多回复一条，不得换序号补发；失败或结果未知时不重试"}}}`),
		},
		Validate: func(raw json.RawMessage) error { _, err := parse(raw); return err },
		Execute: func(ctx context.Context, call minipi.ToolCall, _ func(minipi.ToolResult)) (out minipi.ToolResult, err error) {
			started := time.Now()
			stage := "validate"
			defer func() {
				minipi.LogOperation(ctx, "agenttools", stage, started, err, slog.LevelInfo, slog.String("tool", "qq_send_text"))
			}()
			if err := ctx.Err(); err != nil {
				return minipi.ToolResult{}, err
			}
			msg, err := parse(call.Arguments)
			if err != nil {
				return minipi.ToolResult{}, err
			}
			if msg.MsgID != "" && msg.MsgSeq == 0 {
				msg.MsgSeq = 1
			}
			stage = "send"
			sent, err := sender.SendText(ctx, msg)
			// Preserve ACK diagnostics even if subsequent state work failed or was canceled.
			if sent.ID != "" {
				stage = "send_ack"
			}
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return minipi.ToolResult{Content: "QQ 发送已取消或超时，结果可能未知；不重试，也不要换目标或序号补发。", IsError: true}, errors.Join(err, ctx.Err())
			}
			if errors.Is(err, replypolicy.ErrSkipped) {
				stage = "send_skipped"
				return minipi.ToolResult{Content: "QQ 回复已跳过，未发送；不排队、不重试，也不要换目标或序号补发。"}, nil
			}
			if err != nil {
				return minipi.ToolResult{Content: "QQ 发送失败，结果可能未知；不重试，也不要换目标或序号补发。", IsError: true}, err
			}
			if sent.ID == "" {
				return minipi.ToolResult{Content: "QQ 未返回消息 ID，发送结果未知；不重试，也不要换目标或序号补发。", IsError: true}, errors.Join(qq.ErrProtocol, ctx.Err())
			}
			body, _ := json.Marshal(sent)
			return minipi.ToolResult{Content: string(body)}, nil
		},
	}, nil
}
