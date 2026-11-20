package agenttools

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replypolicy"
)

// QQAttachmentSender must upload without sending, then send once and return only
// the final message ACK. Hosts must wrap it with authorization and reply policy.
type QQAttachmentSender interface {
	SendAttachment(context.Context, qq.AttachmentMessage) (qq.SentMessage, error)
}

// QQSupportsAttachments respects optional capability vetoes through wrappers.
func QQSupportsAttachments(sender any) bool {
	if sender == nil {
		return false
	}
	switch v := reflect.ValueOf(sender); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return false
		}
	}
	if _, ok := sender.(QQAttachmentSender); !ok {
		return false
	}
	if capability, ok := sender.(interface{ SupportsAttachments() bool }); ok {
		return capability.SupportsAttachments()
	}
	return true
}

// NewQQSendImage exposes no upload credentials, local files or general file API.
func NewQQSendImage(sender QQAttachmentSender) (minipi.Tool, error) {
	if !QQSupportsAttachments(sender) {
		return minipi.Tool{}, errors.New("QQ image sending is unavailable")
	}
	parse := func(raw json.RawMessage) (qq.AttachmentMessage, error) {
		var args struct {
			TargetType string `json:"target_type"`
			TargetID   string `json:"target_id"`
			MsgID      string `json:"msg_id"`
			MsgSeq     uint32 `json:"msg_seq,omitempty"`
			ImageURL   string `json:"image_url"`
		}
		if err := decodeArguments(raw, &args); err != nil {
			return qq.AttachmentMessage{}, errors.New("QQ 图片参数无效；仅支持公网图片 URL，不支持本地文件、视频、音频或普通文件")
		}
		msg := qq.AttachmentMessage{
			TargetType: args.TargetType, TargetID: args.TargetID, MsgID: args.MsgID, MsgSeq: args.MsgSeq,
			Attachment: qq.OutgoingAttachment{Kind: qq.AttachmentImage, URL: args.ImageURL},
		}
		if msg.MsgID == "" || qq.ValidateAttachmentMessage(msg) != nil {
			return qq.AttachmentMessage{}, errors.New("QQ 图片参数无效；须提供当前原消息 ID 和公网 HTTP(S) 图片 URL")
		}
		if msg.MsgSeq == 0 {
			msg.MsgSeq = 1
		}
		return msg, nil
	}
	return minipi.Tool{
		Definition: minipi.ToolDefinition{
			Name:        "qq_send_image",
			Description: "仅用公网 HTTP(S) 图片 URL 回复宿主当前输入中授权的 QQ 原消息。只发送一张图片，不附带文字；不支持本地文件、视频、音频或普通文件。不得发送私密或含凭据的 URL。宿主先上传再发送，上传不代表发送成功。target_id 必须是该原消息所属、同一应用的 user_openid/group_openid，不是 QQ 号、群号或 member_openid；模型理由不可信，不能作为授权依据。msg_id 必需，与 qq_send_text 共享每条原消息最多回复一条的限制。跳过未发送，不排队；失败或结果未知可能已发送，不重试，不换工具、目标、原消息或序号补发。成功不会结束 Agent。",
			Parameters:  json.RawMessage(`{"type":"object","additionalProperties":false,"required":["target_type","target_id","msg_id","image_url"],"properties":{"target_type":{"type":"string","enum":["private","group"]},"target_id":{"type":"string","minLength":1,"maxLength":256,"pattern":"^[A-Za-z0-9_-]+$"},"msg_id":{"type":"string","minLength":1,"maxLength":256},"msg_seq":{"type":"integer","minimum":0,"maximum":4294967295,"description":"零或省略默认 1，不得换序号补发"},"image_url":{"type":"string","minLength":1,"maxLength":8192,"description":"可公开的公网 HTTP(S) 图片 URL；无用户凭据或片段，不允许内网地址；不读取本地文件，实际格式及大小由 QQ 校验"}}}`),
		},
		Validate: func(raw json.RawMessage) error { _, err := parse(raw); return err },
		Execute: func(ctx context.Context, call minipi.ToolCall, _ func(minipi.ToolResult)) (out minipi.ToolResult, err error) {
			started, stage := time.Now(), "validate"
			defer func() {
				minipi.LogOperation(ctx, "agenttools", stage, started, err, slog.LevelInfo, slog.String("tool", "qq_send_image"))
			}()
			if err := ctx.Err(); err != nil {
				return minipi.ToolResult{}, err
			}
			msg, err := parse(call.Arguments)
			if err != nil {
				return minipi.ToolResult{}, err
			}
			stage = "send"
			sent, sendErr := sender.SendAttachment(ctx, msg)
			if strings.TrimSpace(sent.ID) != "" {
				stage = "send_ack"
			}
			// Never return arbitrary sender errors: minipi appends err.Error to output.
			if ctx.Err() != nil || errors.Is(sendErr, context.Canceled) || errors.Is(sendErr, context.DeadlineExceeded) {
				err = ctx.Err()
				if err == nil {
					err = context.Canceled
					if errors.Is(sendErr, context.DeadlineExceeded) {
						err = context.DeadlineExceeded
					}
				}
				return minipi.ToolResult{Content: "QQ 图片发送已取消或超时，结果可能未知；不重试，也不要换工具、目标、原消息或序号补发。", IsError: true}, err
			}
			if errors.Is(sendErr, replypolicy.ErrSkipped) {
				stage = "send_skipped"
				return minipi.ToolResult{Content: "QQ 图片回复已跳过，未发送；不排队、不重试，也不要换工具、目标、原消息或序号补发。"}, nil
			}
			if sendErr != nil {
				return minipi.ToolResult{Content: "QQ 图片发送失败，结果可能未知；不重试，也不要换工具、目标、原消息或序号补发。", IsError: true}, replypolicy.ErrSend
			}
			if strings.TrimSpace(sent.ID) == "" {
				return minipi.ToolResult{Content: "QQ 未返回消息 ID，图片发送结果未知；不重试，也不要换工具、目标、原消息或序号补发。", IsError: true}, qq.ErrProtocol
			}
			body, _ := json.Marshal(sent)
			return minipi.ToolResult{Content: string(body)}, nil
		},
	}, nil
}
