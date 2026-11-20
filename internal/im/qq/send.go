package qq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

// TextMessage 仅发送纯文本。TargetType 为 private 或 group，TargetID 必须是
// 同一 AppID 下的 user_openid 或 group_openid，不是 QQ 号、群号或 member_openid。
// MsgID 为空表示主动发送，权限、额度和审核由 QQ 平台决定；非空表示被动回复，
// 必须属于该目标且仍在有效回复窗口。同一 MsgID 多次回复应使用不同 MsgSeq；
// 调用者重试同一条回复应保持序号不变。本包不分配或持久化序号。
type TextMessage struct {
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Content    string `json:"content"`
	MsgID      string `json:"msg_id,omitempty"`
	MsgSeq     uint32 `json:"msg_seq,omitempty"`
}

// SentMessage 是平台确认的发送结果，不保证对端已阅读。
type SentMessage struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp,omitempty"`
}

// ValidateTextMessage 在发出任何网络请求前校验参数。64 KiB 是本地保护上限，
// 不是平台承诺的文本长度；平台可能施加更严格的限制。
func ValidateTextMessage(m TextMessage) error {
	if m.TargetType != "private" && m.TargetType != "group" {
		return errors.New("qq: target_type must be private or group")
	}
	if !validID(m.TargetID) || strings.IndexFunc(m.TargetID, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) >= 0 {
		return errors.New("qq: invalid target OpenID")
	}
	if strings.TrimSpace(m.Content) == "" || len(m.Content) > 64<<10 || !utf8.ValidString(m.Content) {
		return errors.New("qq: content must be nonempty UTF-8, at most 64 KiB")
	}
	if m.MsgID != "" && !validID(m.MsgID) {
		return errors.New("qq: invalid msg_id")
	}
	if m.MsgID == "" && m.MsgSeq != 0 {
		return errors.New("qq: msg_seq requires msg_id")
	}
	return nil
}

// SendText 可与 Run 并发使用；无需先建立 WebSocket 连接。
// 不自动重试 POST（包括 401），网络失败/取消/异常响应可能意味着发送结果未知，
// 不应盲目重发。成功也不结束 Agent 运行，终止策略由上层决定。
func (c *Client) SendText(ctx context.Context, m TextMessage) (_ SentMessage, retErr error) {
	if err := ValidateTextMessage(m); err != nil {
		c.logger().Warn("QQ send rejected", "failure", "invalid_input")
		return SentMessage{}, err
	}
	started := time.Now()
	logger := c.logger().With("target_type", m.TargetType, "passive_reply", m.MsgID != "")
	defer func() {
		if retErr != nil {
			logFailure(logger, slog.LevelWarn, "send", retErr, "duration", time.Since(started))
		} else {
			logger.Info("QQ message sent", "duration", time.Since(started))
		}
	}()
	if m.MsgID != "" && m.MsgSeq == 0 {
		m.MsgSeq = 1
	}
	body, _ := json.Marshal(struct {
		Content string `json:"content"`
		MsgType int    `json:"msg_type"`
		MsgID   string `json:"msg_id,omitempty"`
		MsgSeq  uint32 `json:"msg_seq,omitempty"`
	}{m.Content, 0, m.MsgID, m.MsgSeq})
	token, err := c.accessToken(ctx)
	if err != nil {
		return SentMessage{}, err
	}
	resource := "users"
	if m.TargetType == "group" {
		resource = "groups"
	}
	var result SentMessage
	err = c.request(ctx, http.MethodPost, "/v2/"+resource+"/"+m.TargetID+"/messages", token, bytes.NewReader(body), &result)
	if err != nil {
		return SentMessage{}, errors.Join(err, ctx.Err())
	}
	if !validID(result.ID) {
		return SentMessage{}, errors.Join(ErrProtocol, ctx.Err())
	}
	return result, nil
}
