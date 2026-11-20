package qq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"ojj/internal/event"
)

var ErrMessage = errors.New("qq: invalid or oversized message")

// Publisher 成功返回必须表示事件已经可靠提交，而非仅进入内存队列。
// 实现必须响应 context 取消；退出会等待在途发布结束，避免新旧会话重叠发布。
type Publisher interface {
	Publish(context.Context, event.Input) (event.PublishResult, error)
}

type User struct {
	ID           string `json:"id,omitempty"`
	Username     string `json:"username,omitempty"`
	Bot          bool   `json:"bot,omitempty"`
	UserOpenID   string `json:"user_openid,omitempty"`
	MemberOpenID string `json:"member_openid,omitempty"`
	MemberRole   string `json:"member_role,omitempty"`
}

type Attachment struct {
	URL          string `json:"url,omitempty"`
	Filename     string `json:"filename,omitempty"`
	Width        int64  `json:"width,omitempty"`
	Height       int64  `json:"height,omitempty"`
	Size         int64  `json:"size,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	VoiceWAVURL  string `json:"voice_wav_url,omitempty"`
	ASRReferText string `json:"asr_refer_text,omitempty"`
}

type ARK struct {
	Prompt string         `json:"prompt,omitempty"`
	Type   string         `json:"ark_type,omitempty"`
	Name   string         `json:"ark_name,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}

type Element struct {
	Index       string       `json:"msg_idx,omitempty"`
	Author      User         `json:"author"`
	MessageType int          `json:"message_type"`
	Content     string       `json:"content"`
	Attachments []Attachment `json:"attachments,omitempty"`
	ARK         *ARK         `json:"ark_data,omitempty"`
	Elements    []Element    `json:"msg_elements,omitempty"`
}

type message struct {
	ID          string          `json:"id"`
	Author      User            `json:"author"`
	Content     string          `json:"content"`
	Timestamp   time.Time       `json:"timestamp"`
	GroupOpenID string          `json:"group_openid"`
	MessageType int             `json:"message_type"`
	MessageSeq  json.RawMessage `json:"msg_seq"`
	Scene       struct {
		Source string   `json:"source"`
		Ext    []string `json:"ext"`
	} `json:"message_scene"`
	Attachments []Attachment `json:"attachments"`
	Mentions    []User       `json:"mentions"`
	ARK         *ARK         `json:"ark_data"`
	Elements    []Element    `json:"msg_elements"`
}

// Content 是 im.message 正文中的版本化 JSON 文本；不用于路由。
// 原文不做 TrimSpace，也不重新裁剪群 @ 前缀。URL 仅保存，不下载或执行。
type Content struct {
	Version        int          `json:"version"`
	QQEvent        string       `json:"qq_event"`
	MessageType    int          `json:"message_type"`
	Text           string       `json:"text"`
	Author         User         `json:"author"`
	MessageIndex   string       `json:"msg_idx,omitempty"`
	ReferenceIndex string       `json:"ref_msg_idx,omitempty"`
	Scene          string       `json:"scene,omitempty"`
	Attachments    []Attachment `json:"attachments,omitempty"`
	Mentions       []User       `json:"mentions,omitempty"`
	ARK            *ARK         `json:"ark_data,omitempty"`
	Elements       []Element    `json:"msg_elements,omitempty"`
}

func supported(t string) bool {
	return t == "C2C_MESSAGE_CREATE" || t == "GROUP_AT_MESSAGE_CREATE" || t == "GROUP_MESSAGE_CREATE"
}

func validID(s string) bool {
	return strings.TrimSpace(s) != "" && len(s) <= 256 && utf8.ValidString(s)
}

// Convert 仅接受来自已鉴权接入层的消息；account 必须由配置提供。
// 去重隔离账号、事件类型、会话、发送者、消息 ID，优先 msg_idx，回退 msg_seq。
// 两者皆无时使用消息 ID；同键内容变化由事件存储报告冲突，不静默覆盖。
func Convert(account, kind string, raw []byte) (event.Input, error) {
	var out event.Input
	if !validID(account) || !supported(kind) || len(raw) > 1<<20 || !utf8.Valid(raw) {
		return out, ErrMessage
	}
	var m message
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if !json.Valid(raw) || decoder.Decode(&m) != nil || !validID(m.ID) || m.Timestamp.IsZero() {
		return out, ErrMessage
	}
	sender, conversation, ct := m.Author.UserOpenID, m.Author.UserOpenID, "private"
	if kind != "C2C_MESSAGE_CREATE" {
		sender, conversation, ct = m.Author.MemberOpenID, m.GroupOpenID, "group"
	}
	if !validID(sender) || !validID(conversation) {
		return out, ErrMessage
	}
	indexes := map[string]string{}
	for _, ext := range m.Scene.Ext {
		k, v, ok := strings.Cut(ext, "=")
		if !ok || (k != "msg_idx" && k != "ref_msg_idx") {
			continue
		}
		if v == "" {
			return out, ErrMessage
		}
		if previous, found := indexes[k]; found && previous != v {
			return out, ErrMessage
		}
		indexes[k] = v
	}
	discriminator := "id"
	if idx := indexes["msg_idx"]; idx != "" {
		discriminator = "idx:" + idx
	} else if len(m.MessageSeq) != 0 && string(m.MessageSeq) != "null" {
		var seq string
		if json.Unmarshal(m.MessageSeq, &seq) != nil {
			var n json.Number
			if json.Unmarshal(m.MessageSeq, &n) != nil {
				return out, ErrMessage
			}
			seq = n.String()
		}
		if strings.TrimSpace(seq) == "" {
			return out, ErrMessage
		}
		discriminator = "seq:" + seq
	}
	count := 0
	if !sanitizeElements(m.Elements, 0, &count) {
		return out, ErrMessage
	}
	sanitizeARK(m.ARK)
	body := Content{Version: 1, QQEvent: kind, MessageType: m.MessageType,
		Text: m.Content, Author: m.Author, MessageIndex: indexes["msg_idx"],
		ReferenceIndex: indexes["ref_msg_idx"], Scene: m.Scene.Source,
		Attachments: m.Attachments, Mentions: m.Mentions, ARK: m.ARK, Elements: m.Elements}
	text, err := json.Marshal(body)
	if err != nil || len(text) > 64<<10 {
		return out, ErrMessage
	}
	key, _ := json.Marshal([]string{account, kind, ct, conversation, sender, m.ID, discriminator})
	sum := sha256.Sum256(key)
	out = event.Input{EventType: "im.message", OccurredAt: m.Timestamp.UTC(), Content: string(text),
		SchemaVersion: 1, DeduplicationKey: "qq:v1:" + hex.EncodeToString(sum[:]),
		Source: &event.Source{Platform: "qq", AccountID: account, ConversationType: ct,
			ConversationID: conversation, SenderID: sender, MessageID: m.ID}}
	return out, nil
}

func sanitizeElements(elements []Element, depth int, count *int) bool {
	if depth > 16 {
		return false
	}
	for i := range elements {
		*count++
		if *count > 512 {
			return false
		}
		sanitizeARK(elements[i].ARK)
		if !sanitizeElements(elements[i].Elements, depth+1, count) {
			return false
		}
	}
	return true
}

func sanitizeARK(a *ARK) {
	if a != nil {
		redact(a.Fields)
	}
}

// 扩展字段只提取索引，不保留 auth_token；卡片自由字段递归过滤凭据键。
// 不尝试删改用户正文，也不保证识别正文或 URL 中用户自行携带的秘密。
func redact(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, value := range x {
			switch strings.ToLower(k) {
			case "auth_token", "access_token", "clientsecret", "client_secret", "authorization":
				delete(x, k)
			default:
				redact(value)
			}
		}
	case []any:
		for _, item := range x {
			redact(item)
		}
	}
}
