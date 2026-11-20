package agenttools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replypolicy"
)

type imageSenderFunc func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error)

func (f imageSenderFunc) SendAttachment(ctx context.Context, m qq.AttachmentMessage) (qq.SentMessage, error) {
	return f(ctx, m)
}

type imageVeto struct{ imageSenderFunc }

func (imageVeto) SupportsAttachments() bool { return false }

const imageArgs = `{"target_type":"group","target_id":"private-target","msg_id":"private-source","image_url":"https://example.com/private-image.png"}`

func TestQQImageValidation(t *testing.T) {
	calls := 0
	sender := imageSenderFunc(func(_ context.Context, m qq.AttachmentMessage) (qq.SentMessage, error) {
		calls++
		if m.MsgSeq != 1 || m.Attachment.Kind != qq.AttachmentImage || m.Attachment.URL != "https://example.com/private-image.png" {
			t.Fatal("incorrect message mapping")
		}
		return qq.SentMessage{ID: "ack"}, nil
	})
	for _, absent := range []QQAttachmentSender{nil, imageSenderFunc(nil), imageVeto{sender}} {
		if _, err := NewQQSendImage(absent); err == nil {
			t.Fatal("unsupported sender accepted")
		}
	}
	tool, err := NewQQSendImage(sender)
	if err != nil {
		t.Fatal(err)
	}
	if tool.Definition.Name != "qq_send_image" || !json.Valid(tool.Definition.Parameters) {
		t.Fatal("incorrect definition")
	}
	for _, fragment := range []string{"qq_send_text", "原消息最多回复一条", "普通文件", "上传不代表发送成功", "不换工具"} {
		if !strings.Contains(tool.Definition.Description, fragment) {
			t.Fatal("missing host guidance", fragment)
		}
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, imageArgs + `{}`,
		strings.Replace(imageArgs, `"msg_id":"private-source",`, "", 1),
		strings.Replace(imageArgs, `"msg_id":"private-source"`, `"msg_id":null`, 1),
		strings.Replace(imageArgs, `"msg_id":"private-source"`, `"msg_id":"x","msg_id":"private-source"`, 1),
		strings.Replace(imageArgs, `"target_id"`, `"TARGET_ID"`, 1),
		strings.Replace(imageArgs, `"target_id":"private-target"`, `"target_id":{"private-secret":1}`, 1),
		strings.Replace(imageArgs, `https://example.com/private-image.png`, `file:///private-secret`, 1),
		strings.Replace(imageArgs, `https://example.com/private-image.png`, `http://127.0.0.1/private-secret`, 1),
		strings.Replace(imageArgs, `https://example.com/private-image.png`, `https://private-secret@example.com/image.png`, 1),
		strings.Replace(imageArgs, `"image_url"`, `"file_info"`, 1),
		strings.Replace(imageArgs, `"target_type"`, `"kind":"file","target_type"`, 1),
		strings.Replace(imageArgs, `"target_type"`, `"msg_seq":-1,"target_type"`, 1),
	} {
		if err := tool.Validate(json.RawMessage(raw)); err == nil || strings.Contains(err.Error(), "private-") {
			t.Fatal("unsafe validation result")
		}
		if _, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(raw)}, nil); err == nil || strings.Contains(err.Error(), "private-") {
			t.Fatal("unsafe execution validation")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(`broken`)}, nil); err != context.Canceled || calls != 0 {
		t.Fatal("cancellation or invalid input reached sender")
	}
	if err := tool.Validate(json.RawMessage(imageArgs)); err != nil {
		t.Fatal(err)
	}
	out, err := tool.Execute(context.Background(), minipi.ToolCall{Arguments: json.RawMessage(imageArgs)}, nil)
	if err != nil || out.IsError || out.Terminate || !strings.Contains(out.Content, `"id":"ack"`) || calls != 1 {
		t.Fatal("incorrect successful result", out, err)
	}
}

func TestQQImageOutcomes(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	secret := errors.New("private-error https://example.com/private-token")
	for _, tc := range []struct {
		name, ack string
		failure   error
		want      error
		cancel    bool
	}{
		{"skip", "", errors.Join(replypolicy.ErrSkipped, secret), nil, false},
		{"unknown", "", secret, replypolicy.ErrSend, false},
		{"missing-ack", "", nil, qq.ErrProtocol, false},
		{"blank-ack", " ", nil, qq.ErrProtocol, false},
		{"ack-storage", "ack", errors.Join(replypolicy.ErrStorage, secret), replypolicy.ErrSend, false},
		{"canceled", "", errors.Join(context.Canceled, secret), context.Canceled, false},
		{"timeout", "", errors.Join(context.DeadlineExceeded, secret), context.DeadlineExceeded, false},
		{"parent-canceled-skip", "", errors.Join(replypolicy.ErrSkipped, secret), context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			tool, err := NewQQSendImage(imageSenderFunc(func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error) {
				calls++
				if tc.cancel {
					cancel()
				}
				return qq.SentMessage{ID: tc.ack}, tc.failure
			}))
			if err != nil {
				t.Fatal(err)
			}
			out, err := tool.Execute(ctx, minipi.ToolCall{Arguments: json.RawMessage(imageArgs)}, nil)
			if err != tc.want || out.IsError != (tc.want != nil) || out.Terminate || calls != 1 {
				t.Fatal("incorrect send outcome", out, err)
			}
			text := out.Content + logs.String()
			if err != nil {
				text += err.Error()
			}
			if strings.Contains(text, "private-") || !strings.Contains(out.Content, "不重试") {
				t.Fatal("sensitive output or missing no-retry guidance")
			}
		})
	}
}
