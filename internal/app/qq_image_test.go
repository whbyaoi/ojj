package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"ojj/internal/agenttools"
	"ojj/internal/im/qq"
	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
	"ojj/internal/replypolicy"
)

type appImageSender struct {
	checkedQQSender
	image func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error)
}

func (s *appImageSender) SendAttachment(ctx context.Context, msg qq.AttachmentMessage) (qq.SentMessage, error) {
	return s.image(ctx, msg)
}

type appImageVeto struct{ *appImageSender }

func (appImageVeto) SupportsAttachments() bool { return false }

func executeAppImage(t *testing.T, tool minipi.Tool, ctx context.Context, kind, target, id string) (minipi.ToolResult, error) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"target_type": kind, "target_id": target, "msg_id": id,
		"image_url": "https://example.com/image.png",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tool.Validate(raw); err != nil {
		t.Fatal(err)
	}
	return tool.Execute(ctx, minipi.ToolCall{Name: "qq_send_image", Arguments: raw}, nil)
}

func TestAppImageRegistrationAndAuthorization(t *testing.T) {
	brainTestEnv(t)
	calls := 0
	sender := &appImageSender{image: func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error) {
		calls++
		return qq.SentMessage{ID: "ack"}, nil
	}}
	for _, restricted := range []bool{false, true} {
		cfg, _, err := mainBrainConfig("unused.db")
		if err != nil {
			t.Fatal(err)
		}
		allowed := map[string]struct{}{}
		if restricted {
			allowed["private:user_1"] = struct{}{}
		}
		if err := attachQQTool(&cfg, sender, allowed); err != nil {
			t.Fatal(err)
		}
		if len(cfg.Agent.Tools) != 2 || cfg.Agent.Tools[0].Definition.Name != "qq_send_text" || cfg.Agent.Tools[1].Definition.Name != "qq_send_image" {
			t.Fatal("incorrect image registration or text ordering")
		}
		allowed["group:user_1"] = struct{}{}
		for _, target := range []struct{ kind, id string }{{"private", "user_1"}, {"group", "user_1"}, {"private", "other"}} {
			before := calls
			_, err := executeAppImage(t, cfg.Agent.Tools[1], context.Background(), target.kind, target.id, "source")
			want := !restricted || target.kind == "private" && target.id == "user_1"
			if (err == nil) != want || (calls == before+1) != want {
				t.Fatal("allowlist must be exact and copied")
			}
		}
		before := calls
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := executeAppImage(t, cfg.Agent.Tools[1], ctx, "private", "user_1", "source"); err != context.Canceled || calls != before {
			t.Fatal("canceled image reached transport")
		}
	}
	for _, s := range []agenttools.QQSender{&brainTestSender{}, appImageVeto{sender}} {
		cfg, _, err := mainBrainConfig("unused.db")
		if err != nil {
			t.Fatal(err)
		}
		if err := attachQQTool(&cfg, s, nil); err != nil || len(cfg.Agent.Tools) != 1 {
			t.Fatal("unsupported image tool registered", err)
		}
		if _, err := (authorizedQQSender{sender: s}).SendAttachment(context.Background(), qq.AttachmentMessage{}); err != qq.ErrUnsupportedAttachment {
			t.Fatal("unsupported transport accepted")
		}
	}
	cfg, _, err := mainBrainConfig("unused.db")
	if err != nil {
		t.Fatal(err)
	}
	image, err := agenttools.NewQQSendImage(sender)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Agent.Tools = []minipi.Tool{image}
	if err := attachQQTool(&cfg, sender, nil); err == nil || len(cfg.Agent.Tools) != 1 {
		t.Fatal("duplicate registration was not atomic")
	}
	var absent *appImageSender
	if err := attachQQTool(&cfg, absent, nil); err == nil || len(cfg.Agent.Tools) != 1 {
		t.Fatal("typed nil accepted")
	}
}

func installAppImageSender(t *testing.T, f *replyIntegrationFixture, sender *appImageSender) minipi.Tool {
	t.Helper()
	if err := f.policy.Close(); err != nil {
		t.Fatal(err)
	}
	f.policy = nil
	cfg, allowed, err := mainBrainConfig(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.policy, err = attachReplyPolicy(f.ctx, &cfg, f.builder, f.pc, sender, allowed)
	if err != nil {
		t.Fatal(err)
	}
	f.cfg = cfg
	var image minipi.Tool
	for _, tool := range cfg.Agent.Tools {
		switch tool.Definition.Name {
		case "qq_send_text":
			f.tool = tool
		case "qq_send_image":
			image = tool
		}
	}
	if image.Execute == nil {
		t.Fatal("image tool missing from production assembly")
	}
	return image
}

func TestAppImagePersistentIdempotency(t *testing.T) {
	f := newReplyIntegrationFixture(t, 12, nil)
	calls := 0
	sender := &appImageSender{checkedQQSender: f.sender, image: func(context.Context, qq.AttachmentMessage) (qq.SentMessage, error) {
		calls++
		return qq.SentMessage{ID: "image-ack"}, nil
	}}
	image := installAppImageSender(t, f, sender)
	product := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "source", "请回复图片")
	if out, err := executeAppImage(t, image, f.bind(f.ctx, product), "group", "group_1", "source"); err != nil || out.IsError || calls != 1 {
		t.Fatal("initial image failed", err)
	}
	image = installAppImageSender(t, f, sender)
	ctx := f.bind(f.ctx, product)
	f.skipped(ctx, "group_1", "source", 99)
	out, err := executeAppImage(t, image, ctx, "group", "group_1", "source")
	if err != nil || !strings.Contains(out.Content, "未发送") || calls != 1 || replydelivery.Outcome(ctx) != replydelivery.NotSent {
		t.Fatal("reopening lost shared image/text idempotency")
	}
	// Budget remains available: rejection above must be message idempotency.
	next := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "next", "新的问题")
	f.sent(f.bind(f.ctx, next), "group_1", "next")
}

func TestAppImageProductionPolicy(t *testing.T) {
	for _, name := range []string{"image-first", "text-first", "upload-failed", "missing-ack"} {
		t.Run(name, func(t *testing.T) {
			f := newReplyIntegrationFixture(t, 1, nil)
			for _, tool := range f.cfg.Agent.Tools {
				if tool.Definition.Name == "qq_send_image" {
					t.Fatal("text-only policy incorrectly advertises images")
				}
			}
			imageCalls := 0
			sender := &appImageSender{checkedQQSender: f.sender, image: func(ctx context.Context, msg qq.AttachmentMessage) (qq.SentMessage, error) {
				imageCalls++
				if replydelivery.Outcome(ctx) != replydelivery.Unknown || msg.MsgSeq != 1 || msg.Attachment.Kind != qq.AttachmentImage {
					t.Error("upload must start after reservation, without delivery confirmation")
				}
				if name == "upload-failed" {
					return qq.SentMessage{}, errors.New("synthetic-private-error")
				}
				if name == "missing-ack" {
					return qq.SentMessage{}, nil
				}
				return qq.SentMessage{ID: "image-ack"}, nil
			}}
			image := installAppImageSender(t, f, sender)
			product := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "source", "请回复图片")
			ctx := f.bind(f.ctx, product)
			for _, denied := range []struct {
				ctx       context.Context
				group, id string
			}{{f.ctx, "group_1", "source"}, {ctx, "group_2", "source"}, {ctx, "group_1", "forged"}} {
				out, err := executeAppImage(t, image, denied.ctx, "group", denied.group, denied.id)
				if err != nil || !strings.Contains(out.Content, "未发送") || imageCalls != 0 {
					t.Fatal("untrusted image send reached transport")
				}
			}
			if replydelivery.Outcome(ctx) != replydelivery.NotSent {
				t.Fatal("rejected operation changed delivery evidence")
			}
			if name == "text-first" {
				f.sent(ctx, "group_1", "source")
			}
			out, err := executeAppImage(t, image, ctx, "group", "group_1", "source")
			failed := name == "upload-failed" || name == "missing-ack"
			if failed && (err != replypolicy.ErrSend || !out.IsError || strings.Contains(out.Content, "synthetic-private")) || !failed && err != nil {
				t.Fatal("incorrect image result", err)
			}
			wantState := replydelivery.Sent
			if failed {
				wantState = replydelivery.Unknown
			}
			if replydelivery.Outcome(ctx) != wantState || (imageCalls == 0) != (name == "text-first") {
				t.Fatal("incorrect final ACK evidence or duplicate send")
			}
			f.skipped(ctx, "group_1", "source", 99)
			before := imageCalls
			out, err = executeAppImage(t, image, ctx, "group", "group_1", "source")
			if err != nil || !strings.Contains(out.Content, "未发送") || imageCalls != before {
				t.Fatal("image retry bypassed text/image reservation")
			}
			// Reopen the production policy: both tool types retain the same quota.
			image = installAppImageSender(t, f, sender)
			next := f.group("GROUP_AT_MESSAGE_CREATE", "group_1", "next", "另一条请求")
			nextCtx := f.bind(f.ctx, next)
			f.skipped(nextCtx, "group_1", "next", 1)
			out, err = executeAppImage(t, image, nextCtx, "group", "group_1", "next")
			if err != nil || !strings.Contains(out.Content, "未发送") || imageCalls != before || replydelivery.Outcome(nextCtx) != replydelivery.NotSent {
				t.Fatal("shared persistent quota or new input tracking failed")
			}
		})
	}
}
