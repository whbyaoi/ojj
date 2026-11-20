package brain

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
)

func TestQQImageWindowToBrain(t *testing.T) {
	for _, text := range []string{"", "caption"} {
		t.Run(text, func(t *testing.T) {
			ctx := context.Background()
			requests := make(chan minipi.Request, 1)
			r := openRegion(t, testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
				requests <- req
				return answer("ok"), nil
			})))
			b, err := messagebuilder.Open(ctx, messagebuilder.Config{Path: filepath.Join(t.TempDir(), "window.db"), MaxWindowEvents: 1, PollInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := b.Close(); err != nil {
					t.Error(err)
				}
			})
			raw, err := json.Marshal(map[string]any{
				"id": "message", "author": map[string]string{"user_openid": "sender"},
				"timestamp": time.Now().UTC(), "content": text,
				"attachments":  []qq.Attachment{{URL: "https://example.invalid/image", ContentType: "image/png"}},
				"msg_elements": []qq.Element{{Attachments: []qq.Attachment{{URL: "https://example.invalid/emoji.gif"}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			input, err := qq.Convert("bot", "C2C_MESSAGE_CREATE", raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Handle(ctx, event.Event{ID: "event", ProducerID: "im.qq.bot", ReceivedAt: time.Now(), Input: input}); err != nil {
				t.Fatal(err)
			}
			built, err := b.List(ctx, messagebuilder.MessageQuery{})
			if err != nil || len(built) != 1 || len(built[0].Images) != 2 {
				t.Fatal("QQ image extraction failed", err)
			}
			cancelBrain, brainDone := startRun(t, r)
			deliveryCtx, cancelDelivery := context.WithCancel(ctx)
			t.Cleanup(cancelDelivery)
			deliveryDone := make(chan error, 1)
			go func() { deliveryDone <- b.Run(deliveryCtx, r.Handle) }()
			select {
			case req := <-requests:
				if len(req.Messages) != 1 || req.Messages[0].Content != built[0].Text || !slices.Equal(req.Messages[0].Images, built[0].Images) {
					t.Error("window delivery lost image snapshot")
				}
			case <-time.After(3 * time.Second):
				t.Error("image did not reach model")
			}
			cancelDelivery()
			if err := awaitRun(t, deliveryDone); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			waitStatus(t, r, built[0].ID, StatusSucceeded)
			cancelBrain()
			awaitRun(t, brainDone)
		})
	}
}

func brainTestImages() []minipi.Image {
	return []minipi.Image{{URL: "https://example.invalid/a.png", Label: "event 1"}, {URL: "https://example.invalid/b.gif", Label: "event 2"}}
}

func TestImageCodecIsolation(t *testing.T) {
	images := brainTestImages()
	encoded := encodeMessage(minipi.Message{Role: minipi.RoleUser, Content: "text", Images: images})
	decoded := decodeMessage(encoded)
	if decoded.Content != "text" || !slices.Equal(decoded.Images, images) {
		t.Fatal("image round trip failed")
	}
	images[0].URL = "changed"
	decoded.Images[1].Label = "changed"
	if !slices.Equal(decodeMessage(encoded).Images, brainTestImages()) {
		t.Fatal("image blocks alias input or decoded slices")
	}
	raw, err := json.Marshal(encodeMessage(minipi.Message{Role: minipi.RoleUser, Content: "old"}))
	if err != nil || string(raw) != `{"role":"user","content":[{"type":"text","text":"old"}]}` {
		t.Fatal("legacy encoding changed")
	}
}

func TestImageDurabilityAndHistory(t *testing.T) {
	for _, text := range []string{"", "image caption"} {
		t.Run(text, func(t *testing.T) {
			requests := make(chan minipi.Request, 2)
			cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
				requests <- req
				return answer("ok"), nil
			}))
			r := openRegion(t, cfg)
			input := messagebuilder.Message{ID: "image", Text: text, Images: brainTestImages()}
			if err := r.Handle(context.Background(), input); err != nil {
				t.Fatal(err)
			}
			input.Images[0].URL = "changed"
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			r = openRegion(t, cfg)
			cancel, done := startRun(t, r)
			waitStatus(t, r, "image", StatusSucceeded)
			cancel()
			awaitRun(t, done)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			r = openRegion(t, cfg)
			receive(t, r, "next", "what was in the image?")
			cancel, done = startRun(t, r)
			waitStatus(t, r, "next", StatusSucceeded)
			cancel()
			awaitRun(t, done)
			req := <-requests
			if len(req.Messages) != 1 || req.Messages[0].Content != text || !slices.Equal(req.Messages[0].Images, brainTestImages()) {
				t.Fatal("reopen lost current-window images")
			}
			wantText := text
			if wantText == "" {
				wantText = "[历史图片未加载]"
			}
			req = <-requests
			if len(req.Messages) != 3 || req.Messages[0].Content != wantText || len(req.Messages[0].Images) != 0 {
				t.Fatal("historical images were loaded or historical text changed")
			}
			stored := messages(t, r, "image")
			if !slices.Equal(decodeMessage(stored[0].Message).Images, brainTestImages()) {
				t.Fatal("durable image snapshot changed")
			}
		})
	}
}

func TestImagesStayWithinWindowAcrossToolRounds(t *testing.T) {
	ctx := context.Background()
	requests := make(chan minipi.Request, 4)
	cfg := testConfig(t, modelFunc(func(_ context.Context, req minipi.Request, _ func(minipi.Message)) (minipi.Response, error) {
		requests <- req
		if req.Messages[len(req.Messages)-1].Role == minipi.RoleUser {
			return toolCalls("call-" + req.Messages[len(req.Messages)-1].Content), nil
		}
		return answer("ok"), nil
	}))
	cfg.Agent.Tools = []minipi.Tool{fakeTool(func(context.Context, minipi.ToolCall, func(minipi.ToolResult)) (minipi.ToolResult, error) {
		return minipi.ToolResult{Content: "result"}, nil
	})}
	r := openRegion(t, cfg)
	for i, id := range []string{"first", "second"} {
		if err := r.Handle(ctx, messagebuilder.Message{ID: id, Text: id, Images: brainTestImages()[i : i+1]}); err != nil {
			t.Fatal(err)
		}
		cancel, done := startRun(t, r)
		waitStatus(t, r, id, StatusSucceeded)
		cancel()
		awaitRun(t, done)
		for turn := 0; turn < 2; turn++ {
			req := <-requests
			if len(req.Messages) != i*4+1+turn*2 {
				t.Fatal("unexpected window history length")
			}
			for j, m := range req.Messages {
				if j == i*4 {
					if m.Content != id || !slices.Equal(m.Images, brainTestImages()[i:i+1]) {
						t.Fatal("current window lost images across tool rounds")
					}
				} else if len(m.Images) != 0 {
					t.Fatal("historical images reached model")
				}
			}
		}
	}

	// Historical images no longer consume the next window's context budget.
	receive(t, r, "text", "next")
	in, err := r.Get(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	history, err := r.history(ctx, in)
	if err != nil || len(history.Messages) != 9 {
		t.Fatal("failed to load complete window history", err)
	}
	budget := len(history.SystemPrompt)
	for _, m := range history.Messages {
		if len(m.Images) != 0 {
			t.Fatal("text window loaded historical images")
		}
		raw, err := json.Marshal(encodeMessage(m))
		if err != nil {
			t.Fatal(err)
		}
		budget += len(raw)
	}
	r.cfg.MaxContextBytes = budget
	if _, err := r.history(ctx, in); err != nil {
		t.Fatal("exact filtered history budget rejected", err)
	}
	r.cfg.MaxContextBytes--
	if _, err := r.history(ctx, in); !errors.Is(err, ErrContextCapacity) {
		t.Fatal("filtered history exceeded context budget", err)
	}
	for i, id := range []string{"first", "second"} {
		if !slices.Equal(decodeMessage(messages(t, r, id)[0].Message).Images, brainTestImages()[i:i+1]) {
			t.Fatal("history filtering changed durable snapshots")
		}
	}
}

func TestImageIdempotencyAfterLimitReduction(t *testing.T) {
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		return answer("ok"), nil
	}))
	r := openRegion(t, cfg)
	input := messagebuilder.Message{ID: "image", Text: "same", Images: brainTestImages()}
	if err := r.Handle(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	receive(t, r, "legacy", "same")
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MaxInputBytes, cfg.MaxStoredBytes = 1, 1
	r = openRegion(t, cfg)
	if err := r.Handle(context.Background(), input); err != nil {
		t.Fatal("duplicate rejected after limit reduction", err)
	}
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: "legacy", Text: "same", Images: []minipi.Image{}}); err != nil {
		t.Fatal("empty images differ from legacy nil", err)
	}
	for _, mutate := range []func(*messagebuilder.Message){
		func(m *messagebuilder.Message) { m.Images[0].URL += "?changed=1" },
		func(m *messagebuilder.Message) { m.Images[0].Label += "changed" },
		func(m *messagebuilder.Message) { slices.Reverse(m.Images) },
		func(m *messagebuilder.Message) { m.Images = nil },
		func(m *messagebuilder.Message) { m.ID = "legacy" },
	} {
		changed := input
		changed.Images = slices.Clone(input.Images)
		mutate(&changed)
		if err := r.Handle(context.Background(), changed); !errors.Is(err, ErrConflict) {
			t.Fatal("image mismatch must conflict", err)
		}
	}
}

func TestImageInputAndStorageBudget(t *testing.T) {
	input := messagebuilder.Message{ID: "image", Text: "caption", Images: brainTestImages()}
	imageJSON, _ := json.Marshal(input.Images)
	messageJSON, _ := json.Marshal(encodeMessage(minipi.Message{Role: minipi.RoleUser, Content: input.Text, Images: input.Images}))
	inputCost := len(input.ID) + len(input.Text) + len(imageJSON)
	storedCost := int64(len(input.ID) + len(input.Text) + len(messageJSON))
	for _, kind := range []string{"input", "storage"} {
		for _, delta := range []int{-1, 0} {
			t.Run(kind+string(rune('1'+delta)), func(t *testing.T) {
				cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
					return answer("ok"), nil
				}))
				want := ErrInvalid
				if kind == "input" {
					cfg.MaxInputBytes = inputCost + delta
				} else {
					cfg.MaxStoredBytes = storedCost + int64(delta)
					want = ErrCapacity
				}
				r := openRegion(t, cfg)
				err := r.Handle(context.Background(), input)
				if delta == 0 && err != nil || delta < 0 && !errors.Is(err, want) {
					t.Fatal("incorrect image budget", err)
				}
				if delta < 0 {
					if _, err := r.Get(context.Background(), input.ID); !errors.Is(err, ErrNotFound) {
						t.Fatal("rejected input persisted", err)
					}
				}
			})
		}
	}
}

func TestInvalidImageInputAndContextBudget(t *testing.T) {
	calls := 0
	cfg := testConfig(t, modelFunc(func(context.Context, minipi.Request, func(minipi.Message)) (minipi.Response, error) {
		calls++
		return answer("ok"), nil
	}))
	r := openRegion(t, cfg)
	for _, image := range []minipi.Image{{URL: "file:///secret"}, {URL: "https://example.invalid/secret\n"}, {URL: "https://example.invalid/a", Label: string([]byte{0xff})}} {
		err := r.Handle(context.Background(), messagebuilder.Message{ID: "bad", Images: []minipi.Image{image}})
		if !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid image not safely rejected", err)
		}
	}
	msg := minipi.Message{Role: minipi.RoleUser, Images: brainTestImages()}
	raw, _ := json.Marshal(encodeMessage(msg))
	guard := guardedModel{model: cfg.Agent.Model, maxBytes: len(raw) - 1}
	if _, err := guard.Generate(context.Background(), minipi.Request{Messages: []minipi.Message{msg}}, nil); !errors.Is(err, ErrContextCapacity) || calls != 0 {
		t.Fatal("image context bypassed budget", err)
	}
	guard.maxBytes++
	if _, err := guard.Generate(context.Background(), minipi.Request{Messages: []minipi.Message{msg}}, nil); err != nil || calls != 1 {
		t.Fatal("exact image context budget rejected", err)
	}
	if err := r.Handle(context.Background(), messagebuilder.Message{ID: "image", Images: msg.Images}); err != nil {
		t.Fatal(err)
	}
	in, err := r.Get(context.Background(), "image")
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.MaxContextBytes = len(raw) - 1
	if _, err := r.history(context.Background(), in); !errors.Is(err, ErrContextCapacity) {
		t.Fatal("history ignored image cost", err)
	}
}
