package qq

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"ojj/internal/event"
)

const privateMessage = `{"id":"m1","timestamp":"2026-11-11T10:00:00+08:00","author":{"user_openid":"user1"},"content":"  hello  ","message_type":103,"message_scene":{"ext":["msg_idx=abc==","ref_msg_idx=ref==","auth_token=secret"]},"ark_data":{"fields":{"number":9007199254740993,"nested":[{"access_token":"secret","ok":true}]}},"msg_elements":[{"msg_idx":"nested==","author":{"user_openid":"u2"},"message_type":0,"content":"nested"}],"attachments":[{"url":"https://example.com/file","filename":"file"}]}`

func TestConvertAndPersist(t *testing.T) {
	ctx := context.Background()
	store, err := event.Open(ctx, event.Config{Path: filepath.Join(t.TempDir(), "events.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	producer, err := store.BindProducer("qq.websocket")
	if err != nil {
		t.Fatal(err)
	}
	input, err := Convert("app", "C2C_MESSAGE_CREATE", []byte(privateMessage))
	if err != nil {
		t.Fatal(err)
	}
	if input.Source.AccountID != "app" || input.Source.ConversationID != "user1" || input.Source.Platform != "qq" {
		t.Fatalf("source: %+v", input.Source)
	}
	if input.OccurredAt.Hour() != 2 {
		t.Fatal(input.OccurredAt)
	}
	var body Content
	if err := json.Unmarshal([]byte(input.Content), &body); err != nil {
		t.Fatal(err)
	}
	if body.Text != "  hello  " || body.MessageIndex != "abc==" || body.ReferenceIndex != "ref==" || len(body.Elements) != 1 || len(body.Attachments) != 1 {
		t.Fatal(input.Content)
	}
	if strings.Contains(input.Content, "secret") || !strings.Contains(input.Content, "9007199254740993") {
		t.Fatal(input.Content)
	}
	first, err := producer.Publish(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	again, err := producer.Publish(ctx, input)
	if err != nil || !again.Duplicate || again.Event.ID != first.Event.ID {
		t.Fatalf("duplicate=%+v err=%v", again, err)
	}
	changed, err := Convert("app", "C2C_MESSAGE_CREATE", []byte(strings.Replace(privateMessage, "  hello  ", "changed", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = producer.Publish(ctx, changed); !errors.Is(err, event.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	for _, raw := range []string{strings.Replace(privateMessage, "abc==", "other==", 1), strings.Replace(privateMessage, "user1", "user2", 1)} {
		other, err := Convert("app", "C2C_MESSAGE_CREATE", []byte(raw))
		if err != nil || other.DeduplicationKey == input.DeduplicationKey {
			t.Fatalf("isolation: %v", err)
		}
	}
	other, err := Convert("app2", "C2C_MESSAGE_CREATE", []byte(privateMessage))
	if err != nil || other.DeduplicationKey == input.DeduplicationKey {
		t.Fatal("account isolation")
	}
}

func TestConvertGroupsAndValidation(t *testing.T) {
	raw := `{"id":"g1","timestamp":"2026-11-11T02:00:00Z","author":{"member_openid":"member"},"group_openid":"group","content":"  @ text  ","mentions":[{"member_openid":"u2"}]}`
	at, err := Convert("app", "GROUP_AT_MESSAGE_CREATE", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	all, err := Convert("app", "GROUP_MESSAGE_CREATE", []byte(raw))
	if err != nil || at.DeduplicationKey == all.DeduplicationKey {
		t.Fatal("event types must remain separate")
	}
	if at.Source.ConversationID != "group" || at.Source.SenderID != "member" || at.Source.ConversationType != "group" {
		t.Fatal(at.Source)
	}
	for _, invalid := range []string{"{}", "null", privateMessage + "{}", strings.Replace(privateMessage, "user1", "", 1), strings.Replace(privateMessage, "2026-11-11T10:00:00+08:00", "bad", 1), strings.Replace(privateMessage, "  hello  ", strings.Repeat("x", 65536), 1), strings.Replace(privateMessage, "auth_token=secret", "msg_idx=conflict", 1)} {
		if _, err := Convert("app", "C2C_MESSAGE_CREATE", []byte(invalid)); !errors.Is(err, ErrMessage) {
			t.Errorf("invalid accepted: %v", err)
		}
	}
	if _, err := Convert("app", "UNKNOWN", []byte(privateMessage)); !errors.Is(err, ErrMessage) {
		t.Fatal(err)
	}
}
