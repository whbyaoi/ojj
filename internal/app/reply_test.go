package app

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"ojj/internal/replypolicy"
)

func TestReplyPolicyConfigDefaults(t *testing.T) {
	brainTestEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")
	cfg, err := replyPolicyConfig(path, "account")
	want := replypolicy.DefaultConfig()
	want.Path, want.AccountID = path+".reply.db", "account"
	if err != nil || !reflect.DeepEqual(cfg, want) {
		t.Fatal("unexpected default reply configuration")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("configuration opened storage")
	}
}

func TestReplyPolicyConfigOverrides(t *testing.T) {
	brainTestEnv(t)
	path := filepath.Join(t.TempDir(), "reply.db")
	for key, value := range map[string]string{
		"OJJ_REPLY_DB": path, "OJJ_REPLY_BOT_USER_ID": "bot",
		"OJJ_REPLY_ALIASES": " first , 第二个 ", "OJJ_REPLY_PROACTIVE": "true",
		"OJJ_REPLY_ACTIVE_INTERVAL": "30s", "OJJ_REPLY_ACTIVE_WINDOW": "5m",
		"OJJ_REPLY_ACTIVE_LIMIT": "4", "OJJ_REPLY_SESSION_TTL": "3m",
		"OJJ_REPLY_TOTAL_WINDOW": "2m", "OJJ_REPLY_TOTAL_LIMIT": "20",
	} {
		t.Setenv(key, value)
	}
	cfg, err := replyPolicyConfig("unused", "account")
	want := replypolicy.Config{
		Path: path, AccountID: "account", BotUserID: "bot", Aliases: []string{"first", "第二个"},
		Proactive: true, ActiveInterval: 30 * time.Second, ActiveWindow: 5 * time.Minute,
		ActiveLimit: 4, SessionTTL: 3 * time.Minute, TotalWindow: 2 * time.Minute, TotalLimit: 20,
	}
	if err != nil || !reflect.DeepEqual(cfg, want) {
		t.Fatal("reply configuration overrides not applied")
	}
}

func TestReplyPolicyConfigInvalid(t *testing.T) {
	cases := []struct{ key, value string }{
		{"OJJ_REPLY_DB", ":memory:"}, {"OJJ_REPLY_DB", "file:private-config"},
		{"OJJ_REPLY_DB", " "}, {"OJJ_REPLY_BOT_USER_ID", " private-config"},
		{"OJJ_REPLY_ALIASES", "private-config,"}, {"OJJ_REPLY_ALIASES", " , "},
		{"OJJ_REPLY_ALIASES", strings.Repeat("a,", 32) + "a"},
		{"OJJ_REPLY_ALIASES", strings.Repeat("a", 129)},
		{"OJJ_REPLY_BOT_USER_ID", strings.Repeat("a", 257)},
		{"OJJ_REPLY_PROACTIVE", "private-config"},
		{"OJJ_REPLY_ACTIVE_WINDOW", "1s"},
	}
	for _, key := range []string{"OJJ_REPLY_ACTIVE_INTERVAL", "OJJ_REPLY_ACTIVE_WINDOW", "OJJ_REPLY_SESSION_TTL", "OJJ_REPLY_TOTAL_WINDOW", "OJJ_REPLY_ACTIVE_LIMIT", "OJJ_REPLY_TOTAL_LIMIT"} {
		for _, value := range []string{"private-config", "0", "-1", "999999999999999999999999999999"} {
			cases = append(cases, struct{ key, value string }{key, value})
		}
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			brainTestEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := replyPolicyConfig("unused", "account"); err == nil {
				t.Fatal("accepted invalid reply configuration")
			} else if strings.Contains(err.Error(), "private-config") {
				t.Fatal("configuration leaked into returned error")
			}
		})
	}
}

func TestReplyPolicyValidateWithoutSideEffects(t *testing.T) {
	dir := t.TempDir()
	cfg := replypolicy.Config{Path: filepath.Join(dir, "reply.db"), AccountID: "account", Aliases: []string{"bot"}}
	before := cfg
	before.Aliases = append([]string(nil), cfg.Aliases...)
	if err := cfg.Validate(); err != nil || !reflect.DeepEqual(cfg, before) {
		t.Fatal("validation rejected defaults or mutated caller")
	}
	for _, path := range []string{"", " ", ":memory:", "file:reply.db", "bad\x00path", string([]byte{0xff})} {
		cfg.Path = path
		if err := cfg.Validate(); err != replypolicy.ErrInvalid {
			t.Fatal("accepted invalid storage path")
		}
	}
	cfg.Path = before.Path
	for _, account := range []string{"", " account", strings.Repeat("a", 257), string([]byte{0xff})} {
		cfg.AccountID = account
		if err := cfg.Validate(); err != replypolicy.ErrInvalid {
			t.Fatal("accepted invalid account")
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("validation opened storage")
	}
}
