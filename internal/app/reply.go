package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"ojj/internal/agenttools"
	"ojj/internal/brain"
	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
	"ojj/internal/replydelivery"
	"ojj/internal/replypolicy"
)

// replyPolicyConfig validates startup settings without opening any storage.
func replyPolicyConfig(eventPath, accountID string) (replypolicy.Config, error) {
	cfg := replypolicy.DefaultConfig()
	cfg.Path = os.Getenv("OJJ_REPLY_DB")
	if cfg.Path == "" {
		cfg.Path = eventPath + ".reply.db"
	}
	cfg.AccountID = accountID
	cfg.BotUserID = os.Getenv("OJJ_REPLY_BOT_USER_ID")
	if raw := os.Getenv("OJJ_REPLY_ALIASES"); raw != "" {
		for _, alias := range strings.Split(raw, ",") {
			cfg.Aliases = append(cfg.Aliases, strings.TrimSpace(alias))
		}
	}
	if raw := os.Getenv("OJJ_REPLY_PROACTIVE"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return cfg, errors.New("OJJ_REPLY_PROACTIVE 必须是布尔值")
		}
		cfg.Proactive = value
	}
	for _, setting := range []struct {
		name string
		dest *time.Duration
	}{
		{"OJJ_REPLY_ACTIVE_INTERVAL", &cfg.ActiveInterval},
		{"OJJ_REPLY_ACTIVE_WINDOW", &cfg.ActiveWindow},
		{"OJJ_REPLY_SESSION_TTL", &cfg.SessionTTL},
		{"OJJ_REPLY_TOTAL_WINDOW", &cfg.TotalWindow},
	} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil || value <= 0 {
				return cfg, fmt.Errorf("%s 必须是正时长", setting.name)
			}
			*setting.dest = value
		}
	}
	for _, setting := range []struct {
		name string
		dest *int
	}{
		{"OJJ_REPLY_ACTIVE_LIMIT", &cfg.ActiveLimit},
		{"OJJ_REPLY_TOTAL_LIMIT", &cfg.TotalLimit},
	} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				return cfg, fmt.Errorf("%s 必须是正整数", setting.name)
			}
			*setting.dest = value
		}
	}
	return cfg, cfg.Validate()
}

// attachReplyPolicy binds each real durable input to its permanent event snapshot.
// The caller must close the brain before the policy, and the policy before builder.
func attachReplyPolicy(ctx context.Context, cfg *brain.Config, builder *messagebuilder.Builder,
	pc replypolicy.Config, sender agenttools.QQSender, allowed map[string]struct{}) (*replypolicy.Policy, error) {
	if ctx == nil || cfg == nil || builder == nil {
		return nil, replypolicy.ErrInvalid
	}
	policy, err := replypolicy.Open(ctx, pc, sender)
	if err != nil {
		return nil, err
	}
	// Install atomically so a failed tool assembly leaves the caller unchanged.
	next := *cfg
	next.Agent.Tools = append([]minipi.Tool(nil), cfg.Agent.Tools...)
	if err := attachQQTool(&next, policy, allowed); err != nil {
		return nil, errors.Join(replypolicy.ErrInvalid, policy.Close())
	}
	next.InputContext = func(call context.Context, id string) (context.Context, error) {
		if call == nil {
			return nil, replypolicy.ErrInvalid
		}
		events, err := builder.Events(call, id)
		if err != nil {
			if call.Err() != nil {
				return nil, call.Err()
			}
			return nil, replypolicy.ErrStorage
		}
		bound, err := policy.Bind(call, events)
		if err != nil {
			return nil, err
		}
		bound = replydelivery.WithTracking(bound)
		// Only chat-only windows may be skipped. Heartbeats and other host
		// work must run even alongside quota-blocked QQ messages.
		for _, e := range events {
			if e.ProducerID != "qq.websocket" || e.EventType != "im.message" {
				return brain.WithModelSkipped(bound, false), nil
			}
		}
		ready, err := policy.Precheck(bound)
		if err != nil {
			return nil, err
		}
		return brain.WithModelSkipped(bound, !ready), nil
	}
	*cfg = next
	return policy, nil
}
