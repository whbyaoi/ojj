// Package heartbeat 在没有外部消息时定时发布系统心跳，不负责调用 Agent。
package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"ojj/internal/cancelerr"
	"ojj/internal/event"
)

const DefaultInterval = 5 * time.Minute
const EventType = "system.heartbeat"

var ErrConfig = errors.New("heartbeat: invalid configuration")
var ErrRunning = errors.New("heartbeat: already running")

// Publisher 必须可靠提交并响应 context 取消；接入同一发生器的发布过程串行。
// Run 退出等待自己的在途心跳发布，外部发布的退出由其调用方管理。
type Publisher interface {
	Publish(context.Context, event.Input) (event.PublishResult, error)
}

// Content 是版本化 JSON 正文。SkipMemory 是对后续记忆消费者的约定，
// 不阻止事件存储持久化；当前发生器不写入记忆，也不判断 Agent 是否忙碌。
type Content struct {
	Version    int       `json:"version"`
	Reason     string    `json:"reason"`
	IdleSince  time.Time `json:"idle_since"`
	Interval   string    `json:"interval"`
	SkipMemory bool      `json:"skip_memory"`
}

type Generator struct {
	interval  time.Duration
	publisher Publisher
	running   atomic.Bool
	gate      chan struct{}
	idleSince time.Time
	next      time.Time
	wake      chan struct{}
}

// New 的零间隔使用五分钟，负间隔无效。空闲范围是接入本实例的所有外部消息。
func New(interval time.Duration, publisher Publisher) (*Generator, error) {
	if interval == 0 {
		interval = DefaultInterval
	}
	if interval < 0 || publisher == nil {
		return nil, ErrConfig
	}
	return &Generator{interval: interval, publisher: publisher, gate: make(chan struct{}, 1), wake: make(chan struct{}, 1)}, nil
}

// lock 在等待其他发布时也响应当前调用的取消，不依赖持锁者的上下文。
func (g *Generator) lock(ctx context.Context) error {
	select {
	case g.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			g.unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Generator) unlock() { <-g.gate }

type trackedPublisher struct {
	generator *Generator
	publisher Publisher
}

// Track 包装外部消息生产者，仅成功提交且非重复的 im.message 重置空闲时间。
// 无需增加存储订阅，不会回放历史。使用本地提交时间而非平台消息时间。
func (g *Generator) Track(publisher Publisher) (Publisher, error) {
	if publisher == nil {
		return nil, ErrConfig
	}
	return &trackedPublisher{generator: g, publisher: publisher}, nil
}

func (p *trackedPublisher) Publish(ctx context.Context, input event.Input) (event.PublishResult, error) {
	g := p.generator
	// 将外部提交与心跳的到期检查串行化，避免已提交外部消息之后误发过期心跳。
	if err := g.lock(ctx); err != nil {
		return event.PublishResult{}, err
	}
	defer g.unlock()
	result, err := p.publisher.Publish(ctx, input)
	if err == nil && !result.Duplicate && input.EventType == "im.message" {
		g.idleSince = time.Now()
		g.next = g.idleSince.Add(g.interval)
		select {
		case g.wake <- struct{}{}:
		default:
		}
	}
	return result, err
}

// Run 首次心跳等待完整空闲间隔；再次运行重新计时，不补发离线心跳。
// 发布失败直接返回，不跳过错误或高速重试。调用方负责取消其他发生器。
func (g *Generator) Run(ctx context.Context) (retErr error) {
	logger := slog.Default().With("component", "heartbeat")
	if !g.running.CompareAndSwap(false, true) {
		logger.Warn("heartbeat run rejected", "failure", "already_running")
		return ErrRunning
	}
	defer g.running.Store(false)
	started := time.Now()
	logger.Info("heartbeat started", "interval", g.interval)
	defer func() {
		if retErr != nil {
			switch {
			case cancelerr.Only(retErr, context.Canceled):
				logger.Debug("heartbeat canceled")
			case cancelerr.Only(retErr, context.DeadlineExceeded):
				logger.Warn("heartbeat deadline exceeded", "failure", "timeout")
			default:
				logger.Error("heartbeat failed", "failure", "operation_failure")
			}
		}
		logger.Info("heartbeat stopped", "duration", time.Since(started))
	}()
	if err := g.lock(ctx); err != nil {
		return err
	}
	g.idleSince = time.Now()
	g.next = g.idleSince.Add(g.interval)
	g.unlock()
	timer := time.NewTimer(g.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.wake:
		case <-timer.C:
		}
		delay, err := g.step(ctx)
		if err != nil {
			return err
		}
		timer.Reset(delay)
	}
}

func (g *Generator) step(ctx context.Context) (time.Duration, error) {
	if err := g.lock(ctx); err != nil {
		return 0, err
	}
	defer g.unlock()
	now := time.Now()
	if now.Before(g.next) {
		return g.next.Sub(now), nil
	}
	body, err := json.Marshal(Content{Version: 1, Reason: "idle", IdleSince: g.idleSince.UTC(), Interval: g.interval.String(), SkipMemory: true})
	if err != nil {
		return 0, err
	}
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	logger := slog.Default().With("component", "heartbeat")
	logger.Debug("heartbeat triggered")
	started := time.Now()
	result, err := g.publisher.Publish(publishCtx, event.Input{
		EventType: EventType, OccurredAt: now.UTC(), Content: string(body), SchemaVersion: 1,
	})
	if err != nil {
		return 0, fmt.Errorf("heartbeat: publish failed: %w", err)
	}
	// 从完成时刻重新等待，慢发布不产生追赶式突发。
	g.next = time.Now().Add(g.interval)
	logger.Info("heartbeat published", "event_id", result.Event.ID, "duplicate", result.Duplicate, "duration", time.Since(started))
	return g.interval, nil
}
