// Package app 负责组装应用模块并管理生命周期。
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"ojj/internal/brain"
	"ojj/internal/cancelerr"
	"ojj/internal/event"
	"ojj/internal/heartbeat"
	"ojj/internal/im/qq"
	"ojj/internal/imagecache"
	"ojj/internal/messagebuilder"
	"ojj/internal/minipi"
	"ojj/internal/startup"
)

// Run 启动单账号 QQ 接收、启动事件、空闲心跳和全局事件消息构建。
// OJJ_HEARTBEAT_INTERVAL 默认五分钟；OJJ_MESSAGE_QUIET_PERIOD 默认 8s，
// OJJ_MESSAGE_MAX_WINDOW 默认 30s。OJJ_MESSAGE_DB 默认为事件库路径加 .messages.db。
// 数据库父目录须预先存在；固定订阅恢复积压，首次注册不回放历史。
// 成品可靠入队主决策脑区；模型及工具配置见 mainBrainConfig，不打印正文。
func Run(ctx context.Context) (err error) {
	started := time.Now()
	stage := "qq_config"
	// Registered first so completion includes every resource-close error.
	defer func() {
		minipi.LogOperation(ctx, "app", stage, started, err, slog.LevelInfo)
	}()
	appID, secret := os.Getenv("QQ_APP_ID"), os.Getenv("QQ_CLIENT_SECRET")
	path := os.Getenv("OJJ_EVENT_DB")
	if appID == "" || secret == "" {
		return errors.New("请配置 QQ_APP_ID 和 QQ_CLIENT_SECRET")
	}
	if path == "" {
		path = "events.db"
	}
	stage = "heartbeat_config"
	interval, err := heartbeatInterval(os.Getenv("OJJ_HEARTBEAT_INTERVAL"))
	if err != nil {
		return err
	}
	stage = "messagebuilder_config"
	builderConfig, err := messageBuilderConfig(path)
	if err != nil {
		return err
	}
	// Shared, lazy private cache: independent of message/history database rows.
	images := imagecache.New(path + ".images")
	builderConfig.ImageCache = images
	stage = "brain_config"
	brainConfig, allowed, err := mainBrainConfigWithImages(path, images)
	if err != nil {
		return err
	}
	stage = "reply_config"
	replyConfig, err := replyPolicyConfig(path, appID)
	if err != nil {
		return err
	}
	stage = "event_open"
	store, err := event.Open(ctx, event.Config{Path: path})
	if err != nil {
		return fmt.Errorf("打开事件存储失败: %w", err)
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	stage = "qq_producer_bind"
	producer, err := store.BindProducer("qq.websocket")
	if err != nil {
		return err
	}
	stage = "startup_producer_bind"
	startupProducer, err := store.BindProducer(startup.EventType)
	if err != nil {
		return err
	}
	stage = "heartbeat_producer_bind"
	heartbeatProducer, err := store.BindProducer("system.heartbeat")
	if err != nil {
		return err
	}
	stage = "heartbeat_init"
	generator, err := heartbeat.New(interval, heartbeatProducer)
	if err != nil {
		return err
	}
	stage = "heartbeat_track"
	tracked, err := generator.Track(producer)
	if err != nil {
		return err
	}
	stage = "qq_init"
	client, err := qq.New(qq.Config{AppID: appID, ClientSecret: secret}, tracked)
	if err != nil {
		return err
	}

	stage = "messagebuilder_open"
	builder, err := messagebuilder.Open(ctx, builderConfig)
	if err != nil {
		return fmt.Errorf("打开消息构建存储失败: %w", err)
	}
	defer func() { err = errors.Join(err, builder.Close()) }()
	stage = "reply_open"
	policy, err := attachReplyPolicy(ctx, &brainConfig, builder, replyConfig, client, allowed)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, policy.Close()) }()
	stage = "brain_open"
	region, err := brain.Open(ctx, brainConfig)
	if err != nil {
		return fmt.Errorf("打开主决策脑区失败: %w", err)
	}
	defer func() { err = errors.Join(err, region.Close()) }()
	slog.Info("ojj QQ 接收、空闲心跳、消息构建与主决策脑区启动",
		"component", "app", "stage", "start")
	if len(brainConfig.Agent.Tools) > 1 {
		slog.Warn("脚本工具已启用：具备宿主账号权限，不是沙箱；最小环境变量不阻止读取本地文件", "component", "app", "stage", "command_enabled")
	}
	stage = "run_end"
	// runWithMessages 先注册固定订阅，再启动发生器，首次启动也不会漏掉启动事件。
	err = runWithMessages(ctx, store, builder, region.Handle, func(child context.Context) error {
		return startup.Run(child, startupProducer)
	}, client.Run, generator.Run, region.Run)
	return err
}

// 固定身份与全匹配过滤器：所有生产者进入同一个全局窗口。
const messageSubscriptionID = "messagebuilder.global.v1"

func runWithMessages(ctx context.Context, store *event.Store, builder *messagebuilder.Builder,
	downstream messagebuilder.Handler, producers ...func(context.Context) error) error {
	bindingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, 1)
	if downstream != nil {
		handle := downstream
		downstream = func(call context.Context, msg messagebuilder.Message) error {
			err := handle(call, msg)
			// Reliable admission must not retry forever on conflicts or full storage.
			// Return the error to keep the builder message unacknowledged, and stop.
			if err != nil && !cancelerr.Only(err, call.Err()) {
				select {
				case failures <- fmt.Errorf("脑区消息准入失败: %w", err):
				default:
				}
			}
			return err
		}
	}
	var callbacks sync.WaitGroup
	var callbackMu sync.Mutex
	stopping := false
	sub, err := store.Subscribe(bindingCtx, event.SubscriptionSpec{ID: messageSubscriptionID},
		func(call context.Context, e event.Event) error {
			callbackMu.Lock()
			if stopping {
				callbackMu.Unlock()
				return context.Canceled
			}
			callbacks.Add(1)
			callbackMu.Unlock()
			defer callbacks.Done()
			err := builder.Handle(call, e)
			// 准入失败立即停止应用，避免无人察觉地耗尽上游重试次数。
			if err != nil && !cancelerr.Only(err, call.Err()) {
				select {
				case failures <- err:
				default:
				}
			}
			return err
		})
	if err != nil {
		return err
	}
	defer sub.Close()
	runners := append([]func(context.Context) error{}, producers...)
	runners = append(runners, func(child context.Context) error { return builder.Run(child, downstream) },
		func(child context.Context) error {
			var failure error
			select {
			case <-child.Done():
			case <-sub.Done():
				if child.Err() == nil && sub.Err() == nil {
					failure = errors.New("消息构建订阅意外停止")
				}
			case failure = <-failures:
			}
			sub.Close()
			<-sub.Done()
			// Done 只代表调度结束；禁止迟到调用登记，并等已进入的入库回调退出。
			callbackMu.Lock()
			stopping = true
			callbackMu.Unlock()
			callbacks.Wait()
			select {
			case e := <-failures:
				failure = errors.Join(failure, e)
			default:
			}
			return errors.Join(failure, sub.Err())
		})
	err = runTogether(bindingCtx, runners...)
	// builder.Run waits for its downstream callback. Drain again after all runners
	// stop so an admission error arriving after the monitor exits is not lost.
	select {
	case failure := <-failures:
		err = errors.Join(err, failure)
	default:
	}
	return err
}

func messageBuilderConfig(eventPath string) (messagebuilder.Config, error) {
	cfg := messagebuilder.DefaultConfig()
	cfg.Path = os.Getenv("OJJ_MESSAGE_DB")
	if cfg.Path == "" {
		cfg.Path = eventPath + ".messages.db"
	}
	for _, setting := range []struct {
		name string
		dest *time.Duration
	}{
		{"OJJ_MESSAGE_QUIET_PERIOD", &cfg.QuietPeriod},
		{"OJJ_MESSAGE_MAX_WINDOW", &cfg.MaxWindow},
	} {
		if value := os.Getenv(setting.name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 {
				return cfg, fmt.Errorf("%s 必须是正时长，例如 2s 或 10s", setting.name)
			}
			*setting.dest = duration
		}
	}
	return cfg, cfg.Validate()
}

// heartbeatInterval 为空时默认五分钟；例如 30s、5m、1h，必须大于零。
func heartbeatInterval(value string) (time.Duration, error) {
	if value == "" {
		return heartbeat.DefaultInterval, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, errors.New("OJJ_HEARTBEAT_INTERVAL 必须是正时长，例如 5m 或 30s")
	}
	return interval, nil
}

// runTogether 任一发生器退出即取消其他发生器，等待全部退出后才允许关闭存储。
func runTogether(ctx context.Context, runners ...func(context.Context) error) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(runners))
	for _, run := range runners {
		go func() {
			err := run(child)
			// 只吞掉该发生器因生命周期取消产生的错误，保留其他失败。
			if child.Err() != nil && cancelerr.Only(err, child.Err()) {
				err = nil
			}
			results <- err
		}()
	}
	var result error
	for range runners {
		result = errors.Join(result, <-results)
		cancel()
	}
	return result
}
