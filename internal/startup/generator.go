// Package startup 在服务启动时发布一次系统事件，不直接调用 Agent。
package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"ojj/internal/event"
)

const EventType = "system.startup"

// Publisher 必须可靠提交并响应 context 取消。
type Publisher interface {
	Publish(context.Context, event.Input) (event.PublishResult, error)
}

// Run 每次调用发布一条启动事件，无重试、无离线补发。
// 调用方必须先注册下游订阅；成功后等待取消，避免结束其他长期运行模块。
// 此事件仅代表本地服务模块已初始化，不代表外部平台连接成功。
func Run(ctx context.Context, publisher Publisher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if publisher == nil {
		return errors.New("startup: invalid publisher")
	}
	started := time.Now()
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := publisher.Publish(publishCtx, event.Input{
		EventType: EventType, OccurredAt: started.UTC(), SchemaVersion: 1,
		Content: `{"version":1,"reason":"service_started","skip_memory":true}`,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("startup: publish failed: %w", err)
	}
	slog.Info("startup event published", "component", "startup", "duration", time.Since(started))
	<-ctx.Done()
	return ctx.Err()
}
