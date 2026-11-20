package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var (
	ErrClosed         = errors.New("event: store closed")
	ErrInvalid        = errors.New("event: invalid input")
	ErrNotFound       = errors.New("event: not found")
	ErrConflict       = errors.New("event: deduplication key conflicts with existing input")
	ErrCapacity       = errors.New("event: capacity exceeded")
	ErrOwned          = errors.New("event: database already owned")
	ErrBound          = errors.New("event: subscription already bound or previous callback still running")
	ErrFilterConflict = errors.New("event: subscription filter differs")
	ErrDisabled       = errors.New("event: subscription disabled")
)

// Config 的零值字段采用 DefaultConfig。容量是逻辑记账字节，不是 SQLite 文件上限。
// 调小容量不会删除已有数据；超限拒收。停用仅取消积压，逻辑存储须等待保留期后清理。
type Config struct {
	Path                      string
	MaxContentBytes           int
	MaxPendingPerSubscription int
	MaxStorageBytes           int64
	Retention                 time.Duration
	MaxAttempts               int
	HandlerTimeout            time.Duration
	RetryInitial              time.Duration
	RetryMax                  time.Duration
	PollInterval              time.Duration
	CleanupInterval           time.Duration
	OperationTimeout          time.Duration
	Logger                    *slog.Logger
}

func DefaultConfig() Config {
	return Config{
		MaxContentBytes: 64 << 10, MaxPendingPerSubscription: 10000,
		MaxStorageBytes: 256 << 20, Retention: 7 * 24 * time.Hour,
		MaxAttempts: 5, HandlerTimeout: 30 * time.Second,
		RetryInitial: time.Second, RetryMax: time.Minute,
		PollInterval: 100 * time.Millisecond, CleanupInterval: time.Minute,
		OperationTimeout: 5 * time.Second, Logger: slog.Default(),
	}
}

func (c Config) normalized() (Config, error) {
	d := DefaultConfig()
	if c.MaxContentBytes == 0 {
		c.MaxContentBytes = d.MaxContentBytes
	}
	if c.MaxPendingPerSubscription == 0 {
		c.MaxPendingPerSubscription = d.MaxPendingPerSubscription
	}
	if c.MaxStorageBytes == 0 {
		c.MaxStorageBytes = d.MaxStorageBytes
	}
	if c.Retention == 0 {
		c.Retention = d.Retention
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.HandlerTimeout == 0 {
		c.HandlerTimeout = d.HandlerTimeout
	}
	if c.RetryInitial == 0 {
		c.RetryInitial = d.RetryInitial
	}
	if c.RetryMax == 0 {
		c.RetryMax = d.RetryMax
	}
	if c.PollInterval == 0 {
		c.PollInterval = d.PollInterval
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = d.CleanupInterval
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = d.OperationTimeout
	}
	if c.Logger == nil {
		c.Logger = d.Logger
	}
	if c.Path == "" || c.MaxContentBytes < 1 || c.MaxPendingPerSubscription < 1 || c.MaxStorageBytes < 1 || c.Retention < 0 || c.MaxAttempts < 1 || c.HandlerTimeout < 0 || c.RetryInitial < 0 || c.RetryMax < c.RetryInitial || c.PollInterval < 0 || c.CleanupInterval < 0 || c.OperationTimeout < 0 {
		return c, fmt.Errorf("%w: path, positive limits and valid retry durations required", ErrInvalid)
	}
	return c, nil
}

func (c Config) Validate() error { _, err := c.normalized(); return err }

// Source 是外壳来源而非正文解析结果；im.message 必须完整提供这些字段。
type Source struct {
	Platform         string `json:"platform"`
	AccountID        string `json:"account_id"`
	ConversationType string `json:"conversation_type"`
	ConversationID   string `json:"conversation_id"`
	SenderID         string `json:"sender_id"`
	MessageID        string `json:"message_id"`
}

// Input 不接受 ProducerID，生产者身份只能由 BindProducer 绑定。
// SchemaVersion 为 0 时补为 1；OccurredAt 必填；正文允许空文本。
type Input struct {
	EventType        string    `json:"event_type"`
	OccurredAt       time.Time `json:"occurred_at"`
	Content          string    `json:"content"`
	SchemaVersion    int       `json:"schema_version"`
	Source           *Source   `json:"source,omitempty"`
	DeduplicationKey string    `json:"deduplication_key,omitempty"`
	CorrelationID    string    `json:"correlation_id,omitempty"`
	CausationEventID string    `json:"causation_event_id,omitempty"`
}

type Event struct {
	ID         string
	Sequence   int64
	ProducerID string
	ReceivedAt time.Time
	Input
}

type PublishResult struct {
	Event                Event
	MatchedSubscriptions int
	Duplicate            bool
}

// Filter 中同一字段为 OR，不同字段为 AND；空集合匹配所有。
type Filter struct {
	ProducerIDs []string `json:"producer_ids,omitempty"`
	EventTypes  []string `json:"event_types,omitempty"`
}

type SubscriptionSpec struct {
	ID     string
	Filter Filter
}

type SubscriptionInfo struct {
	SubscriptionSpec
	Enabled    bool
	CreatedAt  time.Time
	DisabledAt time.Time
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetry      Status = "retry"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusCanceled   Status = "canceled"
)

type Delivery struct {
	EventID        string
	Sequence       int64
	SubscriptionID string
	Status         Status
	Attempts       int
	ErrorCount     int
	AttemptToken   string
	NextAttemptAt  time.Time
	LastError      string
	LastErrorAt    time.Time
	StartedAt      time.Time
	CompletedAt    time.Time
}

// Handler 返回 nil 表示责任已可靠完成；业务副作用应按事件 ID 幂等。
// 超时不能强杀 Go 回调。本订阅在旧回调真正退出前不会启动下一次调用。
type Handler func(context.Context, Event) error

type EventQuery struct {
	ProducerID     string
	EventType      string
	ReceivedFrom   time.Time // inclusive
	ReceivedBefore time.Time // exclusive
	SubscriptionID string
	Status         Status // 可与 SubscriptionID 一起筛选，或匹配任一订阅
	AfterSequence  int64
	Limit          int // 默认 100，上限 1000
}

type DeliveryQuery struct {
	EventID        string
	SubscriptionID string
	Status         Status
	AfterSequence  int64
	Limit          int
	Offset         int // 同一序号存在多订阅记录时可分页；查询不是快照
}
