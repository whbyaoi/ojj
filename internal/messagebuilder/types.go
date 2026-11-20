package messagebuilder

import (
	"context"
	"errors"
	"ojj/internal/imagecache"
	"ojj/internal/minipi"
	"time"
)

var (
	ErrClosed   = errors.New("messagebuilder: closed")
	ErrInvalid  = errors.New("messagebuilder: invalid input or configuration")
	ErrNotFound = errors.New("messagebuilder: message not found")
	ErrConflict = errors.New("messagebuilder: event ID conflicts with stored snapshot")
	ErrCapacity = errors.New("messagebuilder: capacity exceeded")
	ErrOwned    = errors.New("messagebuilder: database already owned")
	ErrRunning  = errors.New("messagebuilder: previous Run or callback still running")
)

// Config 零字段采用 DefaultConfig。Path 必须是独立的本地数据库文件。
// 单事件及窗口字节限制计入完整事件 JSON、额外图片 JSON 和每图 19 字节序号预留。
// 图片缓存独立限额，不计入窗口字节；不截断正文或图片引用，容量收束不丢事件。
// v1 活动窗口迁移时重算费用，若已达上限则下次处理或调度收束，不拒绝已接收事件。
// inbox、消息及去重记录永久保留，不自动淘汰。MaxStoredEvents 限制累计接收数，
// 达到上限拒收新事件（重复事件仍可确认）；需由运营归档后切换新库。
// 这些是逻辑限制而非 SQLite/WAL 物理文件大小限制，磁盘空间须另行监控。
// 已打开窗口保留原时间及容量规则；新事件准入仍受当前单事件、窗口字节及累计上限约束。
type Config struct {
	// ImageCache prefetches after commit within the operation deadline.
	// Failure never rejects a committed event.
	ImageCache       imagecache.Resolver
	Path             string
	QuietPeriod      time.Duration
	MaxWindow        time.Duration
	PollInterval     time.Duration
	OperationTimeout time.Duration
	DeliveryTimeout  time.Duration
	RetryInitial     time.Duration
	RetryMax         time.Duration
	MaxEventBytes    int
	MaxWindowEvents  int
	MaxWindowBytes   int
	MaxStoredEvents  int64
}

func DefaultConfig() Config {
	return Config{
		QuietPeriod: 8 * time.Second, MaxWindow: 30 * time.Second,
		PollInterval: 100 * time.Millisecond, OperationTimeout: 5 * time.Second,
		DeliveryTimeout: 30 * time.Second, RetryInitial: time.Second, RetryMax: time.Minute,
		MaxEventBytes: 256 << 10, MaxWindowEvents: 256, MaxWindowBytes: 4 << 20,
		MaxStoredEvents: 100000,
	}
}

func (c Config) normalized() (Config, error) {
	d := DefaultConfig()
	for _, pair := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&c.QuietPeriod, d.QuietPeriod}, {&c.MaxWindow, d.MaxWindow},
		{&c.PollInterval, d.PollInterval}, {&c.OperationTimeout, d.OperationTimeout},
		{&c.DeliveryTimeout, d.DeliveryTimeout}, {&c.RetryInitial, d.RetryInitial}, {&c.RetryMax, d.RetryMax},
	} {
		if *pair.value == 0 {
			*pair.value = pair.fallback
		}
		if *pair.value < 0 {
			return c, ErrInvalid
		}
	}
	if c.MaxEventBytes == 0 {
		c.MaxEventBytes = d.MaxEventBytes
	}
	if c.MaxWindowEvents == 0 {
		c.MaxWindowEvents = d.MaxWindowEvents
	}
	if c.MaxWindowBytes == 0 {
		c.MaxWindowBytes = d.MaxWindowBytes
	}
	if c.MaxStoredEvents == 0 {
		c.MaxStoredEvents = d.MaxStoredEvents
	}
	if c.Path == "" || c.MaxEventBytes < 1 || c.MaxWindowEvents < 1 || c.MaxWindowBytes < 1 || c.MaxStoredEvents < 1 || c.RetryMax < c.RetryInitial {
		return c, ErrInvalid
	}
	return c, nil
}

func (c Config) Validate() error { _, err := c.normalized(); return err }

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetry      Status = "retry"
	StatusSucceeded  Status = "succeeded"
)

type CloseReason string

const (
	ReasonQuiet     CloseReason = "quiet"
	ReasonMaxWindow CloseReason = "max_window"
	ReasonCapacity  CloseReason = "capacity"
)

// Message 正文、图片 URL/标签与事件 ID 是敏感数据，调用方负责访问控制，不能直接写入日志。
// ClosedAt 为逻辑窗口截止时间，CreatedAt 为实际落库时间。
// LastError 仅含固定分类，不保存原始回调错误、panic 值或堆栈。
type Message struct {
	ID            string
	Sequence      int64
	Text          string
	Images        []minipi.Image // Sensitive, stable remote attachment snapshot.
	EventIDs      []string
	OpenedAt      time.Time
	ClosedAt      time.Time
	CreatedAt     time.Time
	CloseReason   CloseReason
	Status        Status
	Attempts      int64
	NextAttemptAt time.Time
	StartedAt     time.Time
	CompletedAt   time.Time
	LastError     string
}

type MessageQuery struct {
	Status        Status
	AfterSequence int64
	Limit         int // 默认 100，上限 1000；结果按 Sequence 升序，查询不是快照。
}

// Handler 至少一次交付；下游必须以稳定 Message.ID 幂等，不保证 exactly-once。
// 返回 nil 表示责任已可靠完成。必须遵守 ctx，不能强杀 Go 回调。
// Run 和 Close 都等待旧回调真正退出，回调不得同步调用 Close 或 Run。
// nil Handler 只构建消息并保持 pending；失败无限退避重试至 RetryMax 间隔。
type Handler func(context.Context, Message) error
