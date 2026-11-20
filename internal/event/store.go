package event

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ojj/internal/cancelerr"

	_ "modernc.org/sqlite"
)

// Store 只支持本地文件系统上的单进程所有者，不支持数据库硬链接别名。
// 不能复制或替换正在使用的数据库及伴随文件，不能删除或替换 .lock 所有权锁文件。
// 所有 SQL 使用一个连接；事务内从不运行消费者。
type Store struct {
	db        *sql.DB
	file      *os.File
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	registry  chan struct{} // 注册和停用串行，但等待可被 context 取消。
	closed    bool
	bindings  map[string]*Subscription
	wg        sync.WaitGroup
	closeDone chan struct{}
	closeErr  error
	lastErr   error
}

const schema = `
CREATE TABLE IF NOT EXISTS event_meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
INSERT OR IGNORE INTO event_meta VALUES(1,1);
CREATE TABLE IF NOT EXISTS events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
 producer_id TEXT NOT NULL, event_type TEXT NOT NULL, received_at INTEGER NOT NULL,
 dedup_key TEXT, input TEXT NOT NULL, logical_bytes INTEGER NOT NULL,
 UNIQUE(producer_id,dedup_key)
);
CREATE INDEX IF NOT EXISTS events_received ON events(received_at);
CREATE TABLE IF NOT EXISTS subscriptions (
 id TEXT PRIMARY KEY, filter TEXT NOT NULL, enabled INTEGER NOT NULL,
 created_at INTEGER NOT NULL, disabled_at INTEGER NOT NULL DEFAULT 0,
 logical_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
 event_sequence INTEGER NOT NULL REFERENCES events(sequence) ON DELETE CASCADE,
 subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
 status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 error_count INTEGER NOT NULL DEFAULT 0, token TEXT NOT NULL DEFAULT '',
 next_at INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
 error_at INTEGER NOT NULL DEFAULT 0, started_at INTEGER NOT NULL DEFAULT 0,
 completed_at INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(event_sequence,subscription_id)
);
CREATE INDEX IF NOT EXISTS deliveries_queue ON deliveries(subscription_id,status,event_sequence);
`

func Open(ctx context.Context, config Config) (_ *Store, retErr error) {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	config.Logger = logger.With("component", "event")
	started := time.Now()
	config.Logger.Info("event store opening")
	defer func() {
		if retErr != nil {
			logFailure(config.Logger, "open", retErr)
		}
	}()
	cfg, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(cfg.Path, "file:") || cfg.Path == ":memory:" {
		return nil, fmt.Errorf("%w: ordinary file path required", ErrInvalid)
	}
	path, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("%w: regular database file required", ErrInvalid), f.Close())
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	// macOS 上 flock 与 SQLite 的文件锁会互相冲突，所有权使用独立锁文件。
	// 锁文件必须保留，不能在存储运行期间删除或替换。
	if err = f.Close(); err != nil {
		return nil, err
	}
	f, err = os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	// 连接可能被 database/sql 丢弃重建；每个新连接都必须重新设置安全 PRAGMA。
	params := url.Values{}
	for _, pragma := range []string{"busy_timeout=1000", "foreign_keys=ON", "journal_mode=WAL", "synchronous=FULL"} {
		params.Add("_pragma", pragma)
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: params.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fail := func(e error) (*Store, error) { return nil, errors.Join(e, db.Close(), f.Close()) }
	op, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	if err = db.PingContext(op); err != nil {
		return fail(err)
	}
	tx, err := db.BeginTx(op, nil)
	if err != nil {
		return fail(err)
	}
	if _, err = tx.ExecContext(op, schema); err == nil {
		var version int
		err = tx.QueryRowContext(op, "SELECT version FROM event_meta WHERE id=1").Scan(&version)
		if err == nil && version != 1 {
			err = fmt.Errorf("event: unsupported database version %d", version)
		}
	}
	if err == nil {
		now := time.Now().UnixNano()
		_, err = tx.ExecContext(op, `UPDATE deliveries SET status=CASE WHEN attempts>=? THEN 'failed' ELSE 'retry' END,
 token='', next_at=?, completed_at=CASE WHEN attempts>=? THEN ? ELSE 0 END,
 error_count=error_count+1,last_error='interrupted by store restart',error_at=? WHERE status='processing'`, cfg.MaxAttempts, now, cfg.MaxAttempts, now, now)
	}
	if err != nil {
		return fail(errors.Join(err, tx.Rollback()))
	}
	if err = tx.Commit(); err != nil {
		return fail(err)
	}
	root, stop := context.WithCancel(context.Background())
	s := &Store{db: db, file: f, cfg: cfg, ctx: root, cancel: stop, bindings: make(map[string]*Subscription), closeDone: make(chan struct{}), registry: make(chan struct{}, 1)}
	s.wg.Add(1)
	go s.maintenance()
	cfg.Logger.Info("event store opened", "duration", time.Since(started))
	return s, nil
}

// Close 等待内部调度退出但不等待不合作回调。可重复、并发调用。
// 回调必须遵守 context；跨 Store 关闭重开无法阻止旧回调的外部副作用。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.cancel()
	for _, sub := range s.bindings {
		sub.cancel()
	}
	s.mu.Unlock()
	started := time.Now()
	s.cfg.Logger.Info("event store closing")
	s.wg.Wait()
	err := errors.Join(s.db.Close(), s.file.Close())
	if err != nil {
		logFailure(s.cfg.Logger, "close", err)
	}
	s.cfg.Logger.Info("event store closed", "duration", time.Since(started), "resources_closed", err == nil)
	s.mu.Lock()
	s.closeErr = errors.Join(err, s.lastErr)
	close(s.closeDone)
	s.mu.Unlock()
	return s.closeErr
}

func (s *Store) operation(ctx context.Context) (context.Context, context.CancelFunc, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, nil, ErrClosed
	}
	op, cancel := context.WithTimeout(ctx, s.cfg.OperationTimeout)
	stop := context.AfterFunc(s.ctx, cancel)
	return op, func() { stop(); cancel() }, nil
}

// lockRegistry 不持有 mu 等待 SQL，并将管理操作纳入关闭等待。
func (s *Store) lockRegistry(ctx context.Context) (func(), error) {
	select {
	case s.registry <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.registry
		return nil, ErrClosed
	}
	s.wg.Add(1)
	s.mu.Unlock()
	return func() { <-s.registry; s.wg.Done() }, nil
}

func (s *Store) recordError(action string, err error) {
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
	logFailure(s.cfg.Logger, action, err)
}

// 日志只使用固定类别，绝不格式化 SQL 或回调错误。
func logFailure(logger *slog.Logger, operation string, err error) {
	level, category := slog.LevelError, "storage_failure"
	switch {
	case cancelerr.Only(err, context.Canceled):
		level, category = slog.LevelDebug, "canceled"
	case cancelerr.Only(err, context.DeadlineExceeded):
		level, category = slog.LevelWarn, "timeout"
	case errors.Is(err, ErrInvalid):
		level, category = slog.LevelWarn, "invalid_input"
	case errors.Is(err, ErrCapacity):
		level, category = slog.LevelWarn, "capacity"
	case errors.Is(err, ErrConflict), errors.Is(err, ErrFilterConflict):
		level, category = slog.LevelWarn, "conflict"
	case errors.Is(err, ErrClosed), errors.Is(err, ErrOwned), errors.Is(err, ErrBound), errors.Is(err, ErrDisabled), errors.Is(err, ErrNotFound):
		level, category = slog.LevelWarn, "unavailable"
	}
	logger.Log(context.Background(), level, "event operation failed", "stage", operation, "failure", category)
}

// LastError 返回最近的后台存储错误；调用方负责对错误内容实施访问控制。
func (s *Store) LastError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.lastErr }

func (s *Store) maintenance() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.CleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if _, err := s.Cleanup(s.ctx); err != nil && !cancelerr.Only(err, s.ctx.Err()) {
				s.recordError("cleanup", err)
			}
		}
	}
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func fromNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// logicalUsage 不包括索引、页碎片、WAL；每份投递预留 2048 字节（含截断错误）。
func logicalUsage(ctx context.Context, tx *sql.Tx) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT SUM(logical_bytes) FROM events),0)
 +COALESCE((SELECT SUM(logical_bytes) FROM subscriptions),0)+(SELECT COUNT(*) FROM deliveries)*2048`).Scan(&n)
	return n, err
}
