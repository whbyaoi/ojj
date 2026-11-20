package messagebuilder

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math"
	"ojj/internal/minipi"

	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Builder 仅支持本地文件系统、单进程所有者，不支持数据库硬链接别名。
// 使用期间不得复制/替换数据库及伴随文件，不得删除/替换独立 .lock 文件。
// 单 SQL 连接使 Handle、收束、领取事务串行；回调从不持有连接或事务。
// Open 不启动调度，调用 Run 后才周期收束和投递；Handle 自身也检查到期窗口。
type Builder struct {
	db        *sql.DB
	file      *os.File
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	running   bool
	wg        sync.WaitGroup
	closeDone chan struct{}
	closeErr  error
}

const schema = `
CREATE TABLE IF NOT EXISTS builder_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, event_count INTEGER NOT NULL
);
INSERT OR IGNORE INTO builder_meta VALUES(1,1,0);
CREATE TABLE IF NOT EXISTS messages (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
 text TEXT NOT NULL, event_ids TEXT NOT NULL,
 opened_at INTEGER NOT NULL, closed_at INTEGER NOT NULL, created_at INTEGER NOT NULL,
 reason TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 next_at INTEGER NOT NULL DEFAULT 0, started_at INTEGER NOT NULL DEFAULT 0,
 completed_at INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS messages_queue ON messages(status,sequence);
CREATE TABLE IF NOT EXISTS inbox (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
 digest BLOB NOT NULL, snapshot BLOB NOT NULL, received_at INTEGER NOT NULL,
 message_sequence INTEGER REFERENCES messages(sequence)
);
CREATE INDEX IF NOT EXISTS inbox_window ON inbox(message_sequence,sequence);
CREATE TABLE IF NOT EXISTS active_window (
 id INTEGER PRIMARY KEY CHECK(id=1), opened_at INTEGER NOT NULL,
 last_at INTEGER NOT NULL, deadline INTEGER NOT NULL, max_at INTEGER NOT NULL,
 quiet_ns INTEGER NOT NULL, event_count INTEGER NOT NULL, byte_count INTEGER NOT NULL,
 max_events INTEGER NOT NULL, max_bytes INTEGER NOT NULL
);
`

func Open(ctx context.Context, config Config) (builder *Builder, err error) {
	started := time.Now()
	var recovered int64
	defer func() {
		minipi.LogOperation(ctx, "messagebuilder", "open", started, err, slog.LevelInfo)
		if err == nil && recovered > 0 {
			minipi.LogOperation(ctx, "messagebuilder", "recover", started, nil, slog.LevelInfo, slog.Int64("count", recovered))
		}
	}()
	cfg, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if strings.HasPrefix(cfg.Path, "file:") || cfg.Path == ":memory:" {
		return nil, ErrInvalid
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
		return nil, errors.Join(ErrInvalid, f.Close())
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	// 独立锁避免 macOS flock 与 SQLite 数据库文件锁相互干扰。
	f, err = os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, err = f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(ErrInvalid, f.Close())
	}
	if err = lockFile(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if err = errors.Join(f.Chmod(0600), os.Chmod(path, 0600)); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	params := url.Values{}
	// database/sql 重建连接时也应用全部 PRAGMA。
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
	fail := func(e error) (*Builder, error) { return nil, errors.Join(e, db.Close(), f.Close()) }
	op, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	tx, err := db.BeginTx(op, nil)
	if err != nil {
		return fail(err)
	}
	// 拒绝误用 event 或其他已有数据库，不修改它们的表。
	var tables, own int
	err = tx.QueryRowContext(op, `SELECT COUNT(*),COALESCE(SUM(name='builder_meta'),0) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables, &own)
	if err == nil && tables > 0 && own == 0 {
		err = ErrInvalid
	}
	if err == nil {
		_, err = tx.ExecContext(op, schema)
	}
	if err == nil {
		var version int
		err = tx.QueryRowContext(op, "SELECT version FROM builder_meta WHERE id=1").Scan(&version)
		if err == nil {
			switch version {
			case 1:
				// Existing sealed messages retain their original text-only payload,
				// even if delivery was interrupted. Never change retry identity.
				_, err = tx.ExecContext(op, `ALTER TABLE messages ADD COLUMN images TEXT NOT NULL DEFAULT '[]'; UPDATE builder_meta SET version=2 WHERE id=1`)
				if err == nil {
					err = migrateWindowCost(op, tx)
				}
			case 2:
			default:
				err = ErrInvalid
			}
		}
	}
	if err == nil {
		var result sql.Result
		result, err = tx.ExecContext(op, `UPDATE messages SET status='retry',next_at=?,last_error='interrupted' WHERE status='processing'`, time.Now().UnixNano())
		if err == nil {
			recovered, _ = result.RowsAffected() // Diagnostic only; do not alter recovery semantics.
		}
	}
	if err != nil {
		rollback(tx, &err)
		return fail(err)
	}
	if err = commit(op, tx); err != nil {
		return fail(err)
	}
	root, stop := context.WithCancel(context.Background())
	return &Builder{db: db, file: f, cfg: cfg, ctx: root, cancel: stop, closeDone: make(chan struct{})}, nil
}

// operation 在 mu 内登记关闭等待，避免 Add 与 Close.Wait 竞态。
func (b *Builder) operation(ctx context.Context) (context.Context, func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, nil, ErrClosed
	}
	b.wg.Add(1)
	op, cancel := context.WithTimeout(ctx, b.cfg.OperationTimeout)
	stop := context.AfterFunc(b.ctx, cancel)
	return op, func() { stop(); cancel(); b.wg.Done() }, nil
}

// Close 可重复并发调用；取消调度并等待全部操作和回调退出后才关库、释放锁。
// 不强制收束未到期窗口。不合作回调会使 Close 等待，不得从回调同步调用。
func (b *Builder) Close() error {
	started := time.Now()
	b.mu.Lock()
	if b.closed {
		done := b.closeDone
		b.mu.Unlock()
		<-done
		return b.closeErr
	}
	b.closed = true
	b.cancel()
	b.mu.Unlock()
	b.wg.Wait()
	err := errors.Join(b.db.Close(), b.file.Close())
	b.mu.Lock()
	b.closeErr = err
	close(b.closeDone)
	b.mu.Unlock()
	minipi.LogOperation(context.Background(), "messagebuilder", "close", started, err, slog.LevelInfo)
	return err
}

// commit 保留真实提交错误，仅归一化 database/sql 自动回滚导致的 ErrTxDone。
func commit(ctx context.Context, tx *sql.Tx) error {
	err := tx.Commit()
	if err == sql.ErrTxDone && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func rollback(tx *sql.Tx, err *error) {
	if e := tx.Rollback(); e != nil && e != sql.ErrTxDone {
		*err = errors.Join(*err, e)
	}
}

// addNanos 对正时长饱和相加，避免极大配置把截止时间溢出到过去。
func addNanos(now, duration int64) int64 {
	if now > math.MaxInt64-duration {
		return math.MaxInt64
	}
	return now + duration
}

func fromNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
