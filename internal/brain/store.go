package brain

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"ojj/internal/minipi"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Region requires a local filesystem, one owner, and no hard-link aliases.
// Never replace database/lock files while open. Callbacks must honor cancellation
// and must not synchronously call Close. Close waits for callbacks to return.
type Region struct {
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
CREATE TABLE brain_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
 region_id TEXT NOT NULL, stored_bytes INTEGER NOT NULL CHECK(stored_bytes>=0)
);
CREATE TABLE inputs (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
 text TEXT NOT NULL, status TEXT NOT NULL, received_at INTEGER NOT NULL,
 started_at INTEGER NOT NULL DEFAULT 0, completed_at INTEGER NOT NULL DEFAULT 0,
 last_error TEXT NOT NULL DEFAULT '', turns INTEGER NOT NULL DEFAULT 0,
 input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
 stop_reason TEXT NOT NULL DEFAULT '',
 qq_delivery TEXT NOT NULL DEFAULT '' CHECK(qq_delivery IN ('','not_sent','unknown','sent'))
);
CREATE INDEX inputs_queue ON inputs(status,sequence);
CREATE TABLE messages (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, input_id TEXT NOT NULL REFERENCES inputs(id),
 message BLOB NOT NULL, turn INTEGER NOT NULL, input_tokens INTEGER NOT NULL,
 output_tokens INTEGER NOT NULL, stop_reason TEXT NOT NULL, error TEXT NOT NULL,
 created_at INTEGER NOT NULL
);
CREATE INDEX messages_input ON messages(input_id,sequence);
`

func Open(ctx context.Context, config Config) (region *Region, err error) {
	started := time.Now()
	var recovered int64
	defer func() {
		minipi.LogOperation(ctx, "brain", "open", started, err, slog.LevelInfo)
		if err == nil && recovered > 0 {
			minipi.LogOperation(ctx, "brain", "recover", started, nil, slog.LevelInfo,
				slog.Int64("count", recovered))
		}
	}()
	cfg, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, ErrInvalid
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
	fail := func(e error) (*Region, error) { return nil, errors.Join(e, db.Close(), f.Close()) }
	op, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
	defer cancel()
	tx, err := db.BeginTx(op, nil)
	if err != nil {
		return fail(err)
	}
	var tables, own, foreign int
	err = tx.QueryRowContext(op, `SELECT COUNT(*),COALESCE(SUM(name='brain_meta'),0),COALESCE(SUM(name NOT IN ('brain_meta','inputs','messages')),0) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables, &own, &foreign)
	if err == nil && tables > 0 && (own != 1 || tables != 3 || foreign != 0) {
		err = ErrIdentity
	}
	if err == nil && tables == 0 {
		_, err = tx.ExecContext(op, schema)
		if err == nil {
			_, err = tx.ExecContext(op, "INSERT INTO brain_meta VALUES(1,2,?,0)", cfg.ID)
		}
	}
	if err == nil {
		var version int
		var id string
		err = tx.QueryRowContext(op, "SELECT version,region_id FROM brain_meta WHERE id=1").Scan(&version, &id)
		if err != nil || (version != 1 && version != 2) || id != cfg.ID {
			err = errors.Join(ErrIdentity, err)
		}
		if err == nil && version == 1 {
			// Old inputs have no reliable delivery evidence. Keep them untracked.
			// Identity validation and migration share the opening transaction.
			_, err = tx.ExecContext(op, `ALTER TABLE inputs ADD COLUMN qq_delivery TEXT NOT NULL DEFAULT '' CHECK(qq_delivery IN ('','not_sent','unknown','sent'))`)
			if err == nil {
				_, err = tx.ExecContext(op, `UPDATE brain_meta SET version=2 WHERE id=1`)
			}
		}
	}
	if err == nil {
		var result sql.Result
		result, err = tx.ExecContext(op, `UPDATE inputs SET status='interrupted',completed_at=?,last_error='interrupted during previous ownership',stop_reason='canceled' WHERE status='processing'`, time.Now().UnixNano())
		if err == nil {
			recovered, _ = result.RowsAffected()
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
	return &Region{db: db, file: f, cfg: cfg, ctx: root, cancel: stop, closeDone: make(chan struct{})}, nil
}

func (r *Region) operation(ctx context.Context) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, ErrClosed
	}
	if ctx == nil {
		return nil, nil, ErrInvalid
	}
	r.wg.Add(1)
	op, cancel := context.WithTimeout(ctx, r.cfg.OperationTimeout)
	stop := context.AfterFunc(r.ctx, cancel)
	return op, func() { stop(); cancel(); r.wg.Done() }, nil
}

// Close is concurrent/idempotent and releases the owner lock only after all work.
func (r *Region) Close() error {
	started := time.Now()
	r.mu.Lock()
	if r.closed {
		done := r.closeDone
		r.mu.Unlock()
		<-done
		return r.closeErr
	}
	r.closed = true
	r.cancel()
	r.mu.Unlock()
	r.wg.Wait()
	err := errors.Join(r.db.Close(), r.file.Close())
	r.mu.Lock()
	r.closeErr = err
	close(r.closeDone)
	r.mu.Unlock()
	minipi.LogOperation(nil, "brain", "close", started, err, slog.LevelInfo)
	return err
}

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

func fromNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
