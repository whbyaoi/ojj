package replypolicy

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS policy_meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, config TEXT NOT NULL, clock INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS actors (actor TEXT PRIMARY KEY, token TEXT NOT NULL, received INTEGER NOT NULL, occurred INTEGER NOT NULL, sequence INTEGER NOT NULL, mode INTEGER NOT NULL, expires INTEGER NOT NULL, target TEXT NOT NULL DEFAULT '', short_ok INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS reservations (message TEXT PRIMARY KEY, target TEXT NOT NULL, at INTEGER NOT NULL, active INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS reservation_budget ON reservations(target,at);
`

// Open requires a dedicated regular SQLite file. The parent directory must exist
// and be host-controlled. AccountID and BotUserID are immutable for this file.
// Changed policy settings start a new generation; identical settings share it.
// Legacy databases must first be opened with their original configuration, with
// old binaries stopped (they cannot enforce generation fencing).
// State uses hashed identities, never message bodies or raw transport errors.
func Open(ctx context.Context, cfg Config, sender Sender) (*Policy, error) {
	if ctx == nil || nilSender(sender) {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, ErrInvalid
	}
	if info, e := os.Lstat(path); e == nil {
		if !info.Mode().IsRegular() {
			return nil, ErrInvalid
		}
	} else if !os.IsNotExist(e) {
		return nil, ErrStorage
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrStorage
	}
	if err = f.Chmod(0600); err != nil {
		_ = f.Close()
		return nil, ErrStorage
	}
	if err = f.Close(); err != nil {
		return nil, ErrStorage
	}
	params := url.Values{"_txlock": {"immediate"}}
	// DELETE journaling avoids persistent WAL growth; FULL keeps reservations durable.
	for _, pragma := range []string{"busy_timeout=1000", "journal_mode=DELETE", "synchronous=FULL", "max_page_count=131072"} {
		params.Add("_pragma", pragma)
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: params.Encode()}).String())
	if err != nil {
		return nil, ErrStorage
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	life, cancel := context.WithCancel(context.Background())
	p := &Policy{cfg: cfg, sender: sender, db: db, now: time.Now, life: life, cancel: cancel, done: make(chan struct{})}
	fingerprintConfig := cfg
	fingerprintConfig.Path = ""
	legacyFingerprint := configDigest(fingerprintConfig)
	identity := digest(cfg.AccountID, cfg.BotUserID)
	fingerprintConfig.AccountID, fingerprintConfig.BotUserID = "", ""
	// Include candidate semantics, not just settings: a pre-upgrade instance
	// must lose its generation even when the host configuration is unchanged.
	// Keep configDigest unchanged for exact legacy v1/v2 migration checks.
	fingerprint := digest("window-message-candidates-v2", configDigest(fingerprintConfig))
	err = p.transaction(ctx, func(tx *sql.Tx, _ time.Time) error {
		var tables, foreign int
		if e := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(name NOT IN ('policy_meta','actors','reservations')),0) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables, &foreign); e != nil {
			return e
		}
		if foreign != 0 || (tables != 0 && tables != 3) {
			return ErrInvalid
		}
		if _, e := tx.ExecContext(ctx, schema); e != nil {
			return e
		}
		if tables == 0 {
			if _, e := tx.ExecContext(ctx, `INSERT INTO policy_meta VALUES(1,2,?,0)`, legacyFingerprint); e != nil {
				return e
			}
		}
		var version int
		var stored string
		if e := tx.QueryRowContext(ctx, `SELECT version,config FROM policy_meta WHERE id=1`).Scan(&version, &stored); e != nil {
			return e
		}
		if version == 1 || version == 2 {
			// A legacy full-config hash cannot prove identity under a new policy.
			// First open with the original normalized config to establish identity.
			if stored != legacyFingerprint {
				return ErrInvalid
			}
			if version == 1 {
				if _, e := tx.ExecContext(ctx, `ALTER TABLE actors ADD COLUMN target TEXT NOT NULL DEFAULT '';
ALTER TABLE actors ADD COLUMN short_ok INTEGER NOT NULL DEFAULT 0;`); e != nil {
					return e
				}
			}
			if _, e := tx.ExecContext(ctx, `ALTER TABLE policy_meta ADD COLUMN identity TEXT NOT NULL DEFAULT '';
ALTER TABLE policy_meta ADD COLUMN generation INTEGER NOT NULL DEFAULT 1;
UPDATE actors SET mode=0,expires=0,short_ok=0;`); e != nil {
				return e
			}
			if _, e := tx.ExecContext(ctx, `UPDATE policy_meta SET version=3,identity=?,config=? WHERE id=1`, identity, fingerprint); e != nil {
				return e
			}
			stored = fingerprint
		} else if version != 3 {
			return ErrInvalid
		}
		var storedIdentity string
		if e := tx.QueryRowContext(ctx, `SELECT identity,generation FROM policy_meta WHERE id=1`).Scan(&storedIdentity, &p.generation); e != nil {
			return e
		}
		if storedIdentity != identity || p.generation <= 0 {
			return ErrInvalid
		}
		if stored != fingerprint {
			// Never wrap the epoch: A -> B -> A must not revive the first A.
			if p.generation == 1<<63-1 {
				return ErrInvalid
			}
			p.generation++
			if _, e := tx.ExecContext(ctx, `UPDATE policy_meta SET config=?,generation=? WHERE id=1`, fingerprint, p.generation); e != nil {
				return e
			}
			// Keep ordering tombstones, quota, idempotency and the high-water clock.
			if _, e := tx.ExecContext(ctx, `UPDATE actors SET mode=0,expires=0,short_ok=0`); e != nil {
				return e
			}
		}
		_, e := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS actor_target ON actors(target)`)
		return e
	})
	if err != nil {
		cancel()
		_ = db.Close()
		return nil, err
	}
	return p, nil
}

// _txlock=immediate obtains SQLite's writer lock at BEGIN, including across
// independent Policy instances. No network operation is ever inside a transaction.
func (p *Policy) transaction(ctx context.Context, fn func(*sql.Tx, time.Time) error) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return safeError(ctx, ErrStorage)
	}
	defer tx.Rollback()
	// Zero is used only by Open before publishing the instance. Bind (even
	// empty windows), reserve and ACK all fence within the same writer lock.
	if p.generation != 0 {
		var version int
		var generation int64
		if err = tx.QueryRowContext(ctx, `SELECT version,generation FROM policy_meta WHERE id=1`).Scan(&version, &generation); err != nil {
			return safeError(ctx, ErrStorage)
		}
		if version != 3 || generation != p.generation {
			return skipReason("policy_generation_changed")
		}
	}
	if err = fn(tx, p.now()); err == nil {
		err = tx.Commit()
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSkipped) || errors.Is(err, ErrInvalid) {
		return err
	}
	return safeError(ctx, ErrStorage)
}

// A persisted high-water clock prevents time rollback from reviving sessions or
// quota. No state is removed: bounded tombstones keep old IDs non-replayable.
func readClock(ctx context.Context, tx *sql.Tx, now time.Time) error {
	var previous int64
	if err := tx.QueryRowContext(ctx, `SELECT clock FROM policy_meta WHERE id=1`).Scan(&previous); err != nil {
		return err
	}
	if now.UnixNano() < previous {
		return skipReason("clock_rollback")
	}
	return nil
}

func checkClock(ctx context.Context, tx *sql.Tx, now time.Time) error {
	if err := readClock(ctx, tx, now); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE policy_meta SET clock=? WHERE id=1`, now.UnixNano())
	return err
}

func (p *Policy) advance(ctx context.Context, tx *sql.Tx, c *candidate, now time.Time) error {
	if err := checkClock(ctx, tx, now); err != nil {
		return err
	}
	var token string
	var received, occurred, sequence, expires int64
	c.short = c.mode == modeShort
	if c.short {
		c.mode = modeContinue
	}
	var mode, shortOK int
	err := tx.QueryRowContext(ctx, `SELECT token,received,occurred,sequence,mode,expires,short_ok FROM actors WHERE actor=?`, c.actor).
		Scan(&token, &received, &occurred, &sequence, &mode, &expires, &shortOK)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	exists := err == nil
	if exists {
		if c.received < received || (c.received == received && c.sequence < sequence) {
			c.mode = modeSilent
			c.reason = "event_older"
			return nil
		}
		if token == c.token {
			c.expires = expires
			// Binding the same event is not an interaction and never refreshes TTL.
			if c.mode != mode || !c.fresh || (c.short && shortOK == 0) || (mode == modeContinue && expires <= now.UnixNano()) {
				switch {
				case !c.fresh:
					// Retain the window's time diagnosis, even if the clock moved
					// between window classification and this transaction.
				case c.short && shortOK == 0:
					c.reason = "focus_lost"
				case mode == modeContinue && expires <= now.UnixNano():
					c.reason = "session_expired"
				default:
					if c.mode != modeSilent || c.reason == "" {
						c.reason = "candidate_state_changed"
					}
				}
				c.mode = modeSilent
			}
			return nil
		}
		if c.received == received && c.sequence == sequence {
			c.mode = modeSilent
			c.reason = "event_order_conflict"
		}
		if c.occurred < occurred {
			c.mode = modeSilent
			c.reason = "event_time_regressed"
		}
	} else {
		var count int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actors`).Scan(&count); err != nil {
			return err
		}
		if count >= maxActors {
			return skipReason("actor_capacity")
		}
	}
	// A newer message from another actor breaks bare-phrase focus, including
	// across windows and instances. Replayed/older messages cannot steal it.
	if _, err = tx.ExecContext(ctx, `UPDATE actors SET short_ok=0 WHERE target=? AND actor<>? AND (received<? OR (received=? AND sequence<=?))`,
		c.target, c.actor, c.received, c.received, c.sequence); err != nil {
		return err
	}
	var newer int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actors WHERE target=? AND actor<>? AND (received>? OR (received=? AND sequence>=?))`,
		c.target, c.actor, c.received, c.received, c.sequence).Scan(&newer); err != nil {
		return err
	}
	if newer != 0 {
		shortOK = 0
	}
	if c.short && shortOK == 0 {
		c.mode = modeSilent
		c.reason = "focus_lost"
	}
	if timeReason := timeFailureReason(c.received, c.occurred, now); timeReason != "" {
		c.mode = modeSilent
		c.reason = timeReason
	}
	// Explicit interactions may restore focus, but still need a successful ACK
	// to open or refresh the session. A bare phrase cannot restore lost focus.
	if !c.short {
		shortOK = 0
		if newer == 0 && (c.mode == modeDirect || c.mode == modeContinue) {
			shortOK = 1
		}
	}
	if c.mode != modeContinue || expires <= now.UnixNano() || c.occurred >= expires {
		expires = 0
		if c.mode == modeContinue {
			c.mode = modeSilent
			c.reason = "session_expired"
		}
	}
	c.expires = expires
	// Direct/active/new-topic events close any previous session. Only successful
	// direct or continued replies can establish/refresh it in the ACK transaction.
	_, err = tx.ExecContext(ctx, `INSERT INTO actors(actor,token,received,occurred,sequence,mode,expires,target,short_ok) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(actor) DO UPDATE SET token=excluded.token,received=excluded.received,occurred=excluded.occurred,sequence=excluded.sequence,mode=excluded.mode,expires=excluded.expires,target=excluded.target,short_ok=excluded.short_ok`,
		c.actor, c.token, c.received, c.occurred, c.sequence, c.mode, expires, c.target, shortOK)
	return err
}

// checkReservation shares all sending guards without mutating policy state.
func (p *Policy) checkReservation(ctx context.Context, tx *sql.Tx, c candidate, now time.Time) error {
	if err := readClock(ctx, tx, now); err != nil {
		return err
	}
	if timeReason := timeFailureReason(c.received, c.occurred, now); timeReason != "" {
		return skipReason(timeReason)
	}
	var fence actorFence
	var shortOK int
	var expires int64
	if err := tx.QueryRowContext(ctx, `SELECT token,received,occurred,sequence,mode,expires,short_ok FROM actors WHERE actor=?`, c.actor).
		Scan(&fence.token, &fence.received, &fence.occurred, &fence.sequence, &fence.mode, &expires, &shortOK); err != nil {
		return err
	}
	if c.mode == modeSilent || fence != c.fence || fence.mode == modeSilent {
		return skipReason("candidate_superseded")
	}
	if c.short && shortOK == 0 {
		return skipReason("focus_lost")
	}
	if c.mode == modeContinue && (expires <= now.UnixNano() || c.expires <= now.UnixNano()) {
		return skipReason("session_expired")
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reservations WHERE message=?`, c.message).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return skipReason("message_reserved")
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reservations`).Scan(&count); err != nil {
		return err
	}
	if count >= maxReservations {
		return skipReason("reservation_capacity")
	}
	if c.mode != modePrivate {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reservations WHERE target=? AND at>?`, c.target, now.Add(-p.cfg.TotalWindow).UnixNano()).Scan(&count); err != nil {
			return err
		}
		if count >= p.cfg.TotalLimit {
			return skipReason("total_quota")
		}
		if c.mode == modeActive {
			var latest sql.NullInt64
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),MAX(at) FROM reservations WHERE target=? AND active=1 AND at>?`, c.target, now.Add(-p.cfg.ActiveWindow).UnixNano()).Scan(&count, &latest); err != nil {
				return err
			}
			if count >= p.cfg.ActiveLimit {
				return skipReason("active_quota")
			}
			if latest.Valid && now.UnixNano()-latest.Int64 < int64(p.cfg.ActiveInterval) {
				return skipReason("active_interval")
			}
		}
	}
	return nil
}

func (p *Policy) reserve(ctx context.Context, tx *sql.Tx, c candidate, now time.Time) error {
	if err := checkClock(ctx, tx, now); err != nil {
		return err
	}
	if err := p.checkReservation(ctx, tx, c, now); err != nil {
		return err
	}
	active := 0
	if c.mode == modeActive {
		active = 1
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO reservations VALUES(?,?,?,?)`, c.message, c.target, now.UnixNano(), active)
	return err
}
