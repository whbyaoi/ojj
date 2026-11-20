package messagebuilder

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"ojj/internal/cancelerr"
)

func TestConnectionPragmasSurviveReconnect(t *testing.T) {
	b := testBuilder(t, Config{})
	b.db.SetMaxIdleConns(0)
	for range 2 {
		conn, err := b.db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"foreign_keys": "1", "journal_mode": "wal", "synchronous": "2", "busy_timeout": "1000"} {
			var got string
			if err := conn.QueryRowContext(context.Background(), "PRAGMA "+key).Scan(&got); err != nil || got != want {
				t.Errorf("pragma %s: %s, %v", key, got, err)
			}
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRejectForeignDatabaseWithoutChangingTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE original(value TEXT); INSERT INTO original VALUES('preserved')"); err != nil {
		t.Fatal(err)
	}
	b, err := Open(context.Background(), Config{Path: path})
	if b != nil {
		b.Close()
		t.Fatal("foreign database accepted")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	var count int
	var value string
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count); err != nil || count != 1 {
		t.Fatal("foreign tables changed", err)
	}
	if err := db.QueryRow("SELECT value FROM original").Scan(&value); err != nil || value != "preserved" {
		t.Fatal("foreign data changed", err)
	}
}

func TestHugeDurationsDoNotOverflow(t *testing.T) {
	b := testBuilder(t, Config{QuietPeriod: math.MaxInt64, MaxWindow: math.MaxInt64, RetryInitial: math.MaxInt64, RetryMax: math.MaxInt64})
	start := time.Now()
	acceptAt(t, b, testEvent("one"), start)
	settleAt(t, b, start.Add(time.Hour))
	if len(listMessages(t, b)) != 0 {
		t.Fatal("duration overflow closed window")
	}
	settleAt(t, b, time.Unix(0, math.MaxInt64))
	m, found, err := b.claim(context.Background())
	if err != nil || !found {
		t.Fatal(err)
	}
	if err := b.finish(m, errors.New("failure")); err != nil {
		t.Fatal(err)
	}
	m, err = b.Get(context.Background(), m.ID)
	if err != nil || m.NextAttemptAt.UnixNano() != math.MaxInt64 {
		t.Fatal("retry overflow", err)
	}
}

func TestCloseRunCancellationRace(t *testing.T) {
	for range 20 {
		b := testBuilder(t, Config{PollInterval: time.Nanosecond})
		_, result := startRun(t, b, nil)
		waitFor(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.running })
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if err := waitRun(t, result); !cancelerr.Only(err, context.Canceled) {
			t.Fatal("close did not preserve pure cancellation", err)
		}
	}
}

func TestValidationAndQueries(t *testing.T) {
	for _, cfg := range []Config{{}, {Path: "db", QuietPeriod: -1}, {Path: "db", MaxWindowEvents: -1}, {Path: "db", RetryInitial: time.Hour, RetryMax: time.Second}} {
		if !errors.Is(cfg.Validate(), ErrInvalid) {
			t.Fatal("invalid config accepted")
		}
	}
	b := testBuilder(t, Config{MaxWindowEvents: 1})
	e := testEvent("invalid")
	e.Content = string([]byte{0xff})
	if !errors.Is(b.Handle(context.Background(), e), ErrInvalid) {
		t.Fatal("invalid UTF-8 accepted")
	}
	acceptAt(t, b, testEvent("one"), time.Now())
	acceptAt(t, b, testEvent("two"), time.Now())
	first, err := b.List(context.Background(), MessageQuery{Limit: 1, Status: StatusPending})
	if err != nil || len(first) != 1 {
		t.Fatal("query limit", err)
	}
	next, err := b.List(context.Background(), MessageQuery{AfterSequence: first[0].Sequence})
	if err != nil || len(next) != 1 || next[0].Sequence <= first[0].Sequence {
		t.Fatal("query cursor", err)
	}
	for _, q := range []MessageQuery{{Limit: 1001}, {Limit: -1}, {Status: "invalid"}, {AfterSequence: -1}} {
		if _, err := b.List(context.Background(), q); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid query accepted", err)
		}
	}
}
