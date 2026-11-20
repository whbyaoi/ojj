package messagebuilder

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/imagecache"
	"ojj/internal/minipi"
)

func imageTestEvent(t *testing.T, id string, attachments ...qq.Attachment) event.Event {
	t.Helper()
	return formatTestEvent(t, id, "image-test", qq.Content{Version: 1, Attachments: attachments})
}

type prefetchResolverFunc func(context.Context, string) (imagecache.Image, error)

func (f prefetchResolverFunc) Resolve(ctx context.Context, raw string) (imagecache.Image, error) {
	return f(ctx, raw)
}

func TestImagePrefetchAfterCommit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			var b *Builder
			calls := 0
			url := "https://example.invalid/prefetch.png"
			resolver := prefetchResolverFunc(func(ctx context.Context, raw string) (imagecache.Image, error) {
				calls++
				// Querying the single-connection pool requires the transaction to be released.
				ms, err := b.List(ctx, MessageQuery{})
				if err != nil || len(ms) != 1 || len(ms[0].Images) != 1 || raw != url {
					t.Error("prefetch preceded durable snapshot")
				}
				if fail {
					return imagecache.Image{}, errors.New("private-download-error")
				}
				return imagecache.Image{Data: []byte("private-image-bytes"), MediaType: "image/png"}, nil
			})
			b = testBuilder(t, Config{MaxWindowEvents: 1, ImageCache: resolver})
			e := imageTestEvent(t, "prefetch", qq.Attachment{URL: url})
			acceptAt(t, b, e, time.Now())
			acceptAt(t, b, e, time.Now())
			if calls != 1 {
				t.Fatal("duplicate event repeated prefetch")
			}
			ms := listMessages(t, b)
			if len(ms) != 1 || len(ms[0].Images) != 1 || ms[0].Images[0].URL != url || strings.Contains(ms[0].Text, "private-") {
				t.Fatal("cache result changed durable message")
			}
			cfg := b.cfg
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			b = testBuilder(t, cfg)
			if !reflect.DeepEqual(listMessages(t, b), ms) {
				t.Fatal("prefetch changed persisted history")
			}
		})
	}
}

func TestImagePrefetchCloseCancellation(t *testing.T) {
	entered := make(chan struct{})
	resolver := prefetchResolverFunc(func(ctx context.Context, _ string) (imagecache.Image, error) {
		close(entered)
		<-ctx.Done()
		return imagecache.Image{}, ctx.Err()
	})
	b := testBuilder(t, Config{MaxWindowEvents: 1, ImageCache: resolver})
	e := imageTestEvent(t, "cancel-prefetch", qq.Attachment{URL: "https://example.invalid/a.png"})
	result := make(chan error, 1)
	go func() { result <- b.Handle(context.Background(), e) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("prefetch did not start")
	}
	cfg := b.cfg
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel prefetch")
	}
	if err := <-result; err != nil {
		t.Fatal("committed event rejected after prefetch cancellation")
	}
	b = testBuilder(t, cfg)
	if len(listMessages(t, b)) != 1 {
		t.Fatal("cancellation lost committed event")
	}
}

func TestImageAttachmentRules(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    qq.Attachment
		want bool
	}{
		{"mime", qq.Attachment{URL: "https://example.invalid/blob", ContentType: "IMAGE/PNG; charset=binary"}, true},
		{"filename", qq.Attachment{URL: "https://example.invalid/blob", Filename: "photo.JPEG"}, true},
		{"binary", qq.Attachment{URL: "https://example.invalid/a.WeBP?token=x", ContentType: "application/octet-stream"}, true},
		{"path", qq.Attachment{URL: "http://example.invalid/a.avif"}, true},
		{"non-image", qq.Attachment{URL: "https://example.invalid/a.png", ContentType: "text/plain"}, false},
		{"invalid-mime", qq.Attachment{URL: "https://example.invalid/a.png", ContentType: "image/png; broken"}, false},
		{"voice", qq.Attachment{URL: "https://example.invalid/a.png", ContentType: "image/png", VoiceWAVURL: "voice"}, false},
		{"query-only", qq.Attachment{URL: "https://example.invalid/blob?name=a.png"}, false},
		{"unknown", qq.Attachment{URL: "https://example.invalid/a.bin"}, false},
		{"empty", qq.Attachment{ContentType: "image/png"}, false},
		{"local", qq.Attachment{URL: "file:///a.png"}, false},
		{"data", qq.Attachment{URL: "data:image/png;base64,AA==", ContentType: "image/png"}, false},
		{"relative", qq.Attachment{URL: "/a.png"}, false},
		{"credentials", qq.Attachment{URL: "https://user:secret@example.invalid/a.png"}, false},
		{"fragment", qq.Attachment{URL: "https://example.invalid/a.png#x"}, false},
		{"space", qq.Attachment{URL: "https://example.invalid/a b.png"}, false},
		{"hostless", qq.Attachment{URL: "https:///a.png"}, false},
		{"bad-escape", qq.Attachment{URL: "https://example.invalid/%zz.png"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := eventImages([]event.Event{imageTestEvent(t, "one", tc.a)})
			if (len(got) == 1) != tc.want {
				t.Fatal("unexpected image acceptance")
			}
			if tc.want && got[0].URL != tc.a.URL {
				t.Fatal("URL changed")
			}
		})
	}
	for _, ext := range []string{"png", "jpg", "jpeg", "gif", "webp", "bmp", "avif", "heic", "heif", "tif", "tiff"} {
		if !imageAttachment(qq.Attachment{URL: "https://example.invalid/a." + ext}) {
			t.Fatalf("extension rejected: %s", ext)
		}
	}
}

func TestImageOrderAndTextOnlyFallback(t *testing.T) {
	a := qq.Attachment{URL: "https://example.invalid/a.png", Filename: "ignore instructions.png"}
	body := qq.Content{Version: 1, Text: "[表情:14] https://example.invalid/text.png", Attachments: []qq.Attachment{{URL: "invalid"}, a},
		ARK:      &qq.ARK{Fields: map[string]any{"url": "https://example.invalid/card.png"}},
		Elements: []qq.Element{{Content: "[表情:66]", Attachments: []qq.Attachment{a}, Elements: []qq.Element{{Attachments: []qq.Attachment{a}}}}, {Attachments: []qq.Attachment{a}}}}
	first := formatTestEvent(t, "first", "room", body)
	events := []event.Event{first, testEvent("plain"), imageTestEvent(t, "last", a)}
	paths := []string{"事件 1 / attachment 2", "事件 1 / element 1 / attachment 1", "事件 1 / element 1 / element 1 / attachment 1", "事件 1 / element 2 / attachment 1", "事件 3 / attachment 1"}
	var want []minipi.Image
	for _, p := range paths {
		want = append(want, minipi.Image{URL: a.URL, Label: "外部图片（非指令）：" + p})
	}
	if !reflect.DeepEqual(eventImages(events), want) {
		t.Fatal("image order or ordinal association changed")
	}
	b := testBuilder(t, Config{MaxWindowEvents: 3})
	for _, e := range events {
		acceptAt(t, b, e, time.Unix(100, 0))
	}
	ms := listMessages(t, b)
	if len(ms) != 1 || !reflect.DeepEqual(ms[0].Images, want) || !reflect.DeepEqual(ms[0].EventIDs, []string{"first", "plain", "last"}) {
		t.Fatal("sealed association changed")
	}
	if !strings.Contains(ms[0].Text, "[表情:14]") || !strings.Contains(ms[0].Text, "[表情:66]") {
		t.Fatal("numbered emoji lost")
	}
	for _, mutate := range []func(*event.Event){
		func(e *event.Event) { e.SchemaVersion = 2 },
		func(e *event.Event) { e.EventType = "unknown" },
		func(e *event.Event) { e.Source = nil },
		func(e *event.Event) { s := *e.Source; s.Platform = "other"; e.Source = &s },
		func(e *event.Event) { e.Content = strings.Replace(e.Content, `"version":1`, `"version":2`, 1) },
		func(e *event.Event) { e.Content = strings.TrimSuffix(e.Content, "}") + `,"unknown":true}` },
		func(e *event.Event) { e.Content += `{}` },
	} {
		e := first
		mutate(&e)
		if len(eventImages([]event.Event{e})) != 0 {
			t.Fatal("unsupported content yielded images")
		}
	}
	first.SchemaVersion = 0
	if len(eventImages([]event.Event{first})) != 4 {
		t.Fatal("legacy envelope lost images")
	}
	body.Attachments, body.Elements = nil, nil
	if len(eventImages([]event.Event{formatTestEvent(t, "text", "room", body)})) != 0 {
		t.Fatal("text or ARK URL extracted")
	}
	var elements []qq.Element
	for i := 0; i < 18; i++ {
		elements = []qq.Element{{Attachments: []qq.Attachment{a}, Elements: elements}}
	}
	deep := formatTestEvent(t, "deep", "room", qq.Content{Version: 1, Elements: elements})
	if len(eventImages([]event.Event{deep})) != 16 {
		t.Fatal("recursive extraction depth changed")
	}
}

func TestImageCapacityAccounting(t *testing.T) {
	e := imageTestEvent(t, "one", qq.Attachment{URL: "https://example.invalid/a.png"})
	raw := formatTestJSON(t, e)
	images := formatTestJSON(t, eventImages([]event.Event{e}))
	want := int64(len(raw) + len(images) + 19)
	cost, err := eventCost(e, len(raw))
	if err != nil || cost != want {
		t.Fatal("image cost mismatch")
	}
	plain := testEvent("plain")
	plainRaw := formatTestJSON(t, plain)
	if n, err := eventCost(plain, len(plainRaw)); err != nil || n != int64(len(plainRaw)) {
		t.Fatal("plain text cost changed")
	}
	for _, cfg := range []Config{{MaxEventBytes: len(raw)}, {MaxWindowBytes: len(raw)}} {
		b := testBuilder(t, cfg)
		if err := b.handle(context.Background(), e, clockAt(time.Now())); !errors.Is(err, ErrCapacity) {
			t.Fatal("image overhead ignored")
		}
		var n int
		if err := b.db.QueryRow("SELECT event_count FROM builder_meta").Scan(&n); err != nil || n != 0 {
			t.Fatal("rejected event persisted")
		}
	}
	b := testBuilder(t, Config{MaxEventBytes: int(want), MaxWindowBytes: int(want)})
	acceptAt(t, b, e, time.Now())
	if ms := listMessages(t, b); len(ms) != 1 || ms[0].CloseReason != ReasonCapacity {
		t.Fatal("exact capacity did not seal")
	}
	cfg := b.cfg
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MaxEventBytes, cfg.MaxWindowBytes = 1, 1
	b = testBuilder(t, cfg)
	acceptAt(t, b, e, time.Now())
	if len(listMessages(t, b)) != 1 {
		t.Fatal("duplicate changed window")
	}
	b = testBuilder(t, Config{MaxWindowBytes: int(want * 2)})
	acceptAt(t, b, e, time.Now())
	e.ID = "two"
	acceptAt(t, b, e, time.Now())
	if ms := listMessages(t, b); len(ms) != 1 || len(ms[0].Images) != 2 || ms[0].CloseReason != ReasonCapacity {
		t.Fatal("multi-event image accounting mismatch")
	}
}

func imageTestV1(t *testing.T, e event.Event, maxBytes int64) (string, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(schema)
	at := time.Unix(100, 0).UTC()
	raw := []byte(formatTestJSON(t, e))
	digest := sha256.Sum256(raw)
	for i, status := range []Status{StatusPending, StatusRetry, StatusProcessing, StatusSucceeded} {
		exec(`INSERT INTO messages(id,text,event_ids,opened_at,closed_at,created_at,reason,status,attempts,next_at,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("legacy-%d", i), "original text", `["old"]`, at.UnixNano(), at.UnixNano(), at.UnixNano(), ReasonQuiet, status, 3, at.Add(time.Hour).UnixNano(), "handler_failed")
		exec(`INSERT INTO inbox(id,digest,snapshot,received_at,message_sequence) VALUES(?,?,?,?,?)`, fmt.Sprintf("old-%d", i), digest[:], raw, at.UnixNano(), i+1)
	}
	exec(`INSERT INTO inbox(id,digest,snapshot,received_at) VALUES(?,?,?,?)`, e.ID, digest[:], raw, at.UnixNano())
	exec(`UPDATE builder_meta SET event_count=5`)
	exec(`INSERT INTO active_window VALUES(1,?,?,?,?,?,?,?,?,?)`, at.UnixNano(), at.UnixNano(), at.Add(2*time.Second).UnixNano(), at.Add(10*time.Second).UnixNano(), int64(2*time.Second), 1, len(raw), 256, maxBytes)
	return path, at
}

func TestImageV1MigrationAndActiveCost(t *testing.T) {
	e := imageTestEvent(t, "active", qq.Attachment{URL: "https://example.invalid/a.png"})
	cost, _ := eventCost(e, len(formatTestJSON(t, e)))
	for _, mode := range []string{"under-limit", "over-limit-settle", "over-limit-handle"} {
		t.Run(mode, func(t *testing.T) {
			limit := cost * 10
			if mode != "under-limit" {
				limit = cost - 1
			}
			path, at := imageTestV1(t, e, limit)
			b := testBuilder(t, Config{Path: path, QuietPeriod: time.Hour, MaxWindowBytes: 1 << 20})
			if b.db.Stats().MaxOpenConnections != 1 {
				t.Fatal("store is not single-connection")
			}
			ms := listMessages(t, b)
			if len(ms) != 4 {
				t.Fatal("legacy messages missing")
			}
			for i, m := range ms {
				wantStatus := []Status{StatusPending, StatusRetry, StatusRetry, StatusSucceeded}[i]
				if m.ID != fmt.Sprintf("legacy-%d", i) || m.Text != "original text" || len(m.Images) != 0 || m.Attempts != 3 || m.Status != wantStatus || !reflect.DeepEqual(m.EventIDs, []string{"old"}) {
					t.Fatal("legacy payload or identity changed")
				}
				if i == 2 {
					if m.LastError != "interrupted" {
						t.Fatal("processing not recovered")
					}
				} else if !m.NextAttemptAt.Equal(at.Add(time.Hour)) {
					t.Fatal("retry schedule changed")
				}
			}
			var version int
			var size, maxBytes, quiet int64
			if err := b.db.QueryRow("SELECT version FROM builder_meta").Scan(&version); err != nil || version != 2 {
				t.Fatal("migration version mismatch")
			}
			if err := b.db.QueryRow("SELECT byte_count,max_bytes,quiet_ns FROM active_window").Scan(&size, &maxBytes, &quiet); err != nil || size != cost || maxBytes != limit || quiet != int64(2*time.Second) {
				t.Fatal("migration cost or policy mismatch")
			}
			acceptAt(t, b, e, at)
			switch mode {
			case "under-limit":
				settleAt(t, b, at.Add(time.Second))
				if len(listMessages(t, b)) != 4 {
					t.Fatal("under-limit window sealed early")
				}
				next := e
				next.ID = "second"
				acceptAt(t, b, next, at.Add(time.Second))
				if err := b.db.QueryRow("SELECT byte_count FROM active_window").Scan(&size); err != nil || size != cost*2 {
					t.Fatal("new and migrated accounting differ")
				}
				settleAt(t, b, at.Add(3*time.Second))
			case "over-limit-settle":
				settleAt(t, b, at.Add(-time.Second))
			case "over-limit-handle":
				acceptAt(t, b, testEvent("next"), at.Add(time.Second))
			}
			ms = listMessages(t, b)
			if len(ms) != 5 || len(ms[4].Images) == 0 || ms[4].ClosedAt.Before(ms[4].OpenedAt) {
				t.Fatal("migrated active window lost")
			}
			if mode != "under-limit" && ms[4].CloseReason != ReasonCapacity {
				t.Fatal("over-limit window not safely sealed")
			}
		})
	}
}

func TestImageMigrationRollbackAndVersionCompatibility(t *testing.T) {
	e := imageTestEvent(t, "active", qq.Attachment{URL: "https://example.invalid/a.png"})
	for _, tc := range []struct {
		name, query string
		version     int
	}{
		{"bad-snapshot", `UPDATE inbox SET snapshot='{' WHERE message_sequence IS NULL`, 1},
		{"wrong-count", `UPDATE active_window SET event_count=2`, 1},
		{"future", `UPDATE builder_meta SET version=3`, 3},
		{"unknown", `UPDATE builder_meta SET version=0`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := imageTestV1(t, e, 100000)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			defer db.Close()
			if _, err := db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			b, err := Open(context.Background(), Config{Path: path})
			if b != nil {
				b.Close()
				t.Fatal("invalid database accepted")
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatal("expected invalid database")
			}
			var version, columns int
			var status string
			if err := db.QueryRow("SELECT version FROM builder_meta").Scan(&version); err != nil || version != tc.version {
				t.Fatal("version modified after failure")
			}
			if err := db.QueryRow("SELECT count(*) FROM pragma_table_info('messages') WHERE name='images'").Scan(&columns); err != nil || columns != 0 {
				t.Fatal("schema migration not rolled back")
			}
			if err := db.QueryRow("SELECT status FROM messages WHERE id='legacy-2'").Scan(&status); err != nil || status != "processing" {
				t.Fatal("failed open changed delivery status")
			}
		})
	}
}

func TestImageReopenAndDeliverySnapshot(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 1, PollInterval: time.Millisecond, RetryInitial: time.Millisecond})
	e := imageTestEvent(t, "image", qq.Attachment{URL: "https://example.invalid/a.png"})
	acceptAt(t, b, e, time.Now())
	original := listMessages(t, b)[0]
	claimed, found, err := b.claim(context.Background())
	if err != nil || !found || !reflect.DeepEqual(claimed.Images, original.Images) {
		t.Fatal("claim lost image snapshot")
	}
	cfg := b.cfg
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = testBuilder(t, cfg)
	recovered, err := b.Get(context.Background(), original.ID)
	if err != nil || recovered.Status != StatusRetry || recovered.LastError != "interrupted" || recovered.Text != original.Text || !reflect.DeepEqual(recovered.Images, original.Images) {
		t.Fatal("recovery changed image snapshot")
	}
	claimed, found, err = b.claim(context.Background())
	if err != nil || !found {
		t.Fatal("recovery could not be claimed")
	}
	if err := b.finish(claimed, errors.New("test failure")); err != nil {
		t.Fatal(err)
	}
	retry, err := b.Get(context.Background(), original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = testBuilder(t, cfg)
	reopened, err := b.Get(context.Background(), original.ID)
	if err != nil || !reflect.DeepEqual(reopened, retry) {
		t.Fatal("v2 reopen changed retry snapshot")
	}
	cancel, result := startRun(t, b, func(ctx context.Context, m Message) error {
		stored, err := b.Get(ctx, m.ID)
		if err != nil {
			return err
		}
		if m.ID != original.ID || m.Text != original.Text || !reflect.DeepEqual(m.Images, original.Images) || !reflect.DeepEqual(stored.Images, m.Images) {
			t.Error("delivery snapshot mismatch")
		}
		return nil
	})
	waitFor(t, func() bool {
		m, err := b.Get(context.Background(), original.ID)
		return err == nil && m.Status == StatusSucceeded
	})
	cancel()
	if err := waitRun(t, result); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestImageSnapshotValidationAndEmptyArray(t *testing.T) {
	b := testBuilder(t, Config{MaxWindowEvents: 1})
	acceptAt(t, b, testEvent("plain"), time.Now())
	m := listMessages(t, b)[0]
	var encoded string
	if err := b.db.QueryRow("SELECT images FROM messages WHERE id=?", m.ID).Scan(&encoded); err != nil || encoded != "[]" {
		t.Fatal("empty snapshot is not an array")
	}
	for _, bad := range []string{`{`, `{}`, `[{"url":"file:///local.png"}]`, `[{"url":"https://user:secret@example.invalid/a.png"}]`} {
		if _, err := b.db.Exec("UPDATE messages SET images=? WHERE id=?", bad, m.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Get(context.Background(), m.ID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid image snapshot readable")
		}
		if _, err := b.List(context.Background(), MessageQuery{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid image snapshot listed")
		}
		if _, _, err := b.claim(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid image snapshot deliverable")
		}
	}
}
