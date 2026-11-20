package imagecache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testURL = "https://gchat.qpic.cn/image?token=test-secret"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func pngBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewNRGBA(image.Rect(0, 0, 2, 3))); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func cacheName(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:]) + ".img"
}

func response(data []byte) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
		ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}
}

func noNetwork(t *testing.T, c *Cache) {
	t.Helper()
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("unexpected network access")
		return nil, errors.New("network disabled")
	})
}

func requireUnavailable(t *testing.T, img Image, err error) {
	t.Helper()
	if err != ErrUnavailable || len(img.Data) != 0 || img.MediaType != "" {
		t.Fatalf("expected sanitized failure, got image bytes=%d, error=%v", len(img.Data), err)
	}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestDownloadPersistAndReuse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	c := New(dir)
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("New must not touch disk")
	}
	data := pngBytes(t)
	calls := 0
	c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodGet || r.URL.String() != testURL {
			t.Fatal("unexpected request")
		}
		res := response(data)
		res.Header.Set("Content-Type", "application/json")
		return res, nil
	})
	img, err := c.Resolve(context.Background(), testURL)
	if err != nil || img.MediaType != "image/png" || !bytes.Equal(img.Data, data) || calls != 1 {
		t.Fatalf("download failed: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != cacheName(testURL) {
		t.Fatal("expected one opaque persisted file")
	}
	for _, path := range []string{dir, filepath.Join(dir, cacheName(testURL))} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatal("cache permissions are not private")
		}
	}
	stored, err := os.ReadFile(filepath.Join(dir, cacheName(testURL)))
	if err != nil || !bytes.Equal(stored, data) {
		t.Fatal("persisted bytes differ")
	}
	for _, cache := range []*Cache{c, New(dir)} {
		noNetwork(t, cache)
		img, err = cache.Resolve(context.Background(), testURL)
		if err != nil || !bytes.Equal(img.Data, data) || img.MediaType != "image/png" {
			t.Fatalf("offline cache miss: %v", err)
		}
	}
}

func TestSharedInstanceSerializesDownloads(t *testing.T) {
	c := New(privateTempDir(t))
	data := pngBytes(t)
	calls := 0
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return response(data), nil
	})
	done := make(chan error, 8)
	for i := 0; i < cap(done); i++ {
		go func() {
			img, err := c.Resolve(context.Background(), testURL)
			if err == nil && !bytes.Equal(img.Data, data) {
				err = errors.New("incorrect shared image")
			}
			done <- err
		}()
	}
	for i := 0; i < cap(done); i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatal("shared cache duplicated downloads")
	}
}

func TestRejectResponses(t *testing.T) {
	pngData := pngBytes(t)
	oversize := append(append([]byte(nil), pngData...), make([]byte, MaxImageBytes+1-len(pngData))...)
	cases := []struct {
		name   string
		data   []byte
		status int
		length int64
	}{
		{"expired_json", []byte(`{"error":"expired","token":"test-secret"}`), 200, -1},
		{"expired_status", pngData, 403, -1},
		{"declared_oversize", pngData, 200, MaxImageBytes + 1},
		{"stream_oversize", oversize, 200, -1},
		{"fake_png", []byte("\x89PNG\r\n\x1a\nnot-an-image"), 200, -1},
		{"truncated_png", pngData[:33], 200, -1},
		{"fake_webp", []byte("RIFF\x14\x00\x00\x00WEBPVP8 fake-data"), 200, -1},
		{"empty", nil, 200, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New(privateTempDir(t))
			c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				res := response(tc.data)
				res.StatusCode, res.ContentLength = tc.status, tc.length
				res.Header.Set("Content-Type", "image/png")
				return res, nil
			})
			img, err := c.Resolve(context.Background(), testURL)
			requireUnavailable(t, img, err)
			entries, err := os.ReadDir(c.dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("rejected response was persisted")
			}
		})
	}
}

func TestURLAndRedirectPolicy(t *testing.T) {
	for _, host := range []string{"multimedia.nt.qq.com.cn", "gchat.qpic.cn", "c2cpicdw.qpic.cn", "q.qpic.cn"} {
		for _, suffix := range []string{"", ":443"} {
			if !allowedURL("https://" + host + suffix + "/image?q=1") {
				t.Fatal("allowlisted host rejected")
			}
		}
	}
	c := New(privateTempDir(t))
	noNetwork(t, c)
	for _, raw := range []string{
		"", "http://gchat.qpic.cn/a", "https://example.com/a", "https://127.0.0.1/a",
		"https://[::1]/a", "https://gchat.qpic.cn.evil.test/a", "https://sub.gchat.qpic.cn/a",
		"https://gchat.qpic.cn./a", "https://gchat.qpic.cn:80/a", "https://gchat.qpic.cn:/a",
		"https://user:password@gchat.qpic.cn/a", "https://gchat.qpic.cn/a#fragment",
		"https:gchat.qpic.cn/a", "file:///tmp/image", "https://gchat.qpic.cn%2eevil.test/a",
		"https://gchat.qpic.cn\\@127.0.0.1/a", "https://gchat.qpic.cn:0443/a",
	} {
		img, err := c.Resolve(context.Background(), raw)
		requireUnavailable(t, img, err)
	}
	for _, location := range []string{"https://127.0.0.1/private", "https://gchat.qpic.cn/other"} {
		calls := 0
		c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			res := response(nil)
			res.StatusCode = http.StatusFound
			res.Header.Set("Location", location)
			return res, nil
		})
		img, err := c.Resolve(context.Background(), testURL)
		requireUnavailable(t, img, err)
		if calls != 1 {
			t.Fatal("redirect was followed")
		}
	}
	transport := New("").client.Transport.(*http.Transport)
	if transport.Proxy != nil || transport.DialContext == nil || transport.TLSClientConfig != nil {
		t.Fatal("unsafe transport defaults")
	}
}

func TestPublicIPAndDialPolicy(t *testing.T) {
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"} {
		if !publicIP(netip.MustParseAddr(raw)) {
			t.Fatal("public address rejected")
		}
	}
	for _, raw := range []string{
		"0.0.0.0", "10.0.0.1", "127.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254",
		"100.64.0.1", "192.0.0.1", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1",
		"224.0.0.1", "240.0.0.1", "255.255.255.255", "::", "::1", "::ffff:127.0.0.1",
		"fc00::1", "fe80::1", "fec0::1", "ff02::1", "2001:db8::1", "2002:7f00:1::",
		"64:ff9b::7f00:1", "64:ff9b:1::1", "2001::1", "3fff::1", "2606:4700::1%en0",
	} {
		if publicIP(netip.MustParseAddr(raw)) {
			t.Fatal("non-public address accepted")
		}
	}
	if publicIP(netip.Addr{}) {
		t.Fatal("invalid address accepted")
	}
	for _, address := range []string{"localhost:443", "127.0.0.1:443", "gchat.qpic.cn:80", "invalid"} {
		conn, err := safeDial(context.Background(), "tcp", address)
		if conn != nil || err != ErrUnavailable {
			t.Fatal("dial boundary accepted invalid destination")
		}
	}
	conn, err := safeDial(context.Background(), "udp", "gchat.qpic.cn:443")
	if conn != nil || err != ErrUnavailable {
		t.Fatal("dial accepted non-TCP network")
	}
}

func TestDirectoryAndFileSafety(t *testing.T) {
	data := pngBytes(t)
	for _, scenario := range []string{"directory_mode", "directory_symlink", "directory_symlink_slash", "file_symlink", "unrelated_symlink", "file_mode", "subdirectory"} {
		t.Run(scenario, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "cache")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(base, "outside.img")
			if err := os.WriteFile(outside, data, 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch scenario {
			case "directory_mode":
				err = os.Chmod(dir, 0755)
			case "directory_symlink", "directory_symlink_slash":
				link := filepath.Join(base, "link")
				err = os.Symlink(dir, link)
				dir = link
				if scenario == "directory_symlink_slash" {
					dir += string(os.PathSeparator)
				}
			case "file_symlink":
				err = os.Symlink(outside, filepath.Join(dir, cacheName(testURL)))
			case "unrelated_symlink":
				err = os.Symlink(outside, filepath.Join(dir, "link"))
			case "file_mode":
				path := filepath.Join(dir, cacheName(testURL))
				err = os.WriteFile(path, data, 0600)
				if err == nil {
					err = os.Chmod(path, 0644)
				}
			case "subdirectory":
				err = os.Mkdir(filepath.Join(dir, "nested"), 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			c := New(dir)
			noNetwork(t, c)
			img, err := c.Resolve(context.Background(), testURL)
			requireUnavailable(t, img, err)
			unchanged, err := os.ReadFile(outside)
			if err != nil || !bytes.Equal(unchanged, data) {
				t.Fatal("outside file changed")
			}
		})
	}
}

func TestCorruptCacheReplacement(t *testing.T) {
	data := pngBytes(t)
	c := New(privateTempDir(t))
	path := filepath.Join(c.dir, cacheName(testURL))
	if err := os.WriteFile(path, []byte(`{"error":"expired"}`), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return response(data), nil
	})
	img, err := c.Resolve(context.Background(), testURL)
	if err != nil || !bytes.Equal(img.Data, data) || calls != 1 {
		t.Fatalf("corrupt cache not repaired: %v", err)
	}
	noNetwork(t, c)
	if _, err := c.Resolve(context.Background(), testURL); err != nil {
		t.Fatal("replacement not durable")
	}
}

func TestCacheQuotas(t *testing.T) {
	if MaxImageBytes != 5<<20 || MaxBatchBytes != 20<<20 || MaxImages != 16 || maxCacheBytes != 512<<20 || maxCacheFiles != 4096 {
		t.Fatal("unexpected resource limits")
	}
	for _, scenario := range []string{"bytes_boundary", "bytes_full", "files_boundary", "files_full"} {
		t.Run(scenario, func(t *testing.T) {
			c := New(privateTempDir(t))
			data := pngBytes(t)
			boundary := strings.HasSuffix(scenario, "boundary")
			if strings.HasPrefix(scenario, "bytes") {
				f, err := os.OpenFile(filepath.Join(c.dir, "reserved"), os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				size := int64(maxCacheBytes)
				if boundary {
					size -= MaxImageBytes
				}
				err = f.Truncate(size)
				closeErr := f.Close()
				if err != nil || closeErr != nil {
					t.Fatal("cannot create sparse quota fixture")
				}
			} else {
				count := maxCacheFiles
				if boundary {
					count--
				}
				for i := 0; i < count; i++ {
					if err := os.WriteFile(filepath.Join(c.dir, strconv.Itoa(i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if boundary {
				c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
					return response(data), nil
				})
				if _, err := c.Resolve(context.Background(), testURL); err != nil {
					t.Fatalf("quota boundary rejected: %v", err)
				}
				noNetwork(t, c)
				if _, err := c.Resolve(context.Background(), testURL); err != nil {
					t.Fatal("full quota blocked existing cache hit")
				}
				img, err := c.Resolve(context.Background(), testURL+"&other=1")
				requireUnavailable(t, img, err)
			} else {
				noNetwork(t, c)
				img, err := c.Resolve(context.Background(), testURL)
				requireUnavailable(t, img, err)
			}
		})
	}
}

type cancelBody struct {
	cancel context.CancelFunc
}

func (b cancelBody) Read([]byte) (int, error) {
	b.cancel()
	return 0, errors.New("private body details")
}

func (cancelBody) Close() error { return nil }

func TestCancellationAndSanitizedErrors(t *testing.T) {
	for _, scenario := range []string{"before", "gate", "request", "body", "deadline", "request_deadline"} {
		t.Run(scenario, func(t *testing.T) {
			c := New(privateTempDir(t))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := error(context.Canceled)
			noNetwork(t, c)
			switch scenario {
			case "before":
				cancel()
			case "gate":
				c.gate <- struct{}{}
				cancel()
			case "request":
				c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
					cancel()
					return nil, errors.New("private request details")
				})
			case "body":
				c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
					res := response(nil)
					res.ContentLength, res.Body = -1, cancelBody{cancel}
					return res, nil
				})
			case "request_deadline":
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer stop()
				want = context.DeadlineExceeded
				c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				})
			case "deadline":
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				want = context.DeadlineExceeded
			}
			img, err := c.Resolve(ctx, testURL)
			if err != want || len(img.Data) != 0 {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
	c := New(privateTempDir(t))
	c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("secret URL and filesystem details")
	})
	img, err := c.Resolve(context.Background(), testURL)
	requireUnavailable(t, img, err)
	img, err = c.Resolve(nil, testURL)
	requireUnavailable(t, img, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := safeDial(ctx, "tcp", "gchat.qpic.cn:443"); err != context.Canceled {
		t.Fatal("dial cancellation lost")
	}
}

func TestInspectSizeAndDimensions(t *testing.T) {
	data := pngBytes(t)
	atLimit := append(append([]byte(nil), data...), make([]byte, MaxImageBytes-len(data))...)
	if _, err := inspect(atLimit); err != nil {
		t.Fatal("5 MiB boundary rejected")
	}
	img, err := inspect(append(atLimit, 0))
	requireUnavailable(t, img, err)
	for _, dims := range [][2]uint32{{0, 1}, {1, 0}, {40000001, 1}, {65536, 65536}, {0x7fffffff, 0x7fffffff}} {
		bad := append([]byte(nil), data...)
		binary.BigEndian.PutUint32(bad[16:20], dims[0])
		binary.BigEndian.PutUint32(bad[20:24], dims[1])
		binary.BigEndian.PutUint32(bad[29:33], crc32.ChecksumIEEE(bad[12:29]))
		img, err := inspect(bad)
		requireUnavailable(t, img, err)
	}
}
