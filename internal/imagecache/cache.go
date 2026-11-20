// Package imagecache stores sensitive image bytes outside message databases.
package imagecache

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	MaxImageBytes = 5 << 20
	MaxBatchBytes = 20 << 20
	MaxImages     = 16
	maxCacheBytes = 512 << 20
	maxCacheFiles = 4096
)

var ErrUnavailable = errors.New("imagecache: image unavailable")

type Image struct {
	Data      []byte
	MediaType string
}

// Cache uses SHA-256 of the original URL as a stable opaque reference. It never
// logs URLs or bytes, and never evicts images still referenced by old histories.
// The directory is operator-owned; quota exhaustion degrades to text only.
type Cache struct {
	dir    string
	client *http.Client
	gate   chan struct{}
}

// New is lazy: configuration validation does not touch disk or the network.
func New(dir string) *Cache {
	transport := &http.Transport{
		Proxy: nil, DialContext: safeDial,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		DisableKeepAlives:     true,
	}
	if dir != "" {
		dir = filepath.Clean(dir)
	}
	return &Cache{dir: dir, gate: make(chan struct{}, 1), client: &http.Client{
		Transport: transport, Timeout: 8 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func allowedURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.Port() != "" && u.Port() != "443") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if strings.ToLower(u.Host) != host && strings.ToLower(u.Host) != host+":443" {
		return false
	}
	return allowedHost(host)
}

func allowedHost(host string) bool {
	// Only known QQ attachment hosts, not arbitrary subdomains or text links.
	switch host {
	case "multimedia.nt.qq.com.cn", "gchat.qpic.cn", "c2cpicdw.qpic.cn", "q.qpic.cn":
		return true
	}
	return false
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	// Reject non-public IPv6 space, including deprecated site-local addresses.
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, raw := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "64:ff9b::/96", "64:ff9b:1::/48"} {
		if netip.MustParsePrefix(raw).Contains(ip) {
			return false
		}
	}
	return true
}

func safeDial(ctx context.Context, network, address string) (conn net.Conn, err error) {
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || port != "443" || !allowedHost(strings.ToLower(host)) || (network != "tcp" && network != "tcp4" && network != "tcp6") {
		return nil, ErrUnavailable
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, ErrUnavailable
	}
	// Validate all answers, then pin dialing to a checked IP (no DNS rebinding).
	for _, ip := range ips {
		if !publicIP(ip) {
			return nil, ErrUnavailable
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, ErrUnavailable
}

func inspect(data []byte) (Image, error) {
	if len(data) == 0 || len(data) > MaxImageBytes {
		return Image{}, ErrUnavailable
	}
	kind := http.DetectContentType(data)
	switch kind {
	case "image/png", "image/jpeg", "image/gif":
		cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
		if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width) > 40_000_000/int64(cfg.Height) {
			return Image{}, ErrUnavailable
		}
		// Config alone accepts truncated headers; validate actual image pixels too.
		if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
			return Image{}, ErrUnavailable
		}
	default:
		// WebP is deliberately unavailable until a complete decoder is provided.
		return Image{}, ErrUnavailable
	}
	return Image{Data: data, MediaType: kind}, nil
}

func read(root *os.Root, name string) (Image, error) {
	info, err := root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > MaxImageBytes {
		return Image{}, ErrUnavailable
	}
	f, err := root.Open(name)
	if err != nil {
		return Image{}, ErrUnavailable
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm()&0077 != 0 {
		return Image{}, ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxImageBytes+1))
	if err != nil {
		return Image{}, ErrUnavailable
	}
	return inspect(data)
}

// Resolve first reads durable cache, then attempts one bounded download. Errors
// deliberately contain no raw network/filesystem error, URL, or response body.
func (c *Cache) Resolve(ctx context.Context, raw string) (result Image, resultErr error) {
	if ctx == nil {
		return Image{}, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	defer func() {
		if err := ctx.Err(); err != nil {
			result, resultErr = Image{}, err
		}
	}()
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return Image{}, ctx.Err()
	}
	if ctx.Err() != nil {
		return Image{}, ctx.Err()
	}
	if !allowedURL(raw) || c.dir == "" {
		return Image{}, ErrUnavailable
	}
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return Image{}, ErrUnavailable
	}
	info, err := os.Lstat(c.dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return Image{}, ErrUnavailable
	}
	root, err := os.OpenRoot(c.dir)
	if err != nil {
		return Image{}, ErrUnavailable
	}
	defer root.Close()
	d, err := root.Open(".")
	if err != nil {
		return Image{}, ErrUnavailable
	}
	defer d.Close()
	opened, err := d.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.IsDir() || opened.Mode().Perm()&0077 != 0 {
		return Image{}, ErrUnavailable
	}
	hash := sha256.Sum256([]byte(raw))
	name := hex.EncodeToString(hash[:]) + ".img"
	if stat, err := root.Lstat(name); err == nil {
		if !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 {
			return Image{}, ErrUnavailable
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Image{}, ErrUnavailable
	}
	if cached, err := read(root, name); err == nil {
		return cached, nil
	}
	entries, err := d.Readdirnames(maxCacheFiles + 1)
	if (err != nil && !errors.Is(err, io.EOF)) || len(entries) >= maxCacheFiles {
		return Image{}, ErrUnavailable
	}
	var size int64
	for _, entry := range entries {
		stat, err := root.Lstat(entry)
		if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 || stat.Size() > maxCacheBytes-size {
			return Image{}, ErrUnavailable
		}
		size += stat.Size()
	}
	if size > maxCacheBytes-MaxImageBytes {
		return Image{}, ErrUnavailable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return Image{}, ErrUnavailable
	}
	res, err := c.client.Do(req)
	if err != nil {
		return Image{}, ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || res.ContentLength > MaxImageBytes {
		return Image{}, ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, MaxImageBytes+1))
	if err != nil || ctx.Err() != nil {
		return Image{}, ErrUnavailable
	}
	img, err := inspect(data)
	if err != nil {
		return Image{}, err
	}
	// Temp file + fsync + rename keeps crashes from exposing partial cache hits.
	if ctx.Err() != nil {
		return Image{}, ctx.Err()
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Image{}, ErrUnavailable
	}
	tmp := ".image-" + hex.EncodeToString(nonce[:])
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Image{}, ErrUnavailable
	}
	defer root.Remove(tmp)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil || root.Rename(tmp, name) != nil {
		return Image{}, ErrUnavailable
	}
	if err := d.Sync(); err != nil {
		return Image{}, ErrUnavailable
	}
	return img, nil
}
