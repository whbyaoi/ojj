package qq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type AttachmentKind string

const (
	AttachmentImage AttachmentKind = "image"
	AttachmentVideo AttachmentKind = "video"
	AttachmentAudio AttachmentKind = "audio"
	AttachmentFile  AttachmentKind = "file"
)

var ErrUnsupportedAttachment = errors.New("qq: unsupported attachment kind")

type OutgoingAttachment struct {
	Kind AttachmentKind `json:"kind"`
	URL  string         `json:"url"`
}

type AttachmentMessage struct {
	TargetType string             `json:"target_type"`
	TargetID   string             `json:"target_id"`
	MsgID      string             `json:"msg_id"`
	MsgSeq     uint32             `json:"msg_seq,omitempty"`
	Attachment OutgoingAttachment `json:"attachment"`
}

// UploadedAttachment is an opaque, client- and target-bound platform credential.
// Its zero value cannot be sent. Do not persist it or expose it to model input.
type UploadedAttachment struct {
	client     *Client
	targetType string
	targetID   string
	kind       AttachmentKind
	fileInfo   string
	expiresAt  time.Time
}

func (UploadedAttachment) String() string     { return "qq: uploaded attachment (opaque)" }
func (u UploadedAttachment) GoString() string { return u.String() }

// ValidateAttachmentMessage performs local syntax/address checks only. QQ fetches
// the URL and validates image format/size. No local file access, download or DNS
// lookup occurs; DNS rebinding and remote redirects are NOT fully SSRF-isolated.
func ValidateAttachmentMessage(m AttachmentMessage) error {
	if err := ValidateTextMessage(TextMessage{
		TargetType: m.TargetType, TargetID: m.TargetID, Content: "attachment",
		MsgID: m.MsgID, MsgSeq: m.MsgSeq,
	}); err != nil {
		return err
	}
	if m.Attachment.Kind != AttachmentImage {
		return ErrUnsupportedAttachment
	}
	if !validAttachmentURL(m.Attachment.URL) {
		return errors.New("qq: invalid public image URL")
	}
	return nil
}

func validAttachmentURL(raw string) bool {
	if raw == "" || len(raw) > 8192 || !utf8.ValidString(raw) || strings.Contains(raw, "#") {
		return false
	}
	if strings.IndexFunc(raw, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return false
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil || strings.IndexFunc(decoded, unicode.IsControl) >= 0 {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Opaque != "" || u.User != nil || u.Host == "" {
		return false
	}
	if port := u.Port(); (port != "" && port != "80" && port != "443") || strings.HasSuffix(u.Host, ":") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if addr, err := netip.ParseAddr(host); err == nil {
		return publicAttachmentIP(addr)
	}
	// Reject malformed IP literals, legacy numeric IP spellings and local names.
	if strings.ContainsAny(u.Host, "[]") || strings.ContainsAny(host, ":%") {
		return false
	}
	host = strings.TrimSuffix(host, ".")
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	for _, suffix := range []string{"localhost", "local", "internal", "invalid", "test"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return false
		}
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	tld := labels[len(labels)-1]
	return strings.IndexFunc(tld, func(r rune) bool { return r >= 'a' && r <= 'z' }) >= 0 && !strings.HasPrefix(tld, "0x")
}

func publicAttachmentIP(addr netip.Addr) bool {
	if addr.Zone() != "" {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return false
	}
	// Exclude special-use ranges not covered by IsGlobalUnicast/IsPrivate.
	for _, cidr := range []string{
		"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24",
		"192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4",
		"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
	} {
		if netip.MustParsePrefix(cidr).Contains(addr) {
			return false
		}
	}
	return addr.Is4() || netip.MustParsePrefix("2000::/3").Contains(addr)
}

func attachmentPath(targetType, targetID, action string) string {
	resource := "users"
	if targetType == "group" {
		resource = "groups"
	}
	return "/v2/" + resource + "/" + targetID + "/" + action
}

// UploadAttachment registers a URL with QQ, without sending a message. POSTs
// are never retried, including on 401. TTL starts before authentication/upload.
func (c *Client) UploadAttachment(ctx context.Context, targetType, targetID string, attachment OutgoingAttachment) (uploaded UploadedAttachment, retErr error) {
	defer func() {
		if err := ctx.Err(); err != nil {
			uploaded, retErr = UploadedAttachment{}, err
		}
	}()
	if err := ctx.Err(); err != nil {
		return UploadedAttachment{}, err
	}
	if err := ValidateAttachmentMessage(AttachmentMessage{TargetType: targetType, TargetID: targetID, Attachment: attachment}); err != nil {
		return UploadedAttachment{}, err
	}
	started := time.Now()
	body, _ := json.Marshal(struct {
		FileType   int    `json:"file_type"`
		URL        string `json:"url"`
		SrvSendMsg bool   `json:"srv_send_msg"`
	}{1, attachment.URL, false})
	token, err := c.accessToken(ctx)
	if err != nil {
		return UploadedAttachment{}, err
	}
	var response struct {
		FileInfo string `json:"file_info"`
		TTL      int64  `json:"ttl"`
	}
	if err := c.request(ctx, http.MethodPost, attachmentPath(targetType, targetID, "files"), token, bytes.NewReader(body), &response); err != nil {
		return UploadedAttachment{}, err
	}
	if strings.TrimSpace(response.FileInfo) == "" || response.TTL <= 0 || response.TTL > math.MaxInt64/int64(time.Second) {
		return UploadedAttachment{}, ErrProtocol
	}
	expires := started.Add(time.Duration(response.TTL) * time.Second)
	if !time.Now().Before(expires) {
		return UploadedAttachment{}, ErrProtocol
	}
	return UploadedAttachment{c, targetType, targetID, attachment.Kind, response.FileInfo, expires}, nil
}

func (u UploadedAttachment) valid(c *Client, m AttachmentMessage) bool {
	return u.client == c && u.targetType == m.TargetType && u.targetID == m.TargetID &&
		u.kind == m.Attachment.Kind && u.kind == AttachmentImage && strings.TrimSpace(u.fileInfo) != "" && time.Now().Before(u.expiresAt)
}

// SendUploadedAttachment sends a previously uploaded image. file_info remains
// opaque and is JSON-escaped only, never base64-encoded again. Unknown ACKs and
// transport failures must not be blindly retried by callers.
func (c *Client) SendUploadedAttachment(ctx context.Context, m AttachmentMessage, uploaded UploadedAttachment) (sent SentMessage, retErr error) {
	defer func() {
		if err := ctx.Err(); err != nil {
			sent, retErr = SentMessage{}, err
		}
	}()
	if err := ctx.Err(); err != nil {
		return SentMessage{}, err
	}
	if err := ValidateAttachmentMessage(m); err != nil {
		return SentMessage{}, err
	}
	if !uploaded.valid(c, m) {
		return SentMessage{}, errors.New("qq: invalid or expired uploaded attachment")
	}
	if m.MsgID != "" && m.MsgSeq == 0 {
		m.MsgSeq = 1
	}
	body, _ := json.Marshal(struct {
		MsgType int `json:"msg_type"`
		Media   struct {
			FileInfo string `json:"file_info"`
		} `json:"media"`
		MsgID  string `json:"msg_id,omitempty"`
		MsgSeq uint32 `json:"msg_seq,omitempty"`
	}{7, struct {
		FileInfo string `json:"file_info"`
	}{uploaded.fileInfo}, m.MsgID, m.MsgSeq})
	token, err := c.accessToken(ctx)
	if err != nil {
		return SentMessage{}, err
	}
	// Authentication may have waited long enough for the credential to expire.
	if !uploaded.valid(c, m) {
		return SentMessage{}, errors.New("qq: invalid or expired uploaded attachment")
	}
	var result SentMessage
	if err := c.request(ctx, http.MethodPost, attachmentPath(m.TargetType, m.TargetID, "messages"), token, bytes.NewReader(body), &result); err != nil {
		return SentMessage{}, err
	}
	if !validID(result.ID) {
		return SentMessage{}, ErrProtocol
	}
	return result, nil
}

// SendAttachment validates the whole message before upload, then sends only
// after a successful upload. There is no local image fetch or automatic retry.
func (c *Client) SendAttachment(ctx context.Context, m AttachmentMessage) (SentMessage, error) {
	if err := ctx.Err(); err != nil {
		return SentMessage{}, err
	}
	if err := ValidateAttachmentMessage(m); err != nil {
		return SentMessage{}, err
	}
	uploaded, err := c.UploadAttachment(ctx, m.TargetType, m.TargetID, m.Attachment)
	if err != nil {
		return SentMessage{}, err
	}
	return c.SendUploadedAttachment(ctx, m, uploaded)
}
