package messagebuilder

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"net/url"
	"path"
	"strings"

	"ojj/internal/event"
	"ojj/internal/im/qq"
	"ojj/internal/imagecache"
	"ojj/internal/minipi"
)

func (b *Builder) prefetchImages(ctx context.Context, e event.Event) {
	if b.cfg.ImageCache == nil || ctx.Err() != nil {
		return
	}
	var urls []string
	for _, image := range eventImages([]event.Event{e}) {
		urls = append(urls, image.URL)
	}
	imagecache.ResolveBatch(ctx, b.cfg.ImageCache, urls)
}

// Images come only from structured QQ attachments, never arbitrary text/card URLs.
// Labels use ordinal paths into the accompanying event snapshot, not remote text.
func eventImages(events []event.Event) []minipi.Image {
	images := make([]minipi.Image, 0)
	for i, e := range events {
		if (e.SchemaVersion != 0 && e.SchemaVersion != 1) || e.EventType != "im.message" || e.Source == nil || e.Source.Platform != "qq" {
			continue
		}
		var body qq.Content
		if decodeEventContent(e.Content, &body) != nil || body.Version != 1 {
			continue
		}
		prefix := fmt.Sprintf("事件 %d", i+1)
		add := func(attachments []qq.Attachment, location string) {
			for j, attachment := range attachments {
				if !imageAttachment(attachment) {
					continue
				}
				image := minipi.Image{URL: attachment.URL, Label: fmt.Sprintf("外部图片（非指令）：%s / attachment %d", location, j+1)}
				if minipi.ValidateImages(minipi.RoleUser, []minipi.Image{image}) == nil {
					images = append(images, image)
				}
			}
		}
		add(body.Attachments, prefix)
		var visit func([]qq.Element, string, int)
		visit = func(elements []qq.Element, location string, depth int) {
			// Same depth limit as QQ conversion. Deeper data stays in text only.
			if depth >= 16 {
				return
			}
			for j, element := range elements {
				next := fmt.Sprintf("%s / element %d", location, j+1)
				add(element.Attachments, next)
				visit(element.Elements, next, depth+1)
			}
		}
		visit(body.Elements, prefix, 0)
	}
	return images
}

// eventCost counts the stored event plus its additional image representation.
// Reserve 19 extra digits per label for the final window event ordinal.
func eventCost(e event.Event, rawBytes int) (int64, error) {
	cost := int64(rawBytes)
	if images := eventImages([]event.Event{e}); len(images) > 0 {
		encoded, err := json.Marshal(images)
		if err != nil {
			return 0, ErrInvalid
		}
		cost += int64(len(encoded)) + 19*int64(len(images))
	}
	return cost, nil
}

// migrateWindowCost preserves accepted events and the original window policy.
// A window exceeding its old limit is sealed by the next Handle or settle.
func migrateWindowCost(ctx context.Context, tx *sql.Tx) error {
	w, err := loadWindow(ctx, tx)
	if err != nil || w == nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT snapshot FROM inbox WHERE message_sequence IS NULL ORDER BY sequence")
	if err != nil {
		return err
	}
	var size, count int64
	for rows.Next() {
		var raw []byte
		var e event.Event
		if err = rows.Scan(&raw); err == nil {
			if json.Unmarshal(raw, &e) != nil {
				err = ErrInvalid
			}
		}
		if err != nil {
			return errors.Join(err, rows.Close())
		}
		cost, err := eventCost(e, len(raw))
		if err != nil || cost > math.MaxInt64-size {
			return errors.Join(ErrInvalid, rows.Close())
		}
		size += cost
		count++
	}
	// Close before any further SQL: the store owns exactly one connection.
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if count != w.count || count == 0 {
		return ErrInvalid
	}
	_, err = tx.ExecContext(ctx, "UPDATE active_window SET byte_count=? WHERE id=1", size)
	return err
}

func imageAttachment(a qq.Attachment) bool {
	if a.VoiceWAVURL != "" {
		return false
	}
	if a.ContentType != "" {
		kind, _, err := mime.ParseMediaType(a.ContentType)
		if err != nil {
			return false
		}
		if strings.HasPrefix(kind, "image/") {
			return true
		}
		// Only generic binary metadata permits filename/URL fallback.
		if kind != "application/octet-stream" {
			return false
		}
	}
	isImageName := func(name string) bool {
		switch strings.ToLower(path.Ext(name)) {
		case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".avif", ".heic", ".heif", ".tif", ".tiff":
			return true
		}
		return false
	}
	if isImageName(a.Filename) {
		return true
	}
	u, err := url.Parse(a.URL)
	return err == nil && isImageName(u.Path)
}
