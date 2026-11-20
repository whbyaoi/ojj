package imagecache

import (
	"context"
	"time"
)

// Resolver must return validated PNG/JPEG/GIF bytes and respect cancellation.
// Cache implements it; injected implementations are trusted host code.
type Resolver interface {
	Resolve(context.Context, string) (Image, error)
}

// ResolveBatch bounds attempts, aggregate bytes and wall time. Callers put the
// most recent images first. Missing entries must degrade to text, never URLs.
func ResolveBatch(ctx context.Context, resolver Resolver, urls []string) map[string]Image {
	out := make(map[string]Image)
	if resolver == nil || ctx == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	seen := make(map[string]bool)
	attempts, size := 0, 0
	for _, raw := range urls {
		if ctx.Err() != nil || attempts >= MaxImages || size >= MaxBatchBytes {
			break
		}
		if seen[raw] {
			continue
		}
		seen[raw] = true
		attempts++
		img, err := resolver.Resolve(ctx, raw)
		if err != nil || ctx.Err() != nil || len(img.Data) == 0 || len(img.Data) > MaxImageBytes || len(img.Data) > MaxBatchBytes-size {
			continue
		}
		switch img.MediaType {
		case "image/png", "image/jpeg", "image/gif":
		default:
			continue
		}
		out[raw] = img
		size += len(img.Data)
	}
	return out
}
