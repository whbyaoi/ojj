package imagecache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type batchResolverFunc func(context.Context, string) (Image, error)

func (f batchResolverFunc) Resolve(ctx context.Context, raw string) (Image, error) {
	return f(ctx, raw)
}

func TestResolveBatchLimits(t *testing.T) {
	for _, mode := range []string{"duplicate", "failure", "bytes", "oversize", "mime", "empty", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			urls := make([]string, 40)
			for i := range urls {
				urls[i] = fmt.Sprint(i)
				if mode == "duplicate" {
					urls[i] = "same"
				}
			}
			resolver := batchResolverFunc(func(child context.Context, _ string) (Image, error) {
				calls++
				deadline, ok := child.Deadline()
				if !ok || time.Until(deadline) > 12*time.Second {
					t.Error("missing batch deadline")
				}
				img := Image{Data: []byte{1}, MediaType: "image/png"}
				switch mode {
				case "failure":
					return Image{}, ErrUnavailable
				case "bytes":
					img.Data = make([]byte, MaxImageBytes)
				case "oversize":
					img.Data = make([]byte, MaxImageBytes+1)
				case "mime":
					img.MediaType = "image/webp"
				case "empty":
					img.Data = nil
				case "cancel":
					cancel()
				}
				return img, nil
			})
			got := ResolveBatch(ctx, resolver, urls)
			wantCalls, wantImages := MaxImages, 0
			switch mode {
			case "duplicate":
				wantCalls, wantImages = 1, 1
			case "bytes":
				wantCalls, wantImages = MaxBatchBytes/MaxImageBytes, MaxBatchBytes/MaxImageBytes
			case "cancel":
				wantCalls = 1
			}
			if calls != wantCalls || len(got) != wantImages {
				t.Fatalf("calls=%d images=%d; want %d/%d", calls, len(got), wantCalls, wantImages)
			}
			if mode != "cancel" && ctx.Err() != nil {
				t.Fatal("batch canceled parent")
			}
		})
	}
}

func TestResolveBatchUnavailable(t *testing.T) {
	resolver := batchResolverFunc(func(context.Context, string) (Image, error) {
		t.Fatal("unexpected resolve")
		return Image{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if len(ResolveBatch(ctx, resolver, []string{"a"})) != 0 ||
		len(ResolveBatch(nil, resolver, []string{"a"})) != 0 ||
		len(ResolveBatch(context.Background(), nil, []string{"a"})) != 0 {
		t.Fatal("unavailable resolver returned images")
	}
}
