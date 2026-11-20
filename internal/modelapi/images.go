package modelapi

import (
	"context"
	"encoding/base64"
	"errors"

	"ojj/internal/imagecache"
	"ojj/internal/minipi"
)

const unavailableImage = "[图片不可用：未缓存、下载失败或超过限制]"

type preparedImages struct {
	items map[[2]int]imagecache.Image
}

// Preparation never mutates input messages or stores Base64 in agent history.
// Budget counts occurrences, not unique URLs, to bound the actual wire body.
func (c *Client) prepareImages(ctx context.Context, in minipi.Request) (*preparedImages, error) {
	var urls []string
	for _, m := range in.Messages {
		if minipi.ValidateImages(m.Role, m.Images) != nil {
			return nil, errors.New("modelapi: invalid message or tool history")
		}
	}
	seen := make(map[string]bool)
	for i := len(in.Messages) - 1; i >= 0 && len(urls) < imagecache.MaxImages; i-- {
		for _, img := range in.Messages[i].Images {
			if len(urls) == imagecache.MaxImages {
				break
			}
			if !seen[img.URL] {
				seen[img.URL] = true
				urls = append(urls, img.URL)
			}
		}
	}
	resolved := imagecache.ResolveBatch(ctx, c.imageCache, urls)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := &preparedImages{items: make(map[[2]int]imagecache.Image)}
	count, size := 0, 0
	for i := len(in.Messages) - 1; i >= 0; i-- {
		for j, ref := range in.Messages[i].Images {
			img, ok := resolved[ref.URL]
			if !ok || count >= imagecache.MaxImages || len(img.Data) > imagecache.MaxBatchBytes-size {
				continue
			}
			out.items[[2]int{i, j}] = img
			count++
			size += len(img.Data)
		}
	}
	return out, nil
}

func (p *preparedImages) source(message, index int) *imageSource {
	if p == nil {
		return nil
	}
	img, ok := p.items[[2]int{message, index}]
	if !ok {
		return nil
	}
	return &imageSource{Type: "base64", MediaType: img.MediaType, Data: base64.StdEncoding.EncodeToString(img.Data)}
}
