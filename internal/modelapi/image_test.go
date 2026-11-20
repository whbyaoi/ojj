package modelapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"ojj/internal/imagecache"
	"ojj/internal/minipi"
)

type imageResolverFunc func(context.Context, string) (imagecache.Image, error)

func (f imageResolverFunc) Resolve(ctx context.Context, raw string) (imagecache.Image, error) {
	return f(ctx, raw)
}

func TestImageRequests(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolOpenAI, ProtocolAnthropic} {
		for _, text := range []string{"", "look at this"} {
			t.Run(string(protocol)+"/"+text, func(t *testing.T) {
				images := []minipi.Image{{URL: "https://images.example/a.png?signature=secret", Label: "event 1 attachment 1"}, {URL: "https://images.example/emoji.gif"}}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body struct {
						Messages []struct {
							Role    string           `json:"role"`
							Content []map[string]any `json:"content"`
						} `json:"messages"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					if len(body.Messages) != 1 || body.Messages[0].Role != "user" {
						t.Error("incorrect user message")
						return
					}
					var urls, texts, kinds []string
					for _, block := range body.Messages[0].Content {
						kinds = append(kinds, block["type"].(string))
						switch block["type"] {
						case "text":
							texts = append(texts, block["text"].(string))
						case "image":
							source := block["source"].(map[string]any)
							if protocol != ProtocolAnthropic || source["type"] != "base64" || source["media_type"] != "image/png" || source["url"] != nil {
								t.Error("wrong image protocol")
							}
							urls = append(urls, "data:image/png;base64,"+source["data"].(string))
						case "image_url":
							if protocol != ProtocolOpenAI {
								t.Error("wrong image protocol")
							}
							urls = append(urls, block["image_url"].(map[string]any)["url"].(string))
						default:
							t.Error("unexpected block")
						}
					}
					imageKind := "image"
					if protocol == ProtocolOpenAI {
						imageKind = "image_url"
					}
					wantKinds := []string{"text", imageKind, imageKind}
					if text != "" {
						wantKinds = append([]string{"text"}, wantKinds...)
					}
					if !reflect.DeepEqual(kinds, wantKinds) {
						t.Error("image label must immediately precede its image")
					}
					wantTexts := []string{images[0].Label}
					if text != "" {
						wantTexts = append([]string{text}, wantTexts...)
					}
					if !reflect.DeepEqual(texts, wantTexts) || !reflect.DeepEqual(urls, []string{"data:image/png;base64,AQID", "data:image/png;base64,AQID"}) {
						t.Error("lost image or text")
					}
					if protocol == ProtocolOpenAI {
						fmt.Fprint(w, openAIGood)
					} else {
						fmt.Fprint(w, `{"type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
					}
				}))
				defer server.Close()
				client, err := New(Config{Protocol: protocol, APIKey: "fake", Model: "vision", BaseURL: server.URL, ImageCache: imageResolverFunc(func(context.Context, string) (imagecache.Image, error) {
					return imagecache.Image{Data: []byte{1, 2, 3}, MediaType: "image/png"}, nil
				})})
				if err != nil {
					t.Fatal(err)
				}
				_, err = client.Generate(context.Background(), minipi.Request{Messages: []minipi.Message{{Role: minipi.RoleUser, Content: text, Images: images}}}, nil)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestImageFallbackAndCancellation(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolOpenAI, ProtocolAnthropic} {
		for _, mode := range []string{"nil", "expired", "cancel", "invalid-history"} {
			t.Run(string(protocol)+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls, downloads := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					body, _ := io.ReadAll(r.Body)
					for _, secret := range []string{"images.example", "signed-secret", "download-error", "base64", `"image_url"`, `"type":"image"`} {
						if strings.Contains(string(body), secret) {
							t.Error("attachment details leaked")
						}
					}
					for _, text := range []string{"keep-text", "keep-label", "图片不可用"} {
						if !strings.Contains(string(body), text) {
							t.Error("fallback lost text")
						}
					}
					if protocol == ProtocolOpenAI {
						fmt.Fprint(w, openAIGood)
					} else {
						fmt.Fprint(w, `{"type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
					}
				}))
				defer server.Close()
				var resolver imagecache.Resolver
				if mode != "nil" {
					resolver = imageResolverFunc(func(context.Context, string) (imagecache.Image, error) {
						downloads++
						if mode == "cancel" {
							cancel()
						}
						return imagecache.Image{}, errors.New("download-error signed-secret")
					})
				}
				client, err := New(Config{Protocol: protocol, APIKey: "fake", Model: "vision", BaseURL: server.URL, ImageCache: resolver})
				if err != nil {
					t.Fatal(err)
				}
				in := minipi.Request{Messages: []minipi.Message{{Role: minipi.RoleUser, Content: "keep-text", Images: []minipi.Image{{URL: "https://images.example/a?signed-secret", Label: "keep-label"}}}}}
				if mode == "invalid-history" {
					in.Messages[0].ToolCallID = "orphan"
				}
				before, _ := json.Marshal(in)
				_, err = client.Generate(ctx, in, nil)
				after, _ := json.Marshal(in)
				if string(before) != string(after) {
					t.Fatal("request/history mutated")
				}
				if mode == "cancel" {
					if !errors.Is(err, context.Canceled) || calls != 0 {
						t.Fatal("cancellation ignored")
					}
				} else if mode == "invalid-history" {
					if err == nil || calls != 0 || downloads != 0 {
						t.Fatal("download before validation")
					}
				} else if err != nil || calls != 1 {
					t.Fatalf("fallback failed: %v, calls=%d", err, calls)
				}
			})
		}
	}
}

func TestPreparedImageBudgets(t *testing.T) {
	for _, size := range []int{3, imagecache.MaxImageBytes} {
		calls := 0
		c := &Client{imageCache: imageResolverFunc(func(context.Context, string) (imagecache.Image, error) {
			calls++
			return imagecache.Image{Data: make([]byte, size), MediaType: "image/png"}, nil
		})}
		in := minipi.Request{}
		for i := 0; i < 20; i++ {
			in.Messages = append(in.Messages, minipi.Message{Role: minipi.RoleUser, Images: []minipi.Image{{URL: "https://images.example/repeated"}}})
		}
		out, err := c.prepareImages(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		want := imagecache.MaxImages
		if size == imagecache.MaxImageBytes {
			want = imagecache.MaxBatchBytes / size
		}
		if calls != 1 || len(out.items) != want {
			t.Fatalf("budget: calls=%d items=%d", calls, len(out.items))
		}
		if out.source(19, 0) == nil || out.source(0, 0) != nil {
			t.Fatal("newest image priority lost")
		}
	}
}

func TestRejectInvalidImages(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolOpenAI, ProtocolAnthropic} {
		client := &Client{protocol: protocol, model: "vision", maxOutputTokens: 100}
		for _, role := range []minipi.Role{minipi.RoleUser, minipi.RoleAssistant, minipi.RoleTool} {
			for _, url := range []string{"https://images.example/a.png", "file:///secret", "data:image/png;base64,secret", "https://user:secret@images.example/a", "https://images.example/a\nsecret", "https://", "//images.example/a"} {
				_, err := client.encodeRequest(minipi.Request{Messages: []minipi.Message{{Role: role, Content: "text", Images: []minipi.Image{{URL: url}}}}})
				valid := role == minipi.RoleUser && url == "https://images.example/a.png"
				if (err == nil) != valid {
					t.Errorf("unexpected validation result for %s", role)
				}
				if err != nil && strings.Contains(err.Error(), "secret") {
					t.Fatal("error leaked URL")
				}
			}
		}
	}
}
