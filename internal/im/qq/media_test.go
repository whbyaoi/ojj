package qq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func mediaMessage() AttachmentMessage {
	return AttachmentMessage{TargetType: "private", TargetID: "user_1", MsgID: "incoming",
		Attachment: OutgoingAttachment{Kind: AttachmentImage, URL: "https://cdn.example.com/a.png?signature=hidden"}}
}

func mediaResponse(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func mediaMockClient(rt sendRoundTripper) *Client {
	return &Client{cfg: Config{APIBaseURL: "https://api.example.invalid", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		http: &http.Client{Transport: rt}, token: "token", refreshAt: time.Now().Add(time.Hour)}
}

func TestMediaContract(t *testing.T) {
	for _, target := range []string{"private", "group"} {
		for _, mode := range []string{"active", "passive-default", "passive-explicit"} {
			t.Run(target+"/"+mode, func(t *testing.T) {
				m := mediaMessage()
				m.TargetType = target
				resource := "users"
				if target == "group" {
					resource = "groups"
				}
				if mode == "active" {
					m.MsgID = ""
				}
				if mode == "passive-explicit" {
					m.MsgSeq = 9
				}
				fileInfo := "  opaque+/=\"\\\n中文  "
				var uploads, sends, tokens atomic.Int32
				c := sendingClient(t, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/app/getAppAccessToken" {
						tokens.Add(1)
						fmt.Fprint(w, `{"access_token":"token","expires_in":7200}`)
						return
					}
					if r.Method != "POST" || r.Header.Get("Authorization") != "QQBot token" || r.Header.Get("Content-Type") != "application/json" {
						t.Error("incorrect HTTP contract")
					}
					var got map[string]any
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
						t.Error(err)
					}
					switch r.URL.Path {
					case "/v2/" + resource + "/user_1/files":
						uploads.Add(1)
						want := map[string]any{"file_type": float64(1), "url": m.Attachment.URL, "srv_send_msg": false}
						if !reflect.DeepEqual(got, want) {
							t.Error("incorrect upload body")
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"file_info": fileInfo, "ttl": 60})
					case "/v2/" + resource + "/user_1/messages":
						sends.Add(1)
						want := map[string]any{"msg_type": float64(7), "media": map[string]any{"file_info": fileInfo}}
						if m.MsgID != "" {
							seq := m.MsgSeq
							if seq == 0 {
								seq = 1
							}
							want["msg_id"], want["msg_seq"] = m.MsgID, float64(seq)
						}
						if !reflect.DeepEqual(got, want) {
							t.Error("incorrect send body or transformed credential")
						}
						fmt.Fprint(w, `{"id":"sent","timestamp":"timestamp"}`)
					default:
						t.Error("unexpected request path")
					}
				})
				u, err := c.UploadAttachment(context.Background(), m.TargetType, m.TargetID, m.Attachment)
				if err != nil {
					t.Fatal(err)
				}
				if uploads.Load() != 1 || sends.Load() != 0 {
					t.Fatal("upload sent a message")
				}
				encoded, err := json.Marshal(u)
				if err != nil || string(encoded) != "{}" {
					t.Fatal("credential is JSON-exposed")
				}
				for _, format := range []string{"%v", "%+v", "%#v"} {
					if strings.Contains(fmt.Sprintf(format, u), "opaque+/") {
						t.Fatal("credential is formatting-exposed")
					}
				}
				sent, err := c.SendUploadedAttachment(context.Background(), m, u)
				if err != nil || sent.ID != "sent" || sent.Timestamp != "timestamp" {
					t.Fatal(sent, err)
				}
				if _, err := c.SendAttachment(context.Background(), m); err != nil {
					t.Fatal(err)
				}
				if uploads.Load() != 2 || sends.Load() != 2 || tokens.Load() != 1 {
					t.Fatal("incorrect request counts")
				}
			})
		}
	}
}

func TestMediaValidationZeroRequests(t *testing.T) {
	calls := 0
	c := mediaMockClient(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected network") })
	for _, raw := range []string{
		"", "/tmp/a.png", "file:///tmp/a.png", "data:image/png;base64,xxx", "ftp://example.com/a", "//example.com/a",
		"https://user:secret@example.com/a", "https://example.com/a#fragment", "https://example.com/a#",
		"https://example.com/a\n", "https://example.com/a%0d", "https://example.com/a\u0085", "https://example.com/a%7f",
		"https://example.com/" + strings.Repeat("x", 8192), "https://example.com/\xff", "https://example.com/%zz",
		"http://localhost/a", "http://LOCALHOST./a", "http://a.localhost/a", "http://host.local/a", "http://host.internal/a",
		"http://127.0.0.1/a", "http://10.0.0.1/a", "http://172.16.0.1/a", "http://192.168.0.1/a", "http://169.254.169.254/a",
		"http://100.64.0.1/a", "http://0.0.0.0/a", "http://224.0.0.1/a", "http://255.255.255.255/a", "http://198.18.0.1/a",
		"http://192.0.2.1/a", "http://198.51.100.1/a", "http://203.0.113.1/a", "http://240.0.0.1/a",
		"http://[::1]/a", "http://[::]/a", "http://[fc00::1]/a", "http://[fe80::1]/a", "http://[ff02::1]/a",
		"http://[::ffff:127.0.0.1]/a", "http://[64:ff9b::a00:1]/a", "http://[2001:db8::1]/a", "http://[2002:7f00:1::]/a",
		"http://[fe80::1%25en0]/a", "http://[example.com]:80/a", "http://127.1/a", "http://2130706433/a", "http://0x7f000001/a",
		"http://0177.0.0.1/a", "http://127.0.0.1./a", "http://0x7f.0.0.0x1/a", "http://example.com:8080/a", "http://example.com:/a",
		"http://example.com:080/a", "http://example.com:bad/a", "http://example..com/a", "http://-example.com/a",
	} {
		m := mediaMessage()
		m.Attachment.URL = raw
		if _, err := c.SendAttachment(context.Background(), m); err == nil || (raw != "" && strings.Contains(err.Error(), raw)) {
			t.Fatal("invalid URL accepted or leaked")
		}
		if _, err := c.UploadAttachment(context.Background(), m.TargetType, m.TargetID, m.Attachment); err == nil {
			t.Fatal("upload accepted invalid URL")
		}
		if _, err := c.SendUploadedAttachment(context.Background(), m, UploadedAttachment{}); err == nil {
			t.Fatal("send accepted invalid URL")
		}
	}
	for _, kind := range []AttachmentKind{AttachmentVideo, AttachmentAudio, AttachmentFile, "", "unknown-sensitive"} {
		m := mediaMessage()
		m.Attachment.Kind = kind
		if _, err := c.SendAttachment(context.Background(), m); !errors.Is(err, ErrUnsupportedAttachment) {
			t.Fatal(err)
		}
		if _, err := c.UploadAttachment(context.Background(), m.TargetType, m.TargetID, m.Attachment); !errors.Is(err, ErrUnsupportedAttachment) {
			t.Fatal(err)
		}
	}
	for _, mutate := range []func(*AttachmentMessage){
		func(m *AttachmentMessage) { m.TargetType = "channel" },
		func(m *AttachmentMessage) { m.TargetID = "../users" },
		func(m *AttachmentMessage) { m.MsgID = "\n" },
		func(m *AttachmentMessage) { m.MsgID = ""; m.MsgSeq = 1 },
	} {
		m := mediaMessage()
		mutate(&m)
		if _, err := c.SendAttachment(context.Background(), m); err == nil {
			t.Fatal("accepted invalid metadata")
		}
	}
	for _, raw := range []string{"http://example.com/a", "https://EXAMPLE.com.:443/a?q=%20", "http://example.com:80/a", "https://8.8.8.8/a", "https://[2606:4700:4700::1111]/a", "https://[::ffff:8.8.8.8]/a"} {
		m := mediaMessage()
		m.Attachment.URL = raw
		if err := ValidateAttachmentMessage(m); err != nil {
			t.Fatal("rejected public URL", err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid inputs made requests", calls)
	}
}

func TestMediaCredentialScope(t *testing.T) {
	calls := 0
	c := mediaMockClient(func(*http.Request) (*http.Response, error) {
		calls++
		return mediaResponse(`{"file_info":"opaque","ttl":60}`), nil
	})
	m := mediaMessage()
	u, err := c.UploadAttachment(context.Background(), m.TargetType, m.TargetID, m.Attachment)
	if err != nil {
		t.Fatal(err)
	}
	other := mediaMockClient(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected") })
	for _, mode := range []string{"zero", "expired", "target-type", "target-id", "client", "kind", "missing", "json-forged"} {
		t.Run(mode, func(t *testing.T) {
			v, msg, client := u, m, c
			switch mode {
			case "zero":
				v = UploadedAttachment{}
			case "expired":
				v.expiresAt = time.Now()
			case "target-type":
				msg.TargetType = "group"
			case "target-id":
				msg.TargetID = "user_2"
			case "client":
				client = other
			case "kind":
				v.kind = AttachmentVideo
			case "missing":
				v.fileInfo = ""
			case "json-forged":
				v = UploadedAttachment{}
				if err := json.Unmarshal([]byte(`{"file_info":"forged","fileInfo":"forged","ttl":60}`), &v); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := client.SendUploadedAttachment(context.Background(), msg, v); err == nil {
				t.Fatal("accepted invalid credential")
			}
		})
	}
	if calls != 1 {
		t.Fatal("invalid credential made requests", calls)
	}
}

func TestMediaFailuresNoRetry(t *testing.T) {
	for _, stage := range []string{"auth", "upload", "send"} {
		for _, body := range []string{`{}`, `null`, `broken`, `{"code":123,"message":"sensitive"}`, `{"file_info":"opaque"}`, `{"file_info":123,"ttl":60}`, `{"file_info":" ","ttl":60}`, `{"file_info":"opaque","ttl":0}`, `{"file_info":"opaque","ttl":-1}`, `{"file_info":"opaque","ttl":9223372037}`, `{"file_info":"opaque","ttl":1.5}`, `{"file_info":"opaque","ttl":"60"}`, `{"id":123}`, `{"id":""}`, "401", "transport", "read", "oversized"} {
			t.Run(stage+"/"+body, func(t *testing.T) {
				calls, sends := 0, 0
				c := mediaMockClient(func(r *http.Request) (*http.Response, error) {
					calls++
					if strings.HasSuffix(r.URL.Path, "/messages") {
						sends++
					}
					if stage == "send" && strings.HasSuffix(r.URL.Path, "/files") {
						return mediaResponse(`{"file_info":"opaque","ttl":60}`), nil
					}
					switch body {
					case "401":
						resp := mediaResponse("sensitive malformed response")
						resp.StatusCode = 401
						return resp, nil
					case "transport":
						return nil, errors.New("sensitive transport URL")
					case "read":
						resp := mediaResponse("")
						resp.Body = sendErrorBody{errors.New("sensitive body")}
						return resp, nil
					case "oversized":
						return mediaResponse(strings.Repeat("x", (1<<20)+1)), nil
					}
					return mediaResponse(body), nil
				})
				if stage == "auth" {
					c.token = ""
				}
				var logs bytes.Buffer
				c.cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
				sent, err := c.SendAttachment(context.Background(), mediaMessage())
				if err == nil || sent.ID != "" {
					t.Fatal("failure acknowledged as success")
				}
				wantCalls, wantSends := 1, 0
				if stage == "send" {
					wantCalls, wantSends = 2, 1
				}
				if calls != wantCalls || sends != wantSends {
					t.Fatal("retried or sent after failure", calls, sends)
				}
				if body == "401" && c.token != "" {
					t.Fatal("401 kept token")
				}
				for _, secret := range []string{"sensitive", "opaque", "signature=hidden", "cdn.example.com"} {
					if strings.Contains(err.Error()+logs.String(), secret) {
						t.Fatal("error/log leaked data")
					}
				}
			})
		}
	}
}

func TestMediaTimeouts(t *testing.T) {
	for _, stage := range []string{"auth", "upload", "send"} {
		for _, bodyTimeout := range []bool{false, true} {
			for _, parentTimeout := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/body=%v/parent=%v", stage, bodyTimeout, parentTimeout), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						calls := 0
						c := mediaMockClient(func(r *http.Request) (*http.Response, error) {
							calls++
							if stage == "send" && strings.HasSuffix(r.URL.Path, "/files") {
								return mediaResponse(`{"file_info":"opaque","ttl":60}`), nil
							}
							if bodyTimeout {
								resp := mediaResponse("")
								resp.Body = sendDeadlineBody{r.Context()}
								return resp, nil
							}
							<-r.Context().Done()
							return nil, r.Context().Err()
						})
						if stage == "auth" {
							c.token = ""
						}
						ctx := context.Background()
						if parentTimeout {
							var cancel context.CancelFunc
							ctx, cancel = context.WithTimeout(ctx, time.Second)
							defer cancel()
						}
						sent, err := c.SendAttachment(ctx, mediaMessage())
						want := 1
						if stage == "send" {
							want = 2
						}
						if err == nil || sent.ID != "" || calls != want || errors.Is(err, context.DeadlineExceeded) != parentTimeout || errors.Is(err, context.Canceled) {
							t.Fatal("timeout misclassified or retried", err, calls)
						}
					})
				})
			}
		}
	}
}

func TestMediaTTL(t *testing.T) {
	for _, stage := range []string{"upload", "upload-auth", "send-auth", "elapsed", "remaining"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls, sends := 0, 0
				c := mediaMockClient(func(r *http.Request) (*http.Response, error) {
					calls++
					if strings.HasSuffix(r.URL.Path, "/messages") {
						sends++
						return mediaResponse(`{"id":"sent"}`), nil
					}
					if r.URL.Path == "/app/getAppAccessToken" {
						time.Sleep(2 * time.Second)
						return mediaResponse(`{"access_token":"token","expires_in":7200}`), nil
					}
					if stage == "upload" {
						time.Sleep(2 * time.Second)
					}
					if stage == "remaining" {
						time.Sleep(500 * time.Millisecond)
					}
					return mediaResponse(`{"file_info":"opaque","ttl":1}`), nil
				})
				m := mediaMessage()
				if stage == "upload-auth" {
					c.token = ""
				}
				u, err := c.UploadAttachment(context.Background(), m.TargetType, m.TargetID, m.Attachment)
				if stage == "upload" || stage == "upload-auth" {
					if !errors.Is(err, ErrProtocol) || u.fileInfo != "" {
						t.Fatal("accepted expired upload", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if stage == "remaining" && time.Until(u.expiresAt) != 500*time.Millisecond {
						t.Fatal("upload duration not deducted")
					}
					if stage == "send-auth" {
						c.token = ""
					} else {
						time.Sleep(time.Until(u.expiresAt))
					}
					if _, err := c.SendUploadedAttachment(context.Background(), m, u); err == nil {
						t.Fatal("accepted expired credential")
					}
				}
				want := 1
				if strings.HasSuffix(stage, "auth") {
					want = 2
				}
				if sends != 0 || calls != want {
					t.Fatal("expired credential sent or retried", calls, sends)
				}
			})
		})
	}
}

func TestMediaCancellationPriority(t *testing.T) {
	for _, stage := range []string{"before", "auth", "upload", "between", "send"} {
		for _, malformed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/malformed=%v", stage, malformed), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				c := mediaMockClient(func(r *http.Request) (*http.Response, error) {
					calls++
					if stage == "auth" || stage == "upload" || strings.HasSuffix(r.URL.Path, "/messages") {
						cancel()
						if malformed {
							return mediaResponse(`broken`), nil
						}
					}
					if strings.HasSuffix(r.URL.Path, "/messages") {
						return mediaResponse(`{"id":"sent"}`), nil
					}
					return mediaResponse(`{"file_info":"opaque","ttl":60}`), nil
				})
				m := mediaMessage()
				if stage == "before" {
					cancel()
				}
				if stage == "auth" {
					c.token = ""
				}
				var sent SentMessage
				var err error
				if stage == "between" {
					u, uploadErr := c.UploadAttachment(ctx, m.TargetType, m.TargetID, m.Attachment)
					if uploadErr != nil {
						t.Fatal(uploadErr)
					}
					cancel()
					sent, err = c.SendUploadedAttachment(ctx, m, u)
				} else {
					sent, err = c.SendAttachment(ctx, m)
				}
				want := 1
				if stage == "before" {
					want = 0
				}
				if stage == "send" {
					want = 2
				}
				if err != context.Canceled || sent.ID != "" || calls != want {
					t.Fatal("cancellation not prioritized", err, calls)
				}
			})
		}
	}
}
