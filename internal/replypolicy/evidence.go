package replypolicy

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"ojj/internal/event"
	"ojj/internal/im/qq"
)

// Both clocks must be recent, non-future and causally ordered. No grace for future
// timestamps: hosts with clock skew fail closed rather than prolong authorization.
func freshTimes(received, occurred int64, now time.Time) bool {
	n := now.UnixNano()
	return occurred > 0 && received >= occurred && received <= n && occurred <= n &&
		n-received <= int64(maxEventAge) && n-occurred <= int64(maxEventAge)
}

// timeFailureReason leaves the authorization predicate unchanged. When several
// checks fail, diagnostics prefer invalid occurrence, future clocks, causal
// inversion, then receive age before occurrence age. Only fixed labels escape.
func timeFailureReason(received, occurred int64, now time.Time) string {
	if freshTimes(received, occurred, now) {
		return ""
	}
	n := now.UnixNano()
	switch {
	case occurred <= 0:
		return "event_occurred_invalid"
	case occurred > n:
		return "event_occurred_in_future"
	case received > n:
		return "event_received_in_future"
	case received < occurred:
		return "event_time_inverted"
	case n-received > int64(maxEventAge):
		return "event_received_expired"
	default:
		return "event_occurred_expired"
	}
}

// conflictingWindow rejects inconsistent snapshots before classification, including
// valid/silent or valid/malformed duplicates. Only exact duplicates may collapse.
// Cross-window snapshot identity remains a contract of the trusted event store.
func (p *Policy) conflictingWindow(events []event.Event) bool {
	ids, messages := make(map[string]string), make(map[string]string)
	for _, e := range events {
		encoded, err := json.Marshal(e)
		if err != nil {
			return true
		}
		snapshot := digest(string(encoded))
		if e.ID != "" {
			if previous, ok := ids[e.ID]; ok && previous != snapshot {
				return true
			}
			ids[e.ID] = snapshot
		}
		s := e.Source
		if e.ProducerID != "qq.websocket" || s == nil || s.Platform != "qq" || s.AccountID != p.cfg.AccountID {
			continue
		}
		key := digest(s.AccountID, s.ConversationType, s.ConversationID, s.MessageID)
		if previous, ok := messages[key]; ok && previous != snapshot {
			return true
		}
		messages[key] = snapshot
	}
	return false
}

func (p *Policy) window(events []event.Event, now time.Time) []candidate {
	out := make([]candidate, 0, len(events))
	for _, e := range events {
		s := e.Source
		if e.ProducerID != "qq.websocket" || e.EventType != "im.message" || e.SchemaVersion != 1 ||
			s == nil || s.Platform != "qq" || s.AccountID != p.cfg.AccountID ||
			!validID(s.ConversationID) || !validID(s.SenderID) || !validID(s.MessageID) ||
			!validID(e.ID) || e.Sequence < 0 || e.ReceivedAt.IsZero() || e.OccurredAt.IsZero() ||
			len(e.Content) > 64<<10 || !utf8.ValidString(e.Content) {
			continue
		}
		var body qq.Content
		if json.Unmarshal([]byte(e.Content), &body) != nil || body.Version != 1 {
			continue
		}
		switch s.ConversationType {
		case "private":
			if body.QQEvent != "C2C_MESSAGE_CREATE" || body.Author.UserOpenID != s.SenderID || s.ConversationID != s.SenderID {
				continue
			}
		case "group":
			if (body.QQEvent != "GROUP_AT_MESSAGE_CREATE" && body.QQEvent != "GROUP_MESSAGE_CREATE") || body.Author.MemberOpenID != s.SenderID {
				continue
			}
		default:
			continue
		}
		target := digest(p.cfg.AccountID, s.ConversationType, s.ConversationID)
		c := candidate{actor: digest(target, s.SenderID), target: target, message: digest(target, s.MessageID),
			token: digest(e.ID, target, s.SenderID, s.MessageID), received: e.ReceivedAt.UnixNano(),
			occurred: e.OccurredAt.UnixNano(), sequence: e.Sequence}
		c.fresh = freshTimes(c.received, c.occurred, now)
		if c.fresh {
			if s.ConversationType == "private" {
				c.mode = modePrivate
			} else {
				c.mode, c.reason = p.classify(body)
			}
		} else {
			c.reason = timeFailureReason(c.received, c.occurred, now)
		}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].received != out[j].received {
			return out[i].received < out[j].received
		}
		return out[i].sequence < out[j].sequence
	})
	return out
}

// classify only identifies explicit addressing; other trusted group messages
// are model-selected candidates subject to active budgets, not text heuristics.
func (p *Policy) classify(c qq.Content) (int, string) {
	text := strings.TrimSpace(c.Text)
	if c.QQEvent == "GROUP_AT_MESSAGE_CREATE" {
		return modeDirect, ""
	}
	for _, m := range c.Mentions {
		if p.cfg.BotUserID != "" && m.ID == p.cfg.BotUserID {
			return modeDirect, ""
		}
	}
	if c.ReferenceIndex != "" {
		if p.cfg.BotUserID != "" {
			for _, element := range c.Elements {
				if element.Index == c.ReferenceIndex && element.Author.ID == p.cfg.BotUserID {
					return modeDirect, ""
				}
			}
		}
	}
	for _, alias := range p.cfg.Aliases {
		if text == alias {
			return modeDirect, ""
		}
		if strings.HasPrefix(text, alias) {
			r, _ := utf8.DecodeRuneInString(text[len(alias):])
			if strings.ContainsRune(" ,，:：!！?？、", r) {
				return modeDirect, ""
			}
		}
	}
	return modeActive, ""
}
