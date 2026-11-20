// Package agenttools supplies opt-in minipi tools. Register the returned tools in
// minipi.Config.Tools; this package does not start a model or consume events.
package agenttools

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Reject non-objects, unknown fields, duplicate keys and null field values so
// malformed calls cannot silently acquire defaults before a side effect.
func decodeArguments(raw json.RawMessage, dst any) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' || !utf8.Valid(raw) {
		return errors.New("arguments must be a UTF-8 JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if _, err := d.Token(); err != nil {
		return err
	}
	// encoding/json normally accepts case-insensitive aliases; reject those so
	// differently cased keys cannot override a schema field or evade duplicates.
	allowed := make(map[string]bool)
	typ := reflect.TypeOf(dst).Elem()
	for i := 0; i < typ.NumField(); i++ {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		allowed[name] = true
	}
	seen := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || !allowed[name] || seen[name] {
			return errors.New("invalid or duplicate argument name")
		}
		seen[name] = true
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("arguments must not contain null fields")
		}
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("arguments must contain exactly one object")
	}
	return nil
}
