package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoggerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		level, format string
		minimum       slog.Level
		json          bool
	}{
		{"", "", slog.LevelInfo, false},
		{" debug ", " JSON ", slog.LevelDebug, true},
		{"INFO", "text", slog.LevelInfo, false},
		{"warn", "json", slog.LevelWarn, true},
		{"ERROR", "text", slog.LevelError, false},
	} {
		t.Run(tc.level+"/"+tc.format, func(t *testing.T) {
			var buf bytes.Buffer
			logger, err := newLogger(&buf, tc.level, tc.format)
			if err != nil {
				t.Fatal(err)
			}
			for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				if logger.Enabled(context.Background(), level) != (level >= tc.minimum) {
					t.Fatal("incorrect log threshold")
				}
			}
			logger.Error("fixture", "component", "test")
			if tc.json {
				var record map[string]any
				if json.Unmarshal(buf.Bytes(), &record) != nil || record["component"] != "test" || record["level"] != "ERROR" {
					t.Fatal("invalid JSON logging")
				}
			} else if !strings.Contains(buf.String(), "level=ERROR") || !strings.Contains(buf.String(), "component=test") {
				t.Fatal("invalid text logging")
			}
		})
	}
	for _, values := range [][2]string{{"secret-marker", "text"}, {"INFO", "secret-marker"}} {
		var buf bytes.Buffer
		logger, err := newLogger(&buf, values[0], values[1])
		if logger != nil || err == nil || strings.Contains(err.Error(), "secret-marker") || buf.Len() != 0 {
			t.Fatal("invalid configuration accepted or leaked")
		}
	}
}

func TestLoadEnv(t *testing.T) {
	const key = "OJJ_TEST_DOTENV_VALUE"
	for _, tc := range []struct {
		name     string
		content  string
		existing bool
		initial  string
		want     string
		wantErr  bool
	}{
		{name: "quoted value", content: key + "='hello # $world'\n", want: "hello # $world"},
		{name: "environment wins", content: key + "=file\n", existing: true, initial: "host", want: "host"},
		{name: "empty environment wins", content: key + "=file\n", existing: true},
		{name: "malformed is atomic and redacted", content: key + "=file\nINVALID!KEY=secret-marker\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.initial)
			if !tc.existing {
				if err := os.Unsetenv(key); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), ".env")
			if err := os.WriteFile(path, []byte(tc.content), 0600); err != nil {
				t.Fatal(err)
			}
			err := loadEnv(path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected load result: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "secret-marker") {
				t.Fatal("parse error leaked configuration")
			}
			if got := os.Getenv(key); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			if tc.wantErr {
				if _, ok := os.LookupEnv(key); ok {
					t.Fatal("malformed file partially loaded")
				}
			}
		})
	}
	t.Run("missing is optional", func(t *testing.T) {
		if err := loadEnv(filepath.Join(t.TempDir(), ".env")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unreadable is fatal", func(t *testing.T) {
		if err := loadEnv(t.TempDir()); err == nil {
			t.Fatal("directory accepted as dotenv file")
		}
	})
}
