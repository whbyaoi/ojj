package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ojj/internal/app"
	"ojj/internal/minipi"

	"github.com/joho/godotenv"
)

// loadEnv reads the working-directory configuration without overriding even empty
// existing environment variables. Parse errors may contain secrets; never expose them.
func loadEnv(path string) error {
	if err := godotenv.Load(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("加载 .env 失败，请检查文件权限和格式（已隐藏错误细节）")
	}
	return nil
}

// newLogger validates configuration without echoing invalid values.
func newLogger(out io.Writer, levelName, format string) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToUpper(strings.TrimSpace(levelName)) {
	case "", "INFO":
		level = slog.LevelInfo
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		return nil, errors.New("OJJ_LOG_LEVEL 仅支持 DEBUG、INFO、WARN、ERROR")
	}
	opts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "text":
		return slog.New(slog.NewTextHandler(out, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(out, opts)), nil
	default:
		return nil, errors.New("OJJ_LOG_FORMAT 仅支持 text、json")
	}
}

func main() {
	// Bootstrap failures also use structured, redacted logs.
	logger, _ := newLogger(os.Stderr, "INFO", "text")
	slog.SetDefault(logger)
	if err := loadEnv(".env"); err != nil {
		slog.Error("配置文件加载失败，请检查权限和格式", "component", "app", "stage", "dotenv", "failure", "configuration")
		os.Exit(1)
	}
	logger, err := newLogger(os.Stderr, os.Getenv("OJJ_LOG_LEVEL"), os.Getenv("OJJ_LOG_FORMAT"))
	if err != nil {
		slog.Error("日志配置无效：级别为 DEBUG/INFO/WARN/ERROR，格式为 text/json", "component", "app", "stage", "logging", "failure", "configuration")
		os.Exit(1)
	}
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now()
	if err := app.Run(ctx); err != nil {
		minipi.LogOperation(ctx, "app", "exit", started, err, slog.LevelError)
		stop()
		os.Exit(1)
	}
}
