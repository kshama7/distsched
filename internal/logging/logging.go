// Package logging builds the structured logger used by all binaries. It uses
// the standard library log/slog for now; Milestone 7 swaps the backend to zap
// behind this same constructor.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON structured logger writing to stderr at the given level
// ("debug", "info", "warn", "error"; defaults to info).
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
