// Package logging builds the structured logger used by all binaries. It exposes
// a standard library *slog.Logger so call sites stay decoupled from the backend,
// but the backend is Uber's zap (via zapslog) for fast, production-grade JSON
// logging.
package logging

import (
	"log/slog"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

// New returns a zap-backed slog logger writing JSON to stderr at the given level
// ("debug", "info", "warn", "error"; defaults to info).
func New(level string) *slog.Logger {
	lvl := zapcore.InfoLevel
	switch strings.ToLower(level) {
	case "debug":
		lvl = zapcore.DebugLevel
	case "warn":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "time"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), zapcore.AddSync(os.Stderr), lvl)

	return slog.New(zapslog.NewHandler(core))
}
