package logger

import (
	"context"
	"log/slog"
	"os"
)

// contextKey is unexported so other packages cannot accidentally use the same
// key when storing values in a context.Context, which would silently overwrite
// the logger.
type contextKey struct{}

// New returns a slog.Logger configured for the given environment.
// Production emits JSON; everything else emits human-friendly text.
func New(env string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}

	var handler slog.Handler
	if env == "production" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}

// WithContext stores the logger in ctx so handlers can retrieve it via
// FromContext without threading the logger through every function signature.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, l)
}

// FromContext returns the logger stored in ctx, or slog.Default() if none.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(contextKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
