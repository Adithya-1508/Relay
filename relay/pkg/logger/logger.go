package logger

import (
	"context"
	"log/slog"
	"os"
)

//ContextKey is unexported - prevents other packages from accidentally using the
// same key in context, which would silently overwrite the logger.


type ContextKey struct {}


func New(env string) *slog.Logger {
	var handler slog.Handler

	opts := &slog.HandlerOptions{
		Level : slog.LevelDebug,
		AddSource: true,
	}

	if env == "production"{
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler =  slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}


// WithContext stores the logger in a context so handlers can retrieve it without 
// threading the logger every function signature

func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ContextKey{}, l)
}


//FromContext retrieves the logger from a context.
// If no logger is found, it returns a shared default logger.

func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ContextKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}