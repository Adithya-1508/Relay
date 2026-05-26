package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Executor is the function signature a job kind must implement. Returning a
// non-nil error marks the job failed; the service decides whether the next
// state is "failed" (retry pending) or "dead" (retries exhausted).
type Executor func(ctx context.Context, payload json.RawMessage) error

// Registry maps kind → Executor.
type Registry struct {
	executors map[string]Executor
}

// NewRegistry returns an empty Registry. Use Register to add executors.
func NewRegistry() *Registry {
	return &Registry{executors: map[string]Executor{}}
}

// Register binds a kind to an Executor. Last writer wins.
func (r *Registry) Register(kind string, exec Executor) {
	r.executors[kind] = exec
}

// Lookup returns the Executor for kind, or false if none is registered.
func (r *Registry) Lookup(kind string) (Executor, bool) {
	e, ok := r.executors[kind]
	return e, ok
}

// RegisterBuiltins binds the three demo executors (noop, delay, http) that
// ship with Relay. Real workloads register their own kinds on top.
func (r *Registry) RegisterBuiltins() {
	r.Register("noop", noopExecutor)
	r.Register("delay", delayExecutor)
	r.Register("http", httpExecutor)
}

// noopExecutor immediately succeeds. Useful for end-to-end smoke tests.
func noopExecutor(_ context.Context, _ json.RawMessage) error { return nil }

// delayPayload models the payload accepted by the "delay" executor.
type delayPayload struct {
	Milliseconds int `json:"ms"`
}

// delayExecutor sleeps the requested number of milliseconds (max 30s) then
// succeeds. Useful for exercising backpressure and graceful shutdown.
func delayExecutor(ctx context.Context, payload json.RawMessage) error {
	var p delayPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("decode delay payload: %w", err)
		}
	}
	if p.Milliseconds < 0 {
		p.Milliseconds = 0
	}
	if p.Milliseconds > 30_000 {
		p.Milliseconds = 30_000
	}
	select {
	case <-time.After(time.Duration(p.Milliseconds) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// httpPayload models the payload accepted by the "http" executor.
type httpPayload struct {
	Method     string            `json:"method"`
	URL        string            `json:"url"`
	Body       string            `json:"body"`
	Headers    map[string]string `json:"headers"`
	TimeoutSec int               `json:"timeout_sec"`
}

// httpExecutor performs a single HTTP request. Non-2xx responses become
// errors so the retry path kicks in.
func httpExecutor(ctx context.Context, payload json.RawMessage) error {
	var p httpPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("decode http payload: %w", err)
	}
	if p.URL == "" {
		return errors.New("http payload missing url")
	}
	if p.Method == "" {
		p.Method = http.MethodGet
	}
	if p.TimeoutSec <= 0 || p.TimeoutSec > 60 {
		p.TimeoutSec = 15
	}

	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(p.TimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, strings.ToUpper(p.Method), p.URL, strings.NewReader(p.Body))
	if err != nil {
		return fmt.Errorf("build http request: %w", err)
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	// Drain body even on success so the keepalive connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http non-2xx: %d", resp.StatusCode)
	}
	return nil
}
