// Package worker wraps the Asynq server + task dispatch so both the
// standalone cmd/worker binary and the api (when running in single-process
// mode on free hosting) can boot it from the same code.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/hibiken/asynq"

	"github.com/adithya/relay/internal/job"
)

// Config wires worker dependencies.
type Config struct {
	RedisAddr     string
	RedisPassword string
	Concurrency   int
	Service       *job.Service
	Logger        *slog.Logger
}

// Worker is the Asynq consumer. Start with Run() in a goroutine, stop with
// Shutdown().
type Worker struct {
	srv *asynq.Server
	mux *asynq.ServeMux
	log *slog.Logger
}

// New builds a Worker. The supplied job.Service must already be wired with
// repository + registry + publisher.
func New(cfg Config) *Worker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPassword},
		asynq.Config{
			Concurrency:    cfg.Concurrency,
			RetryDelayFunc: retryDelayWithJitter,
			Logger:         slogAsynqLogger{log: cfg.Logger},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
				retried, _ := asynq.GetRetryCount(ctx)
				maxRetry, _ := asynq.GetMaxRetry(ctx)
				cfg.Logger.Warn("asynq task error",
					"type", t.Type(),
					"retried", retried,
					"max_retry", maxRetry,
					"error", err,
				)
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(job.TaskType, taskHandler(cfg.Service, cfg.Logger))

	return &Worker{srv: srv, mux: mux, log: cfg.Logger}
}

// Run blocks until the server stops or returns an error. Call it inside a
// goroutine in the api colocation path, or directly in the standalone binary.
func (w *Worker) Run() error {
	w.log.Info("asynq worker listening", "task_type", job.TaskType)
	return w.srv.Run(w.mux)
}

// Shutdown initiates graceful shutdown — drains in-flight tasks then stops.
// Safe to call from a different goroutine than Run().
func (w *Worker) Shutdown() { w.srv.Shutdown() }

// taskHandler adapts an Asynq task to job.Service.ExecuteJob. The Asynq task
// payload only carries the job_id; the full row lives in Postgres.
func taskHandler(svc *job.Service, log *slog.Logger) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p job.TaskPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("decode task payload: %w", err)
		}
		retry, _ := asynq.GetRetryCount(ctx)
		log.Info("execute job", "job_id", p.JobID, "retry", retry)
		return svc.ExecuteJob(ctx, p.JobID)
	}
}

// retryDelayWithJitter is exponential backoff with full jitter.
//
//	base = 5s, cap = 5min
//	raw  = min(cap, base * 2^attempt)
//	out  = rand(0, raw)
//
// Full jitter (AWS-architects' recommendation) maximises retry spread across
// a fleet without unfair clustering near the upper bound.
func retryDelayWithJitter(n int, _ error, _ *asynq.Task) time.Duration {
	const (
		base = 5 * time.Second
		cap_ = 5 * time.Minute
	)
	if n < 0 {
		n = 0
	}
	if n > 16 { // cap exponent to avoid overflow
		n = 16
	}
	raw := time.Duration(math.Min(float64(cap_), float64(base)*math.Pow(2, float64(n))))
	return time.Duration(rand.Int63n(int64(raw) + 1))
}

// slogAsynqLogger adapts a *slog.Logger to the asynq.Logger interface.
type slogAsynqLogger struct{ log *slog.Logger }

func (l slogAsynqLogger) Debug(args ...any) { l.log.Debug(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Info(args ...any)  { l.log.Info(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Warn(args ...any)  { l.log.Warn(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Error(args ...any) { l.log.Error(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Fatal(args ...any) { l.log.Error("FATAL: " + fmt.Sprint(args...)); os.Exit(1) }
