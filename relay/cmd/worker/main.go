package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"

	"github.com/adithya/relay/internal/hub"
	"github.com/adithya/relay/internal/job"
	"github.com/adithya/relay/internal/server"
	"github.com/adithya/relay/pkg/config"
	"github.com/adithya/relay/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	log := logger.New(cfg.App.Env)
	log.Info("starting relay worker",
		"env", cfg.App.Env,
		"concurrency", cfg.Asynq.Concurrency,
	)

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	pool, err := server.NewDBPool(rootCtx, cfg.Database)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("postgres connected")

	rdb, err := server.NewRedis(rootCtx, cfg.Redis)
	if err != nil {
		log.Error("redis init failed", "error", err)
		os.Exit(1)
	}
	defer rdb.Close() //nolint:errcheck

	redisAddr, redisPassword, err := server.RedisConnOpts(cfg.Redis)
	if err != nil {
		log.Error("redis conn opts", "error", err)
		os.Exit(1)
	}

	repo := job.NewPgRepository(pool)
	registry := job.NewRegistry()
	registry.RegisterBuiltins()
	publisher := hub.NewRedisPublisher(rdb)

	// The worker process never enqueues, so the Enqueuer dependency is left
	// nil. EnqueueJob is API-side; ExecuteJob is worker-side. We do publish
	// state-change events so connected WebSocket clients see them in
	// real time.
	svc := job.NewService(job.Config{
		Repository:         repo,
		Registry:           registry,
		Publisher:          publisher,
		Logger:             log,
		DefaultMaxAttempts: 5,
	})

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr, Password: redisPassword},
		asynq.Config{
			Concurrency:    cfg.Asynq.Concurrency,
			RetryDelayFunc: retryDelayWithJitter,
			Logger:         slogAsynqLogger{log: log},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
				retried, _ := asynq.GetRetryCount(ctx)
				maxRetry, _ := asynq.GetMaxRetry(ctx)
				log.Warn("asynq task error",
					"type", t.Type(),
					"retried", retried,
					"max_retry", maxRetry,
					"error", err,
				)
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(job.TaskType, taskHandler(svc, log))

	// Run server in a goroutine so we can drive graceful shutdown from main.
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Error("asynq server stopped", "error", err)
			os.Exit(1)
		}
	}()
	log.Info("asynq worker listening", "task_type", job.TaskType)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("shutdown signal received")

	srv.Shutdown()
	log.Info("worker stopped cleanly")
}

// taskHandler adapts an Asynq task to the job service ExecuteJob entry point.
// The Asynq task payload only carries the job_id; the full row lives in
// Postgres.
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
// Full jitter is the AWS-architects' recommendation: maximises the spread of
// retries across a fleet without unfair clustering near the upper bound.
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
	jitter := time.Duration(rand.Int63n(int64(raw) + 1))
	return jitter
}

// slogAsynqLogger adapts a *slog.Logger to the asynq.Logger interface.
type slogAsynqLogger struct{ log *slog.Logger }

func (l slogAsynqLogger) Debug(args ...any) { l.log.Debug(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Info(args ...any)  { l.log.Info(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Warn(args ...any)  { l.log.Warn(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Error(args ...any) { l.log.Error(fmt.Sprint(args...)) }
func (l slogAsynqLogger) Fatal(args ...any) { l.log.Error("FATAL: " + fmt.Sprint(args...)); os.Exit(1) }
