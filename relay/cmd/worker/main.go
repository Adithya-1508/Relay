package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/adithya/relay/internal/hub"
	"github.com/adithya/relay/internal/job"
	"github.com/adithya/relay/internal/server"
	"github.com/adithya/relay/internal/worker"
	"github.com/adithya/relay/pkg/config"
	"github.com/adithya/relay/pkg/logger"
)

// Standalone worker binary. The actual Asynq server + handler logic lives in
// internal/worker so the api process can boot the same code in-process when
// running on free hosting (APP_RUN_WORKER_IN_PROCESS=true).
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

	// Worker process doesn't enqueue, so Enqueuer is left nil. ExecuteJob
	// transitions state and may release dependents — for those we still need
	// the enqueuer. Construct one even though we don't accept HTTP enqueues.
	enqueuer := job.NewAsynqEnqueuer(redisAddr, redisPassword)
	defer enqueuer.Close() //nolint:errcheck

	svc := job.NewService(job.Config{
		Repository:         repo,
		Enqueuer:           enqueuer,
		Registry:           registry,
		Publisher:          publisher,
		Logger:             log,
		DefaultMaxAttempts: 5,
	})

	w := worker.New(worker.Config{
		RedisAddr:     redisAddr,
		RedisPassword: redisPassword,
		Concurrency:   cfg.Asynq.Concurrency,
		Service:       svc,
		Logger:        log,
	})

	go func() {
		if err := w.Run(); err != nil {
			log.Error("asynq server stopped", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("shutdown signal received")

	w.Shutdown()
	log.Info("worker stopped cleanly")
}
