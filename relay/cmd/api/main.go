package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/adithya/relay/internal/auth"
	"github.com/adithya/relay/internal/hub"
	"github.com/adithya/relay/internal/job"
	"github.com/adithya/relay/internal/middleware"
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
	log.Info("starting relay api", "env", cfg.App.Env, "port", cfg.App.Port)

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	// OpenTelemetry: no-op when cfg.Otel.Enabled is false.
	otelShutdown, err := server.NewTracerProvider(rootCtx, cfg.Otel, "relay-api")
	if err != nil {
		log.Error("otel init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = otelShutdown(shutCtx)
	}()

	pool, err := server.NewDBPool(rootCtx, cfg.Database)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("postgres connected", "max_conns", cfg.Database.MaxConns)

	// PaaS deploys (Render etc.) set APP_AUTO_MIGRATE=true so the api applies
	// schema changes at boot. docker compose runs the standalone migrate
	// sidecar instead and leaves this false.
	if cfg.App.AutoMigrate {
		if err := server.RunMigrations(cfg.Database.URL); err != nil {
			log.Error("auto-migrate failed", "error", err)
			os.Exit(1)
		}
		log.Info("auto-migrate complete")
	}

	readPool, err := server.NewReadDBPool(rootCtx, cfg.Database)
	if err != nil {
		log.Error("postgres read replica init failed", "error", err)
		os.Exit(1)
	}
	if readPool != nil {
		defer readPool.Close()
		log.Info("postgres read replica connected")
	}

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
	log.Info("redis connected", "addr", redisAddr)

	// Auth wiring.
	authRepo := auth.NewPgRepository(pool)
	authSvc := auth.NewService(authRepo, cfg.JWT, time.Now)
	authHandler := auth.NewHandler(authSvc)

	// Realtime hub: subscribes to Redis pub/sub and fans out to local WS clients.
	wsHub := hub.NewHub(rdb, log)
	defer wsHub.Close()
	wsHandler := hub.NewHandler(wsHub)
	publisher := hub.NewRedisPublisher(rdb)

	// Job wiring.
	jobRepoOpts := []job.RepoOption{}
	if readPool != nil {
		jobRepoOpts = append(jobRepoOpts, job.WithReadPool(readPool))
	}
	jobRepo := job.NewPgRepository(pool, jobRepoOpts...)
	jobRegistry := job.NewRegistry()
	jobRegistry.RegisterBuiltins()
	enqueuer := job.NewAsynqEnqueuer(redisAddr, redisPassword)
	defer enqueuer.Close() //nolint:errcheck
	jobSvc := job.NewService(job.Config{
		Repository:         jobRepo,
		Enqueuer:           enqueuer,
		Registry:           jobRegistry,
		Publisher:          publisher,
		Logger:             log,
		DefaultMaxAttempts: 5,
	})
	jobHandler := job.NewHandler(jobSvc)

	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(
		middleware.RequestID(),
		gin.Recovery(),
		otelgin.Middleware("relay-api"),
		middleware.AccessLog(log),
	)

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "relay"})
	})

	// Static dashboard served at /. web/ ships with the binary image.
	r.StaticFile("/", "./web/index.html")
	r.Static("/assets", "./web/assets")

	// pprof: dev/staging only. Standard endpoints under /debug/pprof. CPU
	// profile via `go tool pprof http://localhost:8081/debug/pprof/profile?seconds=30`.
	if cfg.App.Env != "production" {
		registerPprof(r)
	}

	// Unauthenticated auth routes: rate-limited by client IP.
	authGroup := r.Group("/v1/auth")
	authGroup.Use(middleware.RateLimit(rdb, middleware.RateLimitConfig{
		Capacity: 20,
		Rate:     10,
	}))
	authHandler.Mount(authGroup)

	// Authenticated routes: require bearer JWT, then rate-limited per workspace.
	authed := r.Group("/v1")
	authed.Use(middleware.RequireAuth(authSvc))
	authed.Use(middleware.RateLimit(rdb, middleware.RateLimitConfig{
		Capacity: 200,
		Rate:     100,
		KeyFunc: func(c *gin.Context) string {
			id, _ := middleware.WorkspaceIDFrom(c)
			return "rl:ws:" + id.String()
		},
	}))
	authed.GET("/me", authHandler.Me)
	jobHandler.Mount(authed)

	// WebSocket endpoint: separate group so it can take ?token= query
	// fallback, which browsers need (Authorization header is unsettable on
	// the WS open request).
	wsGroup := r.Group("/v1")
	wsGroup.Use(middleware.RequireAuthWithQueryToken(authSvc))
	wsHandler.Mount(wsGroup)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.App.Port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Info("server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-quit
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("forced shutdown", "error", err)
		os.Exit(1)
	}

	log.Info("server stopped cleanly")
}

// registerPprof wires the net/http/pprof handlers under /debug/pprof on a
// Gin engine. We do this manually instead of importing gin-contrib/pprof to
// keep the dep tree small.
func registerPprof(r *gin.Engine) {
	g := r.Group("/debug/pprof")
	g.GET("/", gin.WrapF(pprof.Index))
	g.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	g.GET("/profile", gin.WrapF(pprof.Profile))
	g.GET("/symbol", gin.WrapF(pprof.Symbol))
	g.POST("/symbol", gin.WrapF(pprof.Symbol))
	g.GET("/trace", gin.WrapF(pprof.Trace))
	// Index handles the named profiles (heap, goroutine, allocs, block, mutex,
	// threadcreate) — match-any segment after the prefix.
	g.GET("/:name", gin.WrapF(pprof.Index))
}
