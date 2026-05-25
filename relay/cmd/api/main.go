package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adithya/relay/pkg/config"
	"github.com/adithya/relay/pkg/logger"
)


func main () {
	//Load config 

	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("Failed to Load config", "error", err)
		os.Exit(1)
	}

	log := logger.NewLogger(cfg.App.Env)
	log.Info("Starting Relay API",
	"env", cfg.App.Env,
	"port", cfg.App.Port,
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func (w http.ResponseWriter, r *http.Request)  {
		w.Header().Set("Content Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok", "service":"relay"}`)
	})

	srv := &http.Server{
	Addr: fmt.Sprintf(":%d", cfg.App.Port),
	Handler: mux,
	ReadTimeout: 10 * time.Second,
	WriteTimeout: 10 * time.Second,
	IdleTimeout: 60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)


	//Graceful shutdown 
	//When the OS sends SIGINT or SIGTERM we stop accepting new requests and
	//give in-flight requests 30 seconds to complete before shutting down the server
	go func() {
		log.Info("Server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	<-quit
	log.Info("Shutting signal received")


	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("Forced shutdown", "error", err)
		os.Exit(1)
	}

	log.Info("Server gracefully stopped")


}







