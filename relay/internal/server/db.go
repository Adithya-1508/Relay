package server

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adithya/relay/pkg/config"
)

// NewDBPool builds a pgxpool with sizes from cfg and verifies connectivity
// with a 5s ping. Returning the error here fails the process at startup
// rather than letting the first request discover a bad DSN.
func NewDBPool(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	return newPool(ctx, cfg.URL, cfg.MaxConns, cfg.MinConns)
}

// NewReadDBPool builds a separate pgxpool for the read replica if
// cfg.ReadURL is set. Returns (nil, nil) when no read replica is configured
// — callers should fall back to the primary pool. Same fail-fast ping.
func NewReadDBPool(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	if cfg.ReadURL == "" {
		return nil, nil
	}
	return newPool(ctx, cfg.ReadURL, cfg.MaxConns, cfg.MinConns)
}

func newPool(ctx context.Context, dsn string, maxConns, minConns int32) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	poolCfg.MaxConns = maxConns
	poolCfg.MinConns = minConns
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute
	poolCfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}
