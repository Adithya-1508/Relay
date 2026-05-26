package server

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adithya/relay/pkg/config"
)

// RedisConnOpts derives (addr, password) from cfg, preferring cfg.URL when
// set. Render and other PaaS provide a single REDIS_URL env var; local
// docker compose passes REDIS_ADDR + REDIS_PASSWORD. Both paths converge
// here so callers that need the raw host:port (e.g. Asynq client) can
// stay simple.
func RedisConnOpts(cfg config.RedisConfig) (addr, password string, err error) {
	if cfg.URL != "" {
		opt, perr := redis.ParseURL(cfg.URL)
		if perr != nil {
			return "", "", fmt.Errorf("parse redis url: %w", perr)
		}
		return opt.Addr, opt.Password, nil
	}
	return cfg.Addr, cfg.Password, nil
}

// NewRedis builds a redis client and verifies connectivity with a 5s ping.
// Same fail-fast philosophy as NewDBPool.
func NewRedis(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	addr, password, err := RedisConnOpts(cfg)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}
