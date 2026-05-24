package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application.
// Values are populated from environment variables and config.yaml.
// The google style guide: exported types get doc comments.
type Config struct {
	App      AppConfig
	Database DatabaseConfig
	Redis    RedisConfig
	JWT      JWTConfig
	Asynq    AsynqConfig
	Otel     OtelConfig
}

type AppConfig struct {
	Env    string
	Port   int
	Secret string
}

type DatabaseConfig struct {
	URL      string
	MaxConns int32
	MinConns int32
}

type RedisConfig struct {
	Addr     string
	Password string
}

type JWTConfig struct {
	AccessSecret  string
	RefreshSecret string
	AccessExpiry  time.Duration
	RefreshExpiry time.Duration
}

type AsynqConfig struct {
	Concurrency int
}

type OtelConfig struct {
	Enabled  bool
	Endpoint string
}

// Load reads configuration from environment variables and config.yaml.
// Environment variables take precedence over the config file.
// Returns an error if any required field is missing.
func Load() (*Config, error) {
	v := viper.New()

	// Tell viper to look for config.yaml in the current directory.
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	// AutomaticEnv makes every env var available.
	// SetEnvKeyReplacer makes DATABASE_URL map to database.url in the struct.
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Read config file — not fatal if missing, env vars are enough.
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
	}

	// Defaults — these apply when neither config.yaml nor env var sets a value.
	v.SetDefault("app.port", 8081)
	v.SetDefault("app.env", "development")
	v.SetDefault("database.max_conns", 25)
	v.SetDefault("database.min_conns", 5)
	v.SetDefault("asynq.concurrency", 10)
	v.SetDefault("otel.enabled", false)
	v.SetDefault("jwt.access_expiry", "15m")
	v.SetDefault("jwt.refresh_expiry", "168h") // 7 days

	accessExpiry, err := time.ParseDuration(v.GetString("jwt.access_expiry"))
	if err != nil {
		return nil, fmt.Errorf("parsing jwt.access_expiry: %w", err)
	}

	refreshExpiry, err := time.ParseDuration(v.GetString("jwt.refresh_expiry"))
	if err != nil {
		return nil, fmt.Errorf("parsing jwt.refresh_expiry: %w", err)
	}

	cfg := &Config{
		App: AppConfig{
			Env:    v.GetString("app.env"),
			Port:   v.GetInt("app.port"),
			Secret: v.GetString("app.secret"),
		},
		Database: DatabaseConfig{
			URL:      v.GetString("database.url"),
			MaxConns: int32(v.GetInt("database.max_conns")),
			MinConns: int32(v.GetInt("database.min_conns")),
		},
		Redis: RedisConfig{
			Addr:     v.GetString("redis.addr"),
			Password: v.GetString("redis.password"),
		},
		JWT: JWTConfig{
			AccessSecret:  v.GetString("jwt.access_secret"),
			RefreshSecret: v.GetString("jwt.refresh_secret"),
			AccessExpiry:  accessExpiry,
			RefreshExpiry: refreshExpiry,
		},
		Asynq: AsynqConfig{
			Concurrency: v.GetInt("asynq.concurrency"),
		},
		Otel: OtelConfig{
			Enabled:  v.GetBool("otel.enabled"),
			Endpoint: v.GetString("otel.endpoint"),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

// validate checks that all required config values are present.
// Fail fast at startup — better to crash immediately than to
// discover a missing secret when the first request hits auth.
func (c *Config) validate() error {
	if c.Database.URL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("REDIS_ADDR is required")
	}
	if c.JWT.AccessSecret == "" {
		return fmt.Errorf("JWT_ACCESS_SECRET is required")
	}
	if c.JWT.RefreshSecret == "" {
		return fmt.Errorf("JWT_REFRESH_SECRET is required")
	}
	return nil
}
