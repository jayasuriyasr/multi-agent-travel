// Package config provides centralized, environment-based configuration.
// Single source of truth — no scattered os.Getenv() calls elsewhere.
package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Config holds all application configuration.
type Config struct {
	// Postgres
	PgDSN string

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Fiber HTTP
	FiberPort string

	// Asynq
	AsynqConcurrency int

	// Schedule watcher
	WatcherInterval time.Duration

	// Logging
	LogLevel string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		PgDSN:            envOrDefault("PG_DSN", "postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable"),
		RedisAddr:        envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    envOrDefault("REDIS_PASSWORD", ""),
		RedisDB:          envIntOrDefault("REDIS_DB", 0),
		FiberPort:        envOrDefault("FIBER_PORT", "8080"),
		AsynqConcurrency: envIntOrDefault("ASYNQ_CONCURRENCY", 20),
		WatcherInterval:  time.Duration(envIntOrDefault("WATCHER_INTERVAL_SEC", 120)) * time.Second,
		LogLevel:         envOrDefault("LOG_LEVEL", "info"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// ── Connection Pool Initializers ─────────────────────────────────────────────

// InitPostgres creates and validates a pgxpool connection pool.
// Calls log.Fatalf if the DSN is unparseable or the server is unreachable.
func InitPostgres(ctx context.Context, connString string) *pgxpool.Pool {
	poolCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalf("[config] postgres: failed to parse DSN: %v", err)
	}
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("[config] postgres: failed to create pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("[config] postgres: ping failed — is the container running? %v", err)
	}

	fmt.Println("[config] postgres: connection pool ready")
	return pool
}

// InitRedis creates and validates a Redis client from a redis:// URL.
// Calls log.Fatalf if the URL is unparseable or Redis is unreachable.
func InitRedis(ctx context.Context, redisURL string) *redis.Client {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("[config] redis: failed to parse URL %q: %v", redisURL, err)
	}

	rdb := redis.NewClient(opts)

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[config] redis: ping failed — is the container running? %v", err)
	}

	fmt.Println("[config] redis: client ready")
	return rdb
}
