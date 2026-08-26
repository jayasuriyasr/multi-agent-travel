// Package config is the single source of truth for runtime configuration.
//
// Every tunable is read here, exactly once, at startup. No other package calls
// os.Getenv. That rule is what makes the environment variables in
// docker-compose.yml and .env.example actually mean something — a setting that
// is documented but never read is worse than no setting at all.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/seat"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Config holds all application configuration.
type Config struct {
	// Postgres
	PgDSN         string
	PgMaxConns    int32
	MigrationsDir string
	AutoMigrate   bool

	// Redis
	RedisURL      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// HTTP
	HTTPPort        string
	HTTPReadTimeout time.Duration
	RequestTimeout  time.Duration
	BodyLimitBytes  int
	RateLimitPerMin int
	CORSOrigins     string
	WebRoot         string

	// Asynq
	AsynqConcurrency int

	// Schedule watcher
	WatcherInterval time.Duration

	// Schedule loading
	MaxWalkSeconds         int
	MaxFootpathsPerStation int

	// Seat polling
	ClassifyInterval time.Duration
	SeatProvider     string
	SeatAPIBaseURL   string
	SeatAPIKey       string
	SeatAPITimeout   time.Duration

	// Redis batching. These interact with cluster limits and latency budgets,
	// so an operator hitting a problem should be able to change them without a
	// recompile.
	RedisMGetChunk    int
	StreamBatchSize   int
	MinResyncInterval time.Duration

	// Search
	MaxRounds          int
	MinTransferSeconds int
	DateWindowDays     int
	SearchCandidates   int
	SearchResults      int
	StrictSeatMode     bool

	// Seat-validation circuit breaker.
	BreakerThreshold int
	BreakerCooldown  time.Duration

	// Logging
	LogLevel  string
	LogFormat string
}

// Load reads configuration from the environment, applying documented defaults,
// and returns it alongside any validation problems found.
func Load() (*Config, error) {
	c := &Config{
		PgDSN:         env("PG_DSN", "postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable"),
		PgMaxConns:    int32(envInt("PG_MAX_CONNS", 10)),
		MigrationsDir: env("MIGRATIONS_DIR", "./migrations"),
		AutoMigrate:   envBool("AUTO_MIGRATE", true),

		RedisURL:      env("REDIS_URL", "redis://localhost:6379/0"),
		RedisAddr:     env("REDIS_ADDR", ""),
		RedisPassword: env("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),

		HTTPPort:        env("FIBER_PORT", "8080"),
		HTTPReadTimeout: envDuration("HTTP_READ_TIMEOUT_SEC", 15*time.Second),
		RequestTimeout:  envDuration("REQUEST_TIMEOUT_SEC", 10*time.Second),
		BodyLimitBytes:  envInt("BODY_LIMIT_BYTES", 1<<20),
		RateLimitPerMin: envInt("RATE_LIMIT_PER_MIN", 600),
		CORSOrigins:     env("CORS_ORIGINS", "*"),
		WebRoot:         env("WEB_ROOT", "./web"),

		AsynqConcurrency: envInt("ASYNQ_CONCURRENCY", 20),
		WatcherInterval:  envDuration("WATCHER_INTERVAL_SEC", 30*time.Second),

		MaxWalkSeconds:         envInt("MAX_WALK_SECONDS", schedule.DefaultMaxWalkSeconds),
		MaxFootpathsPerStation: envInt("MAX_FOOTPATHS_PER_STATION", schedule.DefaultMaxFootpathsPerStation),

		RedisMGetChunk:    envInt("REDIS_MGET_CHUNK", seat.DefaultMGetChunk),
		StreamBatchSize:   envInt("STREAM_BATCH_SIZE", seat.DefaultStreamBatch),
		MinResyncInterval: envDuration("MIN_RESYNC_INTERVAL_SEC", seat.DefaultMinResyncInterval),

		ClassifyInterval: envDuration("CLASSIFY_INTERVAL_SEC", 5*time.Minute),
		SeatProvider:     strings.ToLower(env("SEAT_PROVIDER", "mock")),
		SeatAPIBaseURL:   env("SEAT_API_BASE_URL", ""),
		SeatAPIKey:       env("SEAT_API_KEY", ""),
		SeatAPITimeout:   envDuration("SEAT_API_TIMEOUT_SEC", 5*time.Second),

		MaxRounds:          envInt("MAX_ROUNDS", model.DefaultMaxRounds),
		MinTransferSeconds: envInt("MIN_TRANSFER_SECONDS", model.DefaultMinTransferSeconds),
		DateWindowDays:     envInt("DATE_WINDOW_DAYS", model.DefaultDateWindowDays),
		SearchCandidates:   envInt("SEARCH_CANDIDATES", model.MaxAllowedRounds+1),
		SearchResults:      envInt("SEARCH_RESULTS", 5),
		StrictSeatMode:     envBool("STRICT_SEAT_MODE", false),

		BreakerThreshold: envInt("SEAT_BREAKER_THRESHOLD", 5),
		BreakerCooldown:  envDuration("SEAT_BREAKER_COOLDOWN_SEC", 15*time.Second),

		LogLevel:  strings.ToLower(env("LOG_LEVEL", "info")),
		LogFormat: strings.ToLower(env("LOG_FORMAT", "text")),
	}

	// asynq takes host:port, not a redis:// URL. Derive one from the other so a
	// deployment only has to set REDIS_URL.
	if c.RedisAddr == "" {
		opts, err := redis.ParseURL(c.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL %q is not a valid redis URL: %w", c.RedisURL, err)
		}
		c.RedisAddr = opts.Addr
		if c.RedisPassword == "" {
			c.RedisPassword = opts.Password
		}
		if c.RedisDB == 0 {
			c.RedisDB = opts.DB
		}
	}

	return c, c.Validate()
}

// Validate rejects configurations that cannot work, so the process fails at
// boot with a clear message instead of misbehaving under load.
func (c *Config) Validate() error {
	var problems []string

	if c.PgDSN == "" {
		problems = append(problems, "PG_DSN must not be empty")
	}
	if c.PgMaxConns < 1 {
		problems = append(problems, "PG_MAX_CONNS must be >= 1")
	}
	if c.AsynqConcurrency < 1 {
		problems = append(problems, "ASYNQ_CONCURRENCY must be >= 1")
	}
	if c.MaxRounds < 1 || c.MaxRounds > model.MaxAllowedRounds {
		problems = append(problems, fmt.Sprintf("MAX_ROUNDS must be between 1 and %d", model.MaxAllowedRounds))
	}
	if c.MinTransferSeconds < 0 {
		problems = append(problems, "MIN_TRANSFER_SECONDS must be >= 0")
	}
	if c.DateWindowDays < 0 || c.DateWindowDays > 30 {
		problems = append(problems, "DATE_WINDOW_DAYS must be between 0 and 30")
	}
	if c.SearchResults < 1 {
		problems = append(problems, "SEARCH_RESULTS must be >= 1")
	}
	if c.SearchCandidates < c.SearchResults {
		problems = append(problems, "SEARCH_CANDIDATES must be >= SEARCH_RESULTS")
	}
	if c.MaxWalkSeconds < 0 || c.MaxWalkSeconds > 7200 {
		problems = append(problems, "MAX_WALK_SECONDS must be between 0 and 7200")
	}
	if c.MaxFootpathsPerStation < 1 {
		problems = append(problems, "MAX_FOOTPATHS_PER_STATION must be >= 1")
	}
	if c.RedisMGetChunk < 1 || c.RedisMGetChunk > 10000 {
		problems = append(problems, "REDIS_MGET_CHUNK must be between 1 and 10000")
	}
	if c.StreamBatchSize < 1 || c.StreamBatchSize > 10000 {
		problems = append(problems, "STREAM_BATCH_SIZE must be between 1 and 10000")
	}
	if c.BreakerThreshold < 1 {
		problems = append(problems, "SEAT_BREAKER_THRESHOLD must be >= 1 (the breaker cannot be disabled)")
	}
	if c.RateLimitPerMin < 0 {
		problems = append(problems, "RATE_LIMIT_PER_MIN must be >= 0 (0 disables the limiter)")
	}
	switch c.SeatProvider {
	case "mock":
	case "http":
		if c.SeatAPIBaseURL == "" {
			problems = append(problems, "SEAT_PROVIDER=http requires SEAT_API_BASE_URL")
		}
	default:
		problems = append(problems, fmt.Sprintf("SEAT_PROVIDER %q must be \"mock\" or \"http\"", c.SeatProvider))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		problems = append(problems, fmt.Sprintf("LOG_LEVEL %q must be debug, info, warn or error", c.LogLevel))
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

// SlogLevel maps the configured level onto slog.
func (c *Config) SlogLevel() slog.Level {
	switch c.LogLevel {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetupLogging installs the process-wide slog handler.
//
// Search logging sits on the hot path, so it is emitted at debug level and
// guarded by an Enabled check: at the default info level a search does no
// formatting and takes no logger lock at all.
func (c *Config) SetupLogging() {
	opts := &slog.HandlerOptions{Level: c.SlogLevel()}
	var h slog.Handler
	if c.LogFormat == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h))
}

// Redacted returns the config with secrets masked, safe to log.
func (c Config) Redacted() Config {
	c.PgDSN = redactDSN(c.PgDSN)
	if c.RedisPassword != "" {
		c.RedisPassword = "***"
	}
	if c.SeatAPIKey != "" {
		c.SeatAPIKey = "***"
	}
	return c
}

// redactDSN masks the password inside a postgres:// connection string.
func redactDSN(dsn string) string {
	at := strings.LastIndex(dsn, "@")
	scheme := strings.Index(dsn, "://")
	if at < 0 || scheme < 0 || at < scheme {
		return dsn
	}
	creds := dsn[scheme+3 : at]
	colon := strings.Index(creds, ":")
	if colon < 0 {
		return dsn
	}
	return dsn[:scheme+3] + creds[:colon] + ":***" + dsn[at:]
}

// ── Environment helpers ──────────────────────────────────────────────────────

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		slog.Warn("ignoring non-numeric environment value", "key", key, "value", v, "using", fallback)
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		slog.Warn("ignoring non-boolean environment value", "key", key, "value", v, "using", fallback)
	}
	return fallback
}

// envDuration reads a value expressed in seconds.
func envDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
		slog.Warn("ignoring invalid duration (expected whole seconds)", "key", key, "value", v, "using", fallback)
	}
	return fallback
}

// ── Connection pools ─────────────────────────────────────────────────────────

// InitPostgres creates and verifies a pgx connection pool.
//
// It returns an error rather than calling log.Fatal: a library that kills the
// process denies the caller any chance to shut the rest of the system down
// cleanly, and makes the function untestable.
func InitPostgres(ctx context.Context, c *Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(c.PgDSN)
	if err != nil {
		return nil, fmt.Errorf("parse PG_DSN: %w", err)
	}
	poolCfg.MaxConns = c.PgMaxConns
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres unreachable at %s: %w", redactDSN(c.PgDSN), err)
	}

	slog.Info("postgres pool ready", "max_conns", c.PgMaxConns)
	return pool, nil
}

// InitRedis creates and verifies a Redis client.
func InitRedis(ctx context.Context, c *Config) (*redis.Client, error) {
	opts, err := redis.ParseURL(c.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL %q: %w", c.RedisURL, err)
	}
	if c.RedisPassword != "" {
		opts.Password = c.RedisPassword
	}
	opts.DB = c.RedisDB
	opts.ReadTimeout = 5 * time.Second
	opts.WriteTimeout = 5 * time.Second

	rdb := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis unreachable at %s: %w", opts.Addr, err)
	}

	slog.Info("redis client ready", "addr", opts.Addr, "db", opts.DB)
	return rdb, nil
}
