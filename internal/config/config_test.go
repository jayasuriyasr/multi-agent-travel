package config

import (
	"strings"
	"testing"
	"time"

	"axentra/internal/schedule"
	"axentra/internal/seat"
)

func TestLoad_Defaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	if c.HTTPPort != "8080" {
		t.Errorf("HTTPPort = %q, want 8080", c.HTTPPort)
	}
	if c.AsynqConcurrency != 20 {
		t.Errorf("AsynqConcurrency = %d, want 20", c.AsynqConcurrency)
	}
	if c.WatcherInterval != 30*time.Second {
		t.Errorf("WatcherInterval = %v, want 30s", c.WatcherInterval)
	}
	// asynq needs host:port; deriving it from REDIS_URL means a deployment only
	// has to set one variable.
	if c.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want it derived from REDIS_URL", c.RedisAddr)
	}

	// The tuning that used to be compiled in must arrive with working defaults,
	// so promoting it to config did not become "now you must set it".
	if c.RedisMGetChunk != seat.DefaultMGetChunk {
		t.Errorf("RedisMGetChunk = %d, want %d", c.RedisMGetChunk, seat.DefaultMGetChunk)
	}
	if c.StreamBatchSize != seat.DefaultStreamBatch {
		t.Errorf("StreamBatchSize = %d, want %d", c.StreamBatchSize, seat.DefaultStreamBatch)
	}
	if c.MinResyncInterval != seat.DefaultMinResyncInterval {
		t.Errorf("MinResyncInterval = %v, want %v", c.MinResyncInterval, seat.DefaultMinResyncInterval)
	}
	if c.MaxWalkSeconds != schedule.DefaultMaxWalkSeconds {
		t.Errorf("MaxWalkSeconds = %d, want %d", c.MaxWalkSeconds, schedule.DefaultMaxWalkSeconds)
	}
	if c.BreakerThreshold < 1 {
		t.Errorf("BreakerThreshold = %d, want a working default", c.BreakerThreshold)
	}
}

// Every variable documented in .env.example and docker-compose.yml must
// actually reach the running system. A setting that is advertised but never
// read is worse than no setting at all.
func TestLoad_ReadsEveryDocumentedVariable(t *testing.T) {
	t.Setenv("FIBER_PORT", "9999")
	t.Setenv("ASYNQ_CONCURRENCY", "7")
	t.Setenv("WATCHER_INTERVAL_SEC", "120")
	t.Setenv("REDIS_PASSWORD", "hunter2")
	t.Setenv("REDIS_DB", "3")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("MAX_ROUNDS", "6")
	t.Setenv("MIN_TRANSFER_SECONDS", "180")
	t.Setenv("DATE_WINDOW_DAYS", "4")
	t.Setenv("STRICT_SEAT_MODE", "true")
	t.Setenv("SEARCH_RESULTS", "9")
	t.Setenv("SEARCH_CANDIDATES", "20")
	t.Setenv("RATE_LIMIT_PER_MIN", "42")
	t.Setenv("CLASSIFY_INTERVAL_SEC", "60")
	t.Setenv("REDIS_MGET_CHUNK", "256")
	t.Setenv("STREAM_BATCH_SIZE", "128")
	t.Setenv("MIN_RESYNC_INTERVAL_SEC", "90")
	t.Setenv("MAX_WALK_SECONDS", "900")
	t.Setenv("MAX_FOOTPATHS_PER_STATION", "32")
	t.Setenv("SEAT_BREAKER_THRESHOLD", "9")
	t.Setenv("SEAT_BREAKER_COOLDOWN_SEC", "45")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"FIBER_PORT", c.HTTPPort, "9999"},
		{"ASYNQ_CONCURRENCY", c.AsynqConcurrency, 7},
		{"WATCHER_INTERVAL_SEC", c.WatcherInterval, 120 * time.Second},
		{"REDIS_PASSWORD", c.RedisPassword, "hunter2"},
		{"REDIS_DB", c.RedisDB, 3},
		{"LOG_LEVEL", c.LogLevel, "debug"},
		{"MAX_ROUNDS", c.MaxRounds, 6},
		{"MIN_TRANSFER_SECONDS", c.MinTransferSeconds, 180},
		{"DATE_WINDOW_DAYS", c.DateWindowDays, 4},
		{"STRICT_SEAT_MODE", c.StrictSeatMode, true},
		{"SEARCH_RESULTS", c.SearchResults, 9},
		{"SEARCH_CANDIDATES", c.SearchCandidates, 20},
		{"RATE_LIMIT_PER_MIN", c.RateLimitPerMin, 42},
		{"CLASSIFY_INTERVAL_SEC", c.ClassifyInterval, 60 * time.Second},
		{"REDIS_MGET_CHUNK", c.RedisMGetChunk, 256},
		{"STREAM_BATCH_SIZE", c.StreamBatchSize, 128},
		{"MIN_RESYNC_INTERVAL_SEC", c.MinResyncInterval, 90 * time.Second},
		{"MAX_WALK_SECONDS", c.MaxWalkSeconds, 900},
		{"MAX_FOOTPATHS_PER_STATION", c.MaxFootpathsPerStation, 32},
		{"SEAT_BREAKER_THRESHOLD", c.BreakerThreshold, 9},
		{"SEAT_BREAKER_COOLDOWN_SEC", c.BreakerCooldown, 45 * time.Second},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s not applied: got %v, want %v", ch.name, ch.got, ch.want)
		}
	}
}

func TestValidate_RejectsBadSettings(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"rounds too high", func(c *Config) { c.MaxRounds = 99 }, "MAX_ROUNDS"},
		{"rounds too low", func(c *Config) { c.MaxRounds = 0 }, "MAX_ROUNDS"},
		{"negative transfer time", func(c *Config) { c.MinTransferSeconds = -1 }, "MIN_TRANSFER_SECONDS"},
		{"no database", func(c *Config) { c.PgDSN = "" }, "PG_DSN"},
		{"no workers", func(c *Config) { c.AsynqConcurrency = 0 }, "ASYNQ_CONCURRENCY"},
		{"unknown provider", func(c *Config) { c.SeatProvider = "carrier-pigeon" }, "SEAT_PROVIDER"},
		{"http provider without a URL", func(c *Config) { c.SeatProvider = "http"; c.SeatAPIBaseURL = "" }, "SEAT_API_BASE_URL"},
		{"unknown log level", func(c *Config) { c.LogLevel = "chatty" }, "LOG_LEVEL"},
		{"fewer candidates than results", func(c *Config) { c.SearchCandidates = 1; c.SearchResults = 5 }, "SEARCH_CANDIDATES"},
		{"mget chunk of zero", func(c *Config) { c.RedisMGetChunk = 0 }, "REDIS_MGET_CHUNK"},
		{"absurd mget chunk", func(c *Config) { c.RedisMGetChunk = 100000 }, "REDIS_MGET_CHUNK"},
		{"stream batch of zero", func(c *Config) { c.StreamBatchSize = 0 }, "STREAM_BATCH_SIZE"},
		{"walk cap beyond two hours", func(c *Config) { c.MaxWalkSeconds = 99999 }, "MAX_WALK_SECONDS"},
		{"no footpaths per station", func(c *Config) { c.MaxFootpathsPerStation = 0 }, "MAX_FOOTPATHS_PER_STATION"},
		{"breaker that cannot trip", func(c *Config) { c.BreakerThreshold = 0 }, "SEAT_BREAKER_THRESHOLD"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(c)
			err = c.Validate()
			if err == nil {
				t.Fatalf("want an error mentioning %s", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want an error mentioning %s, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestValidate_AcceptsHTTPProviderWithURL(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	c.SeatProvider = "http"
	c.SeatAPIBaseURL = "https://seats.example.com/availability"
	if err := c.Validate(); err != nil {
		t.Fatalf("a fully configured http provider should validate: %v", err)
	}
}

func TestEnvHelpers_FallBackOnGarbage(t *testing.T) {
	t.Setenv("ASYNQ_CONCURRENCY", "twenty")
	t.Setenv("STRICT_SEAT_MODE", "yes-please")
	t.Setenv("WATCHER_INTERVAL_SEC", "-5")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.AsynqConcurrency != 20 {
		t.Errorf("a non-numeric value should fall back to the default, got %d", c.AsynqConcurrency)
	}
	if c.StrictSeatMode {
		t.Error("an unparseable boolean should fall back to the default")
	}
	if c.WatcherInterval != 30*time.Second {
		t.Errorf("a negative duration should fall back to the default, got %v", c.WatcherInterval)
	}
}

// Config is logged at startup, so the password must not be in it.
func TestRedacted_HidesSecrets(t *testing.T) {
	c := Config{
		PgDSN:         "postgres://axentra_user:s3cr3t@db:5432/axentra?sslmode=disable",
		RedisPassword: "hunter2",
		SeatAPIKey:    "sk-live-abc",
	}
	got := c.Redacted()

	for _, secret := range []string{"s3cr3t", "hunter2", "sk-live-abc"} {
		if strings.Contains(got.PgDSN+got.RedisPassword+got.SeatAPIKey, secret) {
			t.Errorf("secret %q survived redaction: %+v", secret, got)
		}
	}
	if !strings.Contains(got.PgDSN, "axentra_user") || !strings.Contains(got.PgDSN, "db:5432") {
		t.Errorf("redaction should keep the useful parts of the DSN: %s", got.PgDSN)
	}
}

func TestRedactDSN_LeavesUnusualInputAlone(t *testing.T) {
	for _, in := range []string{"", "not a dsn", "postgres://nouser@host/db"} {
		if got := redactDSN(in); got != in {
			t.Errorf("redactDSN(%q) = %q, want it unchanged", in, got)
		}
	}
}
