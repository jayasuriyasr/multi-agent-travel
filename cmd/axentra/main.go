// Command axentra is the Axentra transit routing service.
//
// Boot order is not arbitrary — each phase depends on the one before it:
//
//  1. config      — validated up front so a bad setting fails now, not later
//  2. migrations  — the schema exists before anything queries it
//  3. schedule    — route arrays in RAM before any goroutine or handler reads them
//  4. seat        — cold start needs TripIndex; the refresh loop needs a cursor
//  5. workers     — pollers need the schedule in RAM to classify trips
//  6. HTTP        — traffic is admitted last, gated on readiness
//
// Shutdown runs in reverse: stop accepting traffic, drain in-flight requests,
// stop the workers, then close the pools.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"axentra/internal/api"
	"axentra/internal/config"
	"axentra/internal/migrate"
	"axentra/internal/schedule"
	"axentra/internal/seat"
	"axentra/internal/state"

	"github.com/hibiken/asynq"
)

// shutdownGrace bounds how long shutdown waits for in-flight work.
const shutdownGrace = 20 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	seedFlag := flag.Bool("seed", false, "seed the database with mock schedule data, then exit")
	migrateFlag := flag.Bool("migrate", false, "apply database migrations, then exit")
	flag.Parse()

	// ── 1. Configuration ─────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	cfg.SetupLogging()
	slog.Info("axentra starting",
		"log_level", cfg.LogLevel, "port", cfg.HTTPPort,
		"seat_provider", cfg.SeatProvider, "strict_seat_mode", cfg.StrictSeatMode,
		"pg", cfg.Redacted().PgDSN, "redis", cfg.RedisAddr)

	// Signal-driven root context. Everything below derives from it, so one
	// Ctrl-C or one SIGTERM unwinds every goroutine in the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := config.InitPostgres(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// ── 2. Migrations ────────────────────────────────────────────────────────
	if *migrateFlag || cfg.AutoMigrate {
		n, err := migrate.Up(ctx, pool, cfg.MigrationsDir)
		if err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
		if *migrateFlag {
			slog.Info("migrations complete, exiting", "applied", n)
			return nil
		}
	}

	rdb, err := config.InitRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	// ── Seed mode ────────────────────────────────────────────────────────────
	// Must run before the watcher starts: seeding truncates every table, and a
	// watcher reloading mid-truncation would publish an empty schedule.
	if *seedFlag {
		if err := schedule.SeedDatabase(ctx, pool, rdb); err != nil {
			return fmt.Errorf("seed: %w", err)
		}
		slog.Info("seeding complete, exiting")
		return nil
	}

	// ── 3. Schedule into RAM ─────────────────────────────────────────────────
	loadOpts := schedule.LoadOptions{
		MaxWalkSeconds:         cfg.MaxWalkSeconds,
		MaxFootpathsPerStation: cfg.MaxFootpathsPerStation,
	}
	slog.Info("loading schedule into memory",
		"max_walk_seconds", loadOpts.MaxWalkSeconds,
		"max_footpaths_per_station", loadOpts.MaxFootpathsPerStation)
	if err := schedule.ReloadRouteArrays(ctx, pool, loadOpts); err != nil {
		return fmt.Errorf("initial schedule load: %w", err)
	}
	st := schedule.LiveRoutes().Stats()
	if st.Trips == 0 {
		slog.Warn("schedule is empty — run with -seed or ingest a timetable, " +
			"or every search will correctly return no routes")
	}

	var wg sync.WaitGroup
	goroutine := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
			slog.Debug("goroutine stopped", "name", name)
		}()
	}

	goroutine("schedule-watcher", func() { schedule.WatcherLoop(ctx, pool, cfg.WatcherInterval, loadOpts) })

	// ── 4. Seat signals ──────────────────────────────────────────────────────
	// Readiness must not hang on one Redis call at the instant of boot, so cold
	// start retries with backoff — and the service opens either way. An empty
	// seat buffer is a degraded state reported on /healthz/ready, not a reason
	// to serve 503 forever.
	syncer := seat.NewSyncer(rdb, seat.SyncerConfig{
		MGetChunk:         cfg.RedisMGetChunk,
		StreamBatch:       cfg.StreamBatchSize,
		MinResyncInterval: cfg.MinResyncInterval,
	})
	if err := syncer.ColdStartWithRetry(ctx); err != nil && ctx.Err() == nil {
		slog.Error("seat cold start did not succeed; starting with no seat data", "error", err)
	}
	goroutine("seat-refresh", func() { syncer.RefreshLoop(ctx) })

	// ── 5. Workers ───────────────────────────────────────────────────────────
	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}

	asynqClient := asynq.NewClient(redisOpt)
	defer func() { _ = asynqClient.Close() }()

	provider, err := buildProvider(cfg)
	if err != nil {
		return err
	}
	slog.Info("seat provider ready", "provider", provider.Name())

	asynqServer := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.AsynqConcurrency,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
			slog.Error("task failed", "type", task.Type(), "error", err)
		}),
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(seat.TaskSeatPoll, seat.HandlePollTask(rdb, provider))

	workerErr := make(chan error, 1)
	go func() {
		if err := asynqServer.Run(mux); err != nil {
			workerErr <- fmt.Errorf("asynq server: %w", err)
		}
	}()
	slog.Info("asynq workers running", "concurrency", cfg.AsynqConcurrency)

	goroutine("zone-classifier", func() { seat.ZoneClassifyLoop(ctx, asynqClient, cfg.ClassifyInterval) })

	// ── 6. HTTP ──────────────────────────────────────────────────────────────
	state.MarkReady()

	app := api.NewServer(cfg, rdb).BuildApp()
	serverErr := make(chan error, 1)
	go func() {
		if err := app.Listen(":" + cfg.HTTPPort); err != nil {
			serverErr <- fmt.Errorf("http server: %w", err)
		}
	}()
	slog.Info("axentra ready",
		"addr", ":"+cfg.HTTPPort, "routes", st.Routes, "trips", st.Trips,
		"seat_signals", state.SignalCount())

	// ── Wait for a stop signal or a fatal component failure ──────────────────
	var exitErr error
	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	case err := <-serverErr:
		exitErr = err
	case err := <-workerErr:
		exitErr = err
	}

	// ── Shutdown, in reverse order of startup ────────────────────────────────
	// Fail readiness first so a load balancer drains this instance before the
	// listener disappears underneath it.
	state.MarkNotReady()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("http shutdown did not complete cleanly", "error", err)
	} else {
		slog.Info("http server drained")
	}

	asynqServer.Shutdown()
	slog.Info("asynq workers stopped")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		slog.Info("background goroutines stopped")
	case <-shutdownCtx.Done():
		slog.Warn("background goroutines did not stop within the grace period")
	}

	slog.Info("axentra stopped")
	return exitErr
}

// buildProvider selects the seat data source named by SEAT_PROVIDER.
func buildProvider(cfg *config.Config) (seat.Provider, error) {
	switch cfg.SeatProvider {
	case "http":
		return seat.NewHTTPProvider(cfg.SeatAPIBaseURL, cfg.SeatAPIKey, cfg.SeatAPITimeout), nil
	case "mock":
		return seat.NewMockProvider(time.Now().UnixNano()), nil
	default:
		return nil, fmt.Errorf("unknown SEAT_PROVIDER %q", cfg.SeatProvider)
	}
}
