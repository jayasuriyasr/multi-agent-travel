// Package main is the Axentra entrypoint.
// Day 2: connectivity prover (Postgres + Redis ping).
// Day 3: adds -seed flag to populate the DB with mock schedule data.
// Day 4: initial synchronous route array load + background watcher goroutine.
// Day 5: Asynq client + server wired; ZoneClassifyLoop spawned as goroutine.
// Day 7: ColdStart + RefreshLoop stream listener wired after route load.
// Day 8: Lock-free SignalBuffer double-buffer confirmed; ReadyGate middleware verified.
// Day 9: Core RAPTOR search engine implemented.
// Day 10: Fiber API & Web Demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"axentra/internal/config"
	"axentra/internal/schedule"
	"axentra/internal/seat"
	"axentra/internal/api"

	"github.com/hibiken/asynq"
	"github.com/gofiber/fiber/v2"
)

func main() {
	// ── Parse CLI flags FIRST — before any I/O or pool creation ────────────────
	seedFlag := flag.Bool("seed", false, "Seed the database with mock schedule data and exit")
	flag.Parse()

	ctx := context.Background()

	// ── Read connection strings from env with Day 1 compose fallbacks ──────────
	pgConnString := os.Getenv("PG_DSN")
	if pgConnString == "" {
		pgConnString = "postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	// ── Initialize Postgres pool (fast-fail on unreachable) ───────────────────
	pool := config.InitPostgres(ctx, pgConnString)
	defer pool.Close()

	// ── Phase 1: Initial synchronous route array load ─────────────────────────
	// MUST run before anything else touches the schedule buffer.
	// Boot order guarantee: RAM is warm before any request handler or background
	// goroutine tries to read TripIndex or StopTimes.
	log.Println("[main] loading route arrays into memory...")
	if err := schedule.ReloadRouteArrays(ctx, pool); err != nil {
		log.Fatalf("[main] initial route load failed: %v", err)
	}
	buf := schedule.LiveRoutes()
	log.Printf("[main] ✅ route arrays loaded — %d routes, %d trips in RAM",
		len(buf.Routes), len(buf.TripIndex))

	// ── Phase 1b: Background schema watcher ───────────────────────────────────
	// Polls schema_version every 2 minutes and hot-swaps the buffer on change.
	// Spawned AFTER the initial load so the watcher never races with boot.
	go schedule.WatcherLoop(ctx, pool)

	// ── Seed mode: populate DB and exit cleanly ────────────────────────────────
	if *seedFlag {
		schedule.SeedDatabase(ctx, pool)
		log.Println("[main] seeding complete — exiting")
		os.Exit(0)
	}

	// ── Initialize Redis client (fast-fail on unreachable) ────────────────────
	rdb := config.InitRedis(ctx, redisURL)
	defer rdb.Close()

	// ── Phase 2: Seat signal cold-start ───────────────────────────────────────
	// Reads the latest seat:map and seat:ts for every trip currently in the
	// route buffer from Redis, populates the in-memory SignalBuffer, and sets
	// the global READY flag.
	//
	// G8: MUST run after ReloadRouteArrays — TripIndex is empty until then.
	// G8: MUST run before RefreshLoop — the loop needs a valid cursor to avoid
	//     replaying the entire dirty stream from ID "0".
	log.Println("[main] running seat cold-start...")
	if err := seat.ColdStart(ctx, rdb); err != nil {
		// Non-fatal on first boot when no seat data exists yet.
		log.Printf("[main] cold-start warning (expected on first boot): %v", err)
	}

	// ── Phase 2b: Stream listener ─────────────────────────────────────────────
	// Tails seat:dirty_stream with a 2-second blocking XRead. For each message,
	// it fetches the updated seat data via a pipelined GET (no N+1), checks
	// staleness using ClassifyZone, and applies a clone-then-swap to the signal
	// buffer (G9). Falls back to ColdStart if stream lag is detected (G10).
	go seat.RefreshLoop(ctx, rdb)
	log.Println("[main] seat: stream listener goroutine started")

	// ── Phase 3: Asynq engine ─────────────────────────────────────────────────
	// asynq expects a RedisClientOpt (addr:port), not a full redis:// URL.
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisOpt := asynq.RedisClientOpt{Addr: redisAddr}

	asynqClient := asynq.NewClient(redisOpt)
	defer asynqClient.Close()
	log.Println("[main] asynq: client ready")

	asynqServer := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 20,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
			log.Printf("[asynq] error processing task %q: %v", task.Type(), err)
		}),
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc("seat:poll", seat.HandlePollTask(ctx, rdb))

	go func() {
		if err := asynqServer.Run(mux); err != nil {
			log.Fatalf("[main] asynq server exited: %v", err)
		}
	}()
	log.Println("[main] asynq: server running (concurrency=20)")

	// ── Phase 3b: Zone Classify Loop ──────────────────────────────────────────
	// Reads the RAM route buffer every 5 minutes; zero DB I/O.
	// Spawned AFTER RefreshLoop so signal buffer is already warm.
	go seat.ZoneClassifyLoop(ctx, asynqClient)
	log.Println("[main] seat: zone classifier goroutine started")

	// ── Phase 4: API & Web Server ─────────────────────────────────────────────
	app := fiber.New()
	
	// Serve static web files
	app.Static("/", "./web")

	// Register API endpoints (includes ReadyGate)
	api.RegisterRoutes(app, rdb)

	// ── All systems nominal ───────────────────────────────────────────────────
	fmt.Println("✅ Axentra Day 10 Ready — Fiber API and Web Demo live on :8080")

	// Start web server (blocks forever)
	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("[main] fiber server exited: %v", err)
	}
}
