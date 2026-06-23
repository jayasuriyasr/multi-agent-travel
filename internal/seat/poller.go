package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// HandlePollTask returns an Asynq handler closure for "seat:poll" tasks.
//
// The closure captures the Redis client used for both the distributed lock
// (SetNX) and the atomic Lua write-through gate (luaGate).
//
// Execution pipeline per task:
//  1. Decode JSON payload  → TripID, Date
//  2. RAM lookup           → departure unix (no DB I/O)
//  3. Zone classification  → determines lock TTL and poll cadence
//  4. Distributed lock     → SetNX(lockKey, zone.Interval); skip if held (G5)
//  5. Provider fetch       → FetchSeats (mock: 50–200 ms simulated delay)
//  6. Lua gate             → atomic hash-compare → conditional stream write (G6)
//
// Distributed Lock (G5):
//   - SetNX TTL = zone.Interval so the lock window matches the poll cadence.
//   - If SetNX returns false, another worker is already polling — return nil.
//   - No defer rdb.Del(lockKey): the TTL expiry IS the release mechanism.
func HandlePollTask(ctx context.Context, rdb *redis.Client) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, t *asynq.Task) error {
		// ── 1. Decode payload ──────────────────────────────────────────────────
		var p struct {
			TripID string `json:"trip_id"`
			Date   string `json:"date"`
		}
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		// ── 2. Look up departure from RAM buffer (zero DB I/O) ─────────────────
		depUnix := getTripDeparture(p.TripID, p.Date)
		if depUnix == 0 {
			log.Printf("[seat] poll: trip %s/%s not in schedule buffer, skipping", p.TripID, p.Date)
			return nil // trip removed from schedule — discard gracefully
		}

		// ── 3. Zone classification (determines lock TTL) ───────────────────────
		zone := ClassifyZone(depUnix)
		lockKey := fmt.Sprintf("poll_lock:%s:%s", p.TripID, p.Date)

		// ── 4. Distributed lock via SetNX (G5) ────────────────────────────────
		// TTL = zone.Interval: lock expires exactly when the next poll window
		// opens, preventing worker overlap at concurrency=20.
		ok, err := rdb.SetNX(ctx, lockKey, "1", zone.Interval).Result()
		if err != nil {
			return fmt.Errorf("setnx lock %q: %w", lockKey, err)
		}
		if !ok {
			// Another worker already holds the lock — skip silently.
			// G5: Do NOT call rdb.Del(lockKey) — let TTL expire naturally.
			return nil
		}
		// G5: No defer rdb.Del — TTL is the intentional release mechanism.

		// ── 5. Fetch seat data from provider ──────────────────────────────────
		seats, err := FetchSeats(ctx, p.TripID, p.Date)
		if err != nil {
			// Provider error is non-fatal: return error so Asynq can retry
			// according to its backoff policy.
			return fmt.Errorf("provider fetch for %s/%s: %w", p.TripID, p.Date, err)
		}

		// ── 6. Atomic Lua hash gate (G6) ──────────────────────────────────────
		// Returns 1 if seat data changed (stream updated), 0 if identical (skipped).
		// The gate is atomic: hash-compare + map write + stream append in one script.
		changed, err := luaGate(ctx, rdb, p.TripID, p.Date, seats)
		if err != nil {
			return fmt.Errorf("lua gate for %s/%s: %w", p.TripID, p.Date, err)
		}

		if changed == 1 {
			log.Printf("[seat] poll: %s/%s — seats CHANGED, dirty stream updated (zone=%s)",
				p.TripID, p.Date, zone.Name)
		} else {
			log.Printf("[seat] poll: %s/%s — seats UNCHANGED, stream write skipped (zone=%s)",
				p.TripID, p.Date, zone.Name)
		}

		return nil
	}
}
