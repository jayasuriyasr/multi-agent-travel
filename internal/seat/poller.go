package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"axentra/internal/schedule"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// pollPayload is the asynq task payload for a seat poll.
type pollPayload struct {
	TripID string `json:"trip_id"`
	Date   string `json:"date"`
}

// HandlePollTask returns the asynq handler for TaskSeatPoll.
//
// Pipeline per task:
//  1. decode payload → trip ID and date
//  2. RAM lookup     → departure time (no database I/O)
//  3. zone classify  → determines the lock TTL
//  4. distributed lock via SetNX; skip if another worker holds it
//  5. provider fetch → live seat availability
//  6. Lua gate       → atomic hash compare, conditional write, stream append
//
// The lock TTL equals the zone's poll interval, so the lock window matches the
// poll cadence and expiry IS the release. There is deliberately no Del on the
// happy path: deleting the key would let a second worker poll the same trip
// immediately, defeating the rate limiting the lock exists to provide.
func HandlePollTask(rdb *redis.Client, provider Provider) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p pollPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			// Malformed payloads never succeed on retry.
			return fmt.Errorf("unmarshal payload: %w: %w", err, asynq.SkipRetry)
		}
		if p.TripID == "" || p.Date == "" {
			return fmt.Errorf("payload missing trip_id or date: %w", asynq.SkipRetry)
		}

		depUnix := schedule.GetTripDeparture(p.TripID, p.Date)
		if depUnix == 0 {
			// The trip left the schedule between enqueue and execution.
			slog.Debug("poll skipped: trip not in schedule buffer", "trip", p.TripID, "date", p.Date)
			return nil
		}

		zone := ClassifyZone(depUnix)
		lockKey := fmt.Sprintf("poll_lock:%s:%s", p.TripID, p.Date)

		ok, err := rdb.SetNX(ctx, lockKey, "1", zone.Interval).Result()
		if err != nil {
			return fmt.Errorf("acquire poll lock %q: %w", lockKey, err)
		}
		if !ok {
			return nil // another worker owns this poll window
		}

		seats, err := provider.FetchSeats(ctx, p.TripID, p.Date)
		if err != nil {
			// Release the lock so the retry is not blocked by our own lease.
			rdb.Del(ctx, lockKey)
			return fmt.Errorf("provider %s fetch %s/%s: %w", provider.Name(), p.TripID, p.Date, err)
		}

		changed, err := luaGate(ctx, rdb, p.TripID, p.Date, seats)
		if err != nil {
			rdb.Del(ctx, lockKey)
			return fmt.Errorf("seat gate %s/%s: %w", p.TripID, p.Date, err)
		}

		slog.Debug("seat poll complete",
			"trip", p.TripID, "date", p.Date, "zone", zone.Name, "changed", changed == 1)
		return nil
	}
}
