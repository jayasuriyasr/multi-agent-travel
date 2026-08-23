package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"

	"github.com/redis/go-redis/v9"
)

// lastStreamID tracks our position in the dirty stream.
var lastStreamID = "0"

// ColdStart performs a full snapshot of all known trips' seat data from Redis
// into the in-memory signal buffer, then atomically swaps and sets READY.
//
// G8: ColdStart MUST run AFTER ReloadRouteArrays completes. routes.TripIndex
// is empty until the first route load finishes.
func ColdStart(ctx context.Context, rdb *redis.Client) error {
	// 1. Capture stream cursor BEFORE snapshot to avoid missing concurrent writes
	entries, err := rdb.XRevRangeN(ctx, "seat:dirty_stream", "+", "-", 1).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("xrevrange: %w", err)
	}
	if len(entries) > 0 {
		lastStreamID = entries[0].ID
	}

	// 2. Full snapshot for every known trip
	routes := schedule.LiveRoutes()
	staging := make(state.SignalBuffer, len(routes.TripIndex))

	for key := range routes.TripIndex {
		sig, err := fetchSignalFromRedis(ctx, rdb, key)
		if err != nil {
			// Missing seat data for a trip is expected on first boot
			log.Printf("[seat] cold start: no seat data for %s/%s: %v", key.TripID, key.Date, err)
			continue
		}
		staging[key] = sig
	}

	// 3. Atomic swap + mark ready
	state.SwapSignal(staging)
	state.MarkReady()

	log.Printf("[seat] cold start: loaded %d/%d trip signals, READY=true",
		len(staging), len(routes.TripIndex))
	return nil
}

// RefreshLoop continuously reads from the dirty stream and applies deltas
// to the in-memory signal buffer using the clone-then-swap pattern.
//
// G9: Never mutate the live map — clone it, apply deltas, then swap.
// G10: If lastStreamID predates trimmed entries, re-run ColdStart.
func RefreshLoop(ctx context.Context, rdb *redis.Client) {
	for {
		select {
		case <-ctx.Done():
			log.Println("[seat] refresh loop: context cancelled, stopping")
			return
		default:
		}

		result, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"seat:dirty_stream", lastStreamID},
			Count:   500,
			Block:   2 * time.Second,
		}).Result()

		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Printf("[seat] refresh loop: xread error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(result) == 0 || len(result[0].Messages) == 0 {
			continue
		}

		// G10: Check for stream lag — if our cursor is behind the oldest entry,
		// we've missed trimmed entries and must do a full re-sync.
		oldestEntries, lagErr := rdb.XRangeN(ctx, "seat:dirty_stream", "-", "+", 1).Result()
		if lagErr == nil && len(oldestEntries) > 0 {
			if compareStreamIDs(lastStreamID, oldestEntries[0].ID) < 0 &&
				lastStreamID != "0" {
				log.Println("[seat] refresh loop: stream lag detected, re-running ColdStart")
				if err := ColdStart(ctx, rdb); err != nil {
					log.Printf("[seat] refresh loop: cold start failed: %v", err)
				}
				continue
			}
		}

		// Clone the current live buffer into staging (G9)
		live := *state.LiveSignal()
		staging := make(state.SignalBuffer, len(live))
		for k, v := range live {
			staging[k] = v
		}

		// Apply deltas from stream messages
		updated := 0
		for _, msg := range result[0].Messages {
			tripDate, ok := msg.Values["trip"].(string)
			if !ok {
				lastStreamID = msg.ID
				continue
			}

			parts := strings.SplitN(tripDate, ":", 2)
			if len(parts) != 2 {
				lastStreamID = msg.ID
				continue
			}

			key := model.TripKey{TripID: parts[0], Date: parts[1]}
			sig, err := fetchSignalFromRedis(ctx, rdb, key)
			if err != nil {
				lastStreamID = msg.ID
				continue
			}

			staging[key] = sig
			lastStreamID = msg.ID
			updated++
		}

		// Atomic swap — new map, never mutate the live one
		state.SwapSignal(staging)
		log.Printf("[seat] refresh loop: updated %d signals, cursor=%s", updated, lastStreamID)
	}
}

// fetchSignalFromRedis retrieves seat data and timestamp for a trip using
// a pipelined GET to minimize round trips.
func fetchSignalFromRedis(ctx context.Context, rdb *redis.Client, key model.TripKey) (model.SeatSignal, error) {
	pipe := rdb.Pipeline()
	mapCmd := pipe.Get(ctx, fmt.Sprintf("seat:map:%s:%s", key.TripID, key.Date))
	tsCmd := pipe.Get(ctx, fmt.Sprintf("seat:ts:%s:%s", key.TripID, key.Date))
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return model.SeatSignal{}, fmt.Errorf("pipeline exec: %w", err)
	}

	mapVal := mapCmd.Val()
	if mapVal == "" {
		return model.SeatSignal{}, fmt.Errorf("no seat data")
	}

	var byClass map[string]int
	if err := json.Unmarshal([]byte(mapVal), &byClass); err != nil {
		return model.SeatSignal{}, fmt.Errorf("unmarshal seat map: %w", err)
	}

	ts, _ := strconv.ParseFloat(tsCmd.Val(), 64)

	// Determine staleness based on zone interval
	depUnix := getTripDeparture(key.TripID, key.Date)
	stale := false
	if depUnix > 0 {
		zone := ClassifyZone(depUnix)
		stale = (float64(time.Now().Unix()) - ts) > 2*zone.Interval.Seconds()
	}

	total := 0
	for _, v := range byClass {
		total += v
	}

	return model.SeatSignal{
		ByClass:    byClass,
		Total:      total,
		Stale:      stale,
		SnapshotTs: ts,
	}, nil
}

// compareStreamIDs compares two Redis stream IDs lexicographically.
// Returns -1 if a < b, 0 if equal, 1 if a > b.
func compareStreamIDs(a, b string) int {
	aParts := strings.SplitN(a, "-", 2)
	bParts := strings.SplitN(b, "-", 2)

	aMs, _ := strconv.ParseInt(aParts[0], 10, 64)
	bMs, _ := strconv.ParseInt(bParts[0], 10, 64)

	if aMs < bMs {
		return -1
	}
	if aMs > bMs {
		return 1
	}

	aSeq, bSeq := int64(0), int64(0)
	if len(aParts) > 1 {
		aSeq, _ = strconv.ParseInt(aParts[1], 10, 64)
	}
	if len(bParts) > 1 {
		bSeq, _ = strconv.ParseInt(bParts[1], 10, 64)
	}

	if aSeq < bSeq {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	return 0
}
