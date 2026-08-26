package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"

	"github.com/redis/go-redis/v9"
)

// Defaults for SyncerConfig.
const (
	DefaultMGetChunk         = 512
	DefaultStreamBatch       = 500
	DefaultStreamBlock       = 2 * time.Second
	DefaultColdStartAttempts = 5

	// DefaultMinResyncInterval is the floor between full re-synchronisations.
	//
	// A cold start scans every known trip. Stream lag — the condition that
	// triggers one — is exactly what a struggling Redis produces, so an
	// unthrottled resync path answers "Redis is unhealthy" with "read
	// everything from Redis", repeatedly, for as long as the lag persists.
	// That is a self-amplifying loop, and this interval is what breaks it.
	DefaultMinResyncInterval = 30 * time.Second
)

// SyncerConfig tunes how seat data is pulled from Redis.
type SyncerConfig struct {
	// MGetChunk bounds keys per MGET. Interacts with cluster limits, so it is
	// operator-tunable rather than compiled in.
	MGetChunk int
	// StreamBatch bounds entries consumed per XREAD.
	StreamBatch int
	// StreamBlock is how long XREAD parks waiting for new entries.
	StreamBlock time.Duration
	// ColdStartAttempts is how many times boot retries Redis before opening
	// the service with no seat data.
	ColdStartAttempts int
	// MinResyncInterval is the floor between lag-triggered cold starts.
	MinResyncInterval time.Duration
}

func (c SyncerConfig) withDefaults() SyncerConfig {
	if c.MGetChunk <= 0 {
		c.MGetChunk = DefaultMGetChunk
	}
	if c.StreamBatch <= 0 {
		c.StreamBatch = DefaultStreamBatch
	}
	if c.StreamBlock <= 0 {
		c.StreamBlock = DefaultStreamBlock
	}
	if c.ColdStartAttempts <= 0 {
		c.ColdStartAttempts = DefaultColdStartAttempts
	}
	if c.MinResyncInterval <= 0 {
		c.MinResyncInterval = DefaultMinResyncInterval
	}
	return c
}

// Syncer keeps the in-memory seat signal buffer in step with Redis.
//
// It owns the stream cursor and the resync throttle rather than keeping them in
// package globals, so a restarted loop cannot race a running one and tests can
// run independent instances side by side.
type Syncer struct {
	rdb *redis.Client
	cfg SyncerConfig

	mu         sync.Mutex
	cursor     string
	lastResync time.Time
}

// NewSyncer builds a Syncer with the given configuration.
func NewSyncer(rdb *redis.Client, cfg SyncerConfig) *Syncer {
	return &Syncer{rdb: rdb, cfg: cfg.withDefaults(), cursor: "0"}
}

// Cursor returns the current dirty-stream read position.
func (s *Syncer) Cursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

func (s *Syncer) setCursor(id string) {
	s.mu.Lock()
	s.cursor = id
	s.mu.Unlock()
}

// ColdStart snapshots seat availability for every known trip from Redis into
// the in-memory signal buffer and publishes it.
//
// Ordering requirements:
//   - MUST run after ReloadRouteArrays: TripIndex is empty until then.
//   - MUST run before RefreshLoop: the loop needs a cursor, or it replays the
//     entire dirty stream from ID "0".
//
// Seat data is fetched in chunked MGETs — two pipelined GETs per trip is an
// N+1 that costs thousands of round trips on a month of seeded schedule.
func (s *Syncer) ColdStart(ctx context.Context) error {
	// Capture the stream cursor BEFORE the snapshot, so writes that land during
	// the snapshot are replayed rather than lost.
	entries, err := s.rdb.XRevRangeN(ctx, model.DirtyStreamKey, "+", "-", 1).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("read dirty stream head: %w", err)
	}
	if len(entries) > 0 {
		s.setCursor(entries[0].ID)
	}

	routes := schedule.LiveRoutes()
	keys := make([]model.TripKey, 0, len(routes.TripIndex))
	for key := range routes.TripIndex {
		keys = append(keys, key)
	}

	staging, err := s.fetchSignals(ctx, routes, keys)
	if err != nil {
		return fmt.Errorf("snapshot seat signals: %w", err)
	}

	state.SwapSignal(staging)
	s.markResynced()

	if len(staging) == 0 && len(routes.TripIndex) > 0 {
		slog.Warn("cold start loaded no seat signals; every search result will be "+
			"rejected by seat validation until Redis is populated "+
			"(run with -seed, or let the pollers fill it)",
			"known_trips", len(routes.TripIndex))
	}
	slog.Info("seat cold start complete",
		"loaded", len(staging), "known_trips", len(routes.TripIndex),
		"stale", state.StaleSignalCount(), "cursor", s.Cursor())
	return nil
}

// ColdStartWithRetry runs ColdStart with bounded exponential backoff.
//
// Readiness must not hinge on a single Redis call succeeding at the instant of
// boot: a transient error here previously skipped MarkReady entirely and the
// service answered 503 forever, logging it as expected.
func (s *Syncer) ColdStartWithRetry(ctx context.Context) error {
	var lastErr error
	delay := 500 * time.Millisecond
	for attempt := 1; attempt <= s.cfg.ColdStartAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.ColdStart(ctx); err == nil {
			return nil
		} else {
			lastErr = err
			slog.Warn("seat cold start attempt failed",
				"attempt", attempt, "of", s.cfg.ColdStartAttempts, "retry_in", delay, "error", err)
		}
		if attempt < s.cfg.ColdStartAttempts {
			sleepCtx(ctx, delay)
			delay *= 2
		}
	}
	return lastErr
}

// markResynced records that a full snapshot just completed.
func (s *Syncer) markResynced() {
	s.mu.Lock()
	s.lastResync = time.Now()
	s.mu.Unlock()
}

// resyncWait returns how long to wait before another full resync is permitted.
func (s *Syncer) resyncWait() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastResync.IsZero() {
		return 0
	}
	if since := time.Since(s.lastResync); since < s.cfg.MinResyncInterval {
		return s.cfg.MinResyncInterval - since
	}
	return 0
}

// RefreshLoop tails the dirty stream and applies seat deltas to the in-memory
// buffer using clone-then-swap.
//
// Never mutate the live map: searches read it concurrently without a lock.
// Build a new map, apply the deltas, publish with an atomic swap.
func (s *Syncer) RefreshLoop(ctx context.Context) {
	slog.Info("seat refresh loop started", "cursor", s.Cursor())
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			slog.Info("seat refresh loop stopping")
			return
		}

		result, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{model.DirtyStreamKey, s.Cursor()},
			Count:   int64(s.cfg.StreamBatch),
			Block:   s.cfg.StreamBlock,
		}).Result()

		if err == redis.Nil {
			continue // block expired with nothing new
		}
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("seat refresh loop stopping")
				return
			}
			slog.Error("dirty stream read failed", "error", err, "retry_in", backoff)
			sleepCtx(ctx, backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		if len(result) == 0 || len(result[0].Messages) == 0 {
			continue
		}
		msgs := result[0].Messages

		// Detect a trimmed stream: if the cursor predates the oldest surviving
		// entry, deltas were dropped and the buffer must be resynchronised.
		if s.lagged(ctx) {
			s.resync(ctx)
			continue
		}

		// Collect the distinct trips touched by this batch, so N stream entries
		// cost one chunked MGET instead of N pipelined round trips.
		seen := make(map[model.TripKey]struct{}, len(msgs))
		dirty := make([]model.TripKey, 0, len(msgs))
		lastID := s.Cursor()
		for _, msg := range msgs {
			lastID = msg.ID
			raw, ok := msg.Values["trip"].(string)
			if !ok {
				continue
			}
			key, ok := model.ParseSeatTripDate(raw)
			if !ok {
				slog.Warn("unparseable dirty stream entry", "id", msg.ID, "trip", raw)
				continue
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			dirty = append(dirty, key)
		}

		if len(dirty) == 0 {
			s.setCursor(lastID)
			continue
		}

		routes := schedule.LiveRoutes()
		updates, err := s.fetchSignals(ctx, routes, dirty)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to fetch seat deltas", "error", err, "trips", len(dirty))
			sleepCtx(ctx, backoff)
			continue
		}

		live := *state.LiveSignal()
		staging := make(state.SignalBuffer, len(live)+len(updates))
		for k, v := range live {
			staging[k] = v
		}
		for k, v := range updates {
			staging[k] = v
		}
		state.SwapSignal(staging)
		s.setCursor(lastID)

		slog.Debug("seat signals refreshed",
			"entries", len(msgs), "trips", len(dirty), "applied", len(updates), "cursor", lastID)
	}
}

// resync performs a throttled full re-synchronisation after stream lag.
//
// The throttle is the point. Lag means Redis is behind or the stream was
// trimmed, and a cold start reads every known trip — so resyncing on every
// lagging read would pile a full scan onto an already-struggling Redis, over
// and over, for as long as the condition lasts.
func (s *Syncer) resync(ctx context.Context) {
	if wait := s.resyncWait(); wait > 0 {
		slog.Warn("dirty stream lag detected, delaying resync to avoid piling onto redis",
			"wait", wait, "min_interval", s.cfg.MinResyncInterval)
		sleepCtx(ctx, wait)
		if ctx.Err() != nil {
			return
		}
	}
	slog.Warn("dirty stream lag detected, re-running cold start", "cursor", s.Cursor())
	if err := s.ColdStart(ctx); err != nil && ctx.Err() == nil {
		slog.Error("cold start after stream lag failed", "error", err)
		// Still count the attempt, so a permanently failing Redis is retried on
		// the throttle rather than on every loop iteration.
		s.markResynced()
	}
}

// lagged reports whether the cursor has fallen behind the oldest entry still
// in the stream, meaning entries were trimmed before they could be consumed.
func (s *Syncer) lagged(ctx context.Context) bool {
	oldest, err := s.rdb.XRangeN(ctx, model.DirtyStreamKey, "-", "+", 1).Result()
	if err != nil || len(oldest) == 0 {
		return false
	}
	cur := s.Cursor()
	return cur != "0" && compareStreamIDs(cur, oldest[0].ID) < 0
}

// fetchSignals reads seat maps and timestamps for the given trips using
// chunked MGETs — two commands per chunk, not two per trip.
func (s *Syncer) fetchSignals(ctx context.Context, routes *schedule.RouteBuffer,
	trips []model.TripKey) (state.SignalBuffer, error) {

	out := make(state.SignalBuffer, len(trips))
	if len(trips) == 0 {
		return out, nil
	}
	now := float64(time.Now().Unix())

	for start := 0; start < len(trips); start += s.cfg.MGetChunk {
		end := start + s.cfg.MGetChunk
		if end > len(trips) {
			end = len(trips)
		}
		chunk := trips[start:end]

		mapKeys := make([]string, len(chunk))
		tsKeys := make([]string, len(chunk))
		for i, k := range chunk {
			mapKeys[i] = model.SeatMapKey(k.TripID, k.Date)
			tsKeys[i] = model.SeatTSKey(k.TripID, k.Date)
		}

		pipe := s.rdb.Pipeline()
		mapCmd := pipe.MGet(ctx, mapKeys...)
		tsCmd := pipe.MGet(ctx, tsKeys...)
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return out, fmt.Errorf("mget seat chunk: %w", err)
		}
		mapVals, tsVals := mapCmd.Val(), tsCmd.Val()

		for i, key := range chunk {
			if i >= len(mapVals) || mapVals[i] == nil {
				continue // no seat data for this trip yet
			}
			raw, ok := mapVals[i].(string)
			if !ok || raw == "" {
				continue
			}
			var byClass map[string]int
			if err := json.Unmarshal([]byte(raw), &byClass); err != nil {
				slog.Warn("unparseable seat map", "trip", key.TripID, "date", key.Date, "error", err)
				continue
			}

			var ts float64
			if i < len(tsVals) && tsVals[i] != nil {
				if str, ok := tsVals[i].(string); ok {
					ts, _ = strconv.ParseFloat(str, 64)
				}
			}

			total := 0
			for _, v := range byClass {
				total += v
			}

			// A signal is stale once it is older than two polling intervals for
			// its urgency zone — the search then defers to the strict validator.
			stale := false
			if dep := routes.TripDeparture(key); dep > 0 && ts > 0 {
				stale = (now - ts) > 2*ClassifyZone(dep).Interval.Seconds()
			} else if ts == 0 {
				stale = true
			}

			out[key] = model.SeatSignal{
				ByClass:    byClass,
				Total:      total,
				Stale:      stale,
				SnapshotTs: ts,
			}
		}
	}
	return out, nil
}

// compareStreamIDs orders two Redis stream IDs ("<ms>-<seq>").
// Returns -1 if a < b, 0 if equal, 1 if a > b.
func compareStreamIDs(a, b string) int {
	aMs, aSeq := splitStreamID(a)
	bMs, bSeq := splitStreamID(b)
	switch {
	case aMs < bMs:
		return -1
	case aMs > bMs:
		return 1
	case aSeq < bSeq:
		return -1
	case aSeq > bSeq:
		return 1
	default:
		return 0
	}
}

func splitStreamID(id string) (ms, seq int64) {
	parts := strings.SplitN(id, "-", 2)
	ms, _ = strconv.ParseInt(parts[0], 10, 64)
	if len(parts) > 1 {
		seq, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return ms, seq
}

// sleepCtx sleeps for d, returning early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
