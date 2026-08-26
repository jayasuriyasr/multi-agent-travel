package seat

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const itDate = "2026-08-23"

// newSyncer builds a Syncer with fast test timings.
func newSyncer(rdb *redis.Client) *Syncer {
	return NewSyncer(rdb, SyncerConfig{
		StreamBlock:       200 * time.Millisecond,
		MinResyncInterval: 50 * time.Millisecond,
	})
}

// testRedis connects to REDIS_TEST_ADDR on a scratch database and flushes it.
// Skipped when unset, so `go test ./...` needs no infrastructure.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR (e.g. 127.0.0.1:6379) to run seat integration tests")
	}
	// DB 15 keeps these tests away from anything a developer is running locally.
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis at %s not reachable: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test db: %v", err)
	}
	t.Cleanup(func() {
		_ = rdb.FlushDB(context.Background())
		_ = rdb.Close()
	})
	return rdb
}

// publishTrips installs a schedule buffer holding the given trips, all
// departing soon so they classify as RED and their signals count as fresh.
func publishTrips(t *testing.T, tripIDs ...string) {
	t.Helper()
	buf := &schedule.RouteBuffer{
		TripIndex:    map[model.TripKey]model.TripLocation{},
		StopToRoutes: map[string][]schedule.RouteStop{},
		Footpaths:    map[string][]model.Footpath{},
	}
	dep := time.Now().Add(2 * time.Hour).Unix()
	trips := make([]model.TripStopTimes, 0, len(tripIDs))
	for i, id := range tripIDs {
		key := model.TripKey{TripID: id, Date: itDate}
		trips = append(trips, model.TripStopTimes{
			Key: key, StationIDs: []string{"A", "B"},
			Arrivals: []int64{dep, dep + 3600}, Departures: []int64{dep, dep + 3600},
		})
		buf.TripIndex[key] = model.TripLocation{RouteIdx: 0, TripIdx: i}
	}
	buf.Routes = append(buf.Routes, model.RouteEntry{RouteID: "R1", StopIDs: []string{"A", "B"}})
	buf.StopTimes = append(buf.StopTimes, trips)
	buf.RouteFIFO = append(buf.RouteFIFO, true)
	schedule.SwapRoutes(buf)
	state.SwapSignal(make(state.SignalBuffer))
}

func TestLuaGate_Integration(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	seats := map[string]int{"lower": 5, "upper": 3}

	changed, err := luaGate(ctx, rdb, "T1", itDate, seats)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("the first write must report a change, got %d", changed)
	}

	// Identical data must not fire the stream again. Without the canonical hash
	// this would be nondeterministic: Go randomises map order, so re-marshalling
	// the same map can produce a different byte string every time.
	for i := 0; i < 25; i++ {
		again, err := luaGate(ctx, rdb, "T1", itDate, map[string]int{"upper": 3, "lower": 5})
		if err != nil {
			t.Fatal(err)
		}
		if again != 0 {
			t.Fatalf("iteration %d: unchanged data reported a change", i)
		}
	}

	// The timestamp is refreshed even when nothing changed, so staleness
	// detection tracks "when did we last look", not "when did it last move".
	if ts, err := rdb.Get(ctx, model.SeatTSKey("T1", itDate)).Result(); err != nil || ts == "" {
		t.Errorf("timestamp should always be written: %q %v", ts, err)
	}

	changed, err = luaGate(ctx, rdb, "T1", itDate, map[string]int{"lower": 4, "upper": 3})
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatal("a real change must be reported")
	}

	raw, err := rdb.Get(ctx, model.SeatMapKey("T1", itDate)).Result()
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]int
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	if stored["lower"] != 4 {
		t.Errorf("stored seat map = %v, want lower=4", stored)
	}

	// Exactly two stream entries: one per real change.
	entries, err := rdb.XRange(ctx, model.DirtyStreamKey, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 dirty-stream entries (one per change), got %d", len(entries))
	}
	if got := entries[0].Values["trip"]; got != model.SeatTripDate("T1", itDate) {
		t.Errorf("stream entry trip = %v", got)
	}
}

func TestColdStart_Integration(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	publishTrips(t, "T1", "T2", "T3")

	// T3 deliberately has no seat data.
	for _, id := range []string{"T1", "T2"} {
		if _, err := luaGate(ctx, rdb, id, itDate, map[string]int{"lower": 7}); err != nil {
			t.Fatal(err)
		}
	}

	syncer := newSyncer(rdb)
	if err := syncer.ColdStart(ctx); err != nil {
		t.Fatalf("cold start: %v", err)
	}

	buf := *state.LiveSignal()
	if len(buf) != 2 {
		t.Fatalf("want 2 signals loaded (T3 has no data), got %d", len(buf))
	}
	sig := buf[model.TripKey{TripID: "T1", Date: itDate}]
	if sig.ByClass["lower"] != 7 || sig.Total != 7 {
		t.Errorf("signal = %+v, want lower=7 total=7", sig)
	}
	if sig.Stale {
		t.Error("a signal written moments ago must not be marked stale")
	}
	if !state.HasSeatData() {
		t.Error("HasSeatData should be true after a successful cold start")
	}

	// The cursor must be parked at the stream head, or the refresh loop replays
	// the entire backlog from "0" on every boot.
	if syncer.Cursor() == "0" {
		t.Error("cold start should capture the dirty-stream cursor")
	}
}

func TestColdStart_EmptyRedisIsNotAnError(t *testing.T) {
	rdb := testRedis(t)
	publishTrips(t, "T1")

	if err := newSyncer(rdb).ColdStart(context.Background()); err != nil {
		t.Fatalf("an empty seat store is a degraded state, not an error: %v", err)
	}
	if n := state.SignalCount(); n != 0 {
		t.Errorf("want 0 signals, got %d", n)
	}
}

func TestFetchSignals_ChunksAndMarksStale(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()

	// More trips than one MGET chunk, to exercise the chunking path.
	total := DefaultMGetChunk + 25
	ids := make([]string, total)
	for i := range ids {
		ids[i] = "BULK_" + itoa(i)
	}
	publishTrips(t, ids...)

	pipe := rdb.Pipeline()
	for _, id := range ids {
		pipe.Set(ctx, model.SeatMapKey(id, itDate), `{"lower":3}`, 0)
		pipe.Set(ctx, model.SeatTSKey(id, itDate), "1000.0", 0) // ancient
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	keys := make([]model.TripKey, 0, total)
	for _, id := range ids {
		keys = append(keys, model.TripKey{TripID: id, Date: itDate})
	}

	got, err := newSyncer(rdb).fetchSignals(ctx, schedule.LiveRoutes(), keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != total {
		t.Fatalf("want %d signals across chunks, got %d", total, len(got))
	}
	for _, sig := range got {
		if !sig.Stale {
			t.Fatal("a 1970 timestamp must be classified as stale")
		}
		break
	}
}

func TestRefreshLoop_AppliesDeltas(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	publishTrips(t, "T1", "T2")

	if _, err := luaGate(ctx, rdb, "T1", itDate, map[string]int{"lower": 9}); err != nil {
		t.Fatal(err)
	}
	syncer := newSyncer(rdb)
	if err := syncer.ColdStart(ctx); err != nil {
		t.Fatal(err)
	}
	if (*state.LiveSignal())[model.TripKey{TripID: "T1", Date: itDate}].ByClass["lower"] != 9 {
		t.Fatal("cold start did not load T1")
	}

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { syncer.RefreshLoop(loopCtx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	// Publish a change and a brand-new trip; both must reach memory.
	if _, err := luaGate(ctx, rdb, "T1", itDate, map[string]int{"lower": 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := luaGate(ctx, rdb, "T2", itDate, map[string]int{"lower": 4}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		buf := *state.LiveSignal()
		if buf[model.TripKey{TripID: "T1", Date: itDate}].ByClass["lower"] == 2 &&
			buf[model.TripKey{TripID: "T2", Date: itDate}].ByClass["lower"] == 4 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("deltas never reached the signal buffer: %+v", *state.LiveSignal())
}

func TestHandlePollTask_Integration(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	publishTrips(t, "T1")

	provider := NewMockProvider(7)
	provider.MinDelay, provider.MaxDelay = time.Millisecond, 2*time.Millisecond
	handler := HandlePollTask(rdb, provider)

	payload, _ := json.Marshal(pollPayload{TripID: "T1", Date: itDate})
	task := newTask(payload)

	if err := handler(ctx, task); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if _, err := rdb.Get(ctx, model.SeatMapKey("T1", itDate)).Result(); err != nil {
		t.Fatalf("poll should have written seat data: %v", err)
	}

	// The distributed lock must make an immediate second poll a no-op, so a
	// fleet of workers cannot hammer the upstream provider.
	before, _ := rdb.Get(ctx, model.SeatMapKey("T1", itDate)).Result()
	if err := handler(ctx, task); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	after, _ := rdb.Get(ctx, model.SeatMapKey("T1", itDate)).Result()
	if before != after {
		t.Error("a locked-out poll must not overwrite seat data")
	}

	// A trip that is no longer in the schedule is discarded, not retried.
	gone, _ := json.Marshal(pollPayload{TripID: "GHOST", Date: itDate})
	if err := handler(ctx, newTask(gone)); err != nil {
		t.Errorf("an unknown trip should be dropped quietly, got %v", err)
	}

	// Malformed payloads must not be retried forever.
	if err := handler(ctx, newTask([]byte("{not json"))); err == nil {
		t.Error("a malformed payload should error")
	}
}

// newTask wraps a payload as the asynq task the poll handler expects.
func newTask(payload []byte) *asynq.Task {
	return asynq.NewTask(TaskSeatPoll, payload)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
