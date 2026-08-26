package raptor

import (
	"context"
	"os"
	"testing"
	"time"

	"axentra/internal/breaker"
	"axentra/internal/model"

	"github.com/redis/go-redis/v9"
)

func transitPath(trips ...string) model.Path {
	p := model.Path{}
	for _, id := range trips {
		p.Legs = append(p.Legs, model.Leg{
			Kind: model.LegTransit, TripID: id, Date: testDate,
			RouteID: "R", BoardStation: "A", AlightStation: "B",
		})
	}
	return p
}

func TestValidatePath(t *testing.T) {
	params := model.SearchParams{Origin: "A", Destination: "C", DepTime: 1000}

	good := model.Path{Legs: []model.Leg{
		{Kind: model.LegTransit, TripID: "T1", BoardStation: "A", AlightStation: "B", DepartureUnix: 1000, ArrivalUnix: 2000},
		{Kind: model.LegTransit, TripID: "T2", BoardStation: "B", AlightStation: "C", DepartureUnix: 2100, ArrivalUnix: 3000},
	}}
	if err := ValidatePath(good, params); err != nil {
		t.Fatalf("a well-formed path should validate, got %v", err)
	}

	cases := []struct {
		name string
		path model.Path
		want string
	}{
		{"no legs", model.Path{}, "no legs"},
		{
			name: "starts somewhere else",
			path: model.Path{Legs: []model.Leg{{Kind: model.LegTransit, TripID: "T", BoardStation: "Z", AlightStation: "C", DepartureUnix: 1000, ArrivalUnix: 2000}}},
			want: "expected origin",
		},
		{
			name: "ends somewhere else",
			path: model.Path{Legs: []model.Leg{{Kind: model.LegTransit, TripID: "T", BoardStation: "A", AlightStation: "Z", DepartureUnix: 1000, ArrivalUnix: 2000}}},
			want: "expected destination",
		},
		{
			name: "departs before the requested time",
			path: model.Path{Legs: []model.Leg{{Kind: model.LegTransit, TripID: "T", BoardStation: "A", AlightStation: "C", DepartureUnix: 500, ArrivalUnix: 2000}}},
			want: "before requested",
		},
		{
			name: "arrives before it departs",
			path: model.Path{Legs: []model.Leg{{Kind: model.LegTransit, TripID: "T", BoardStation: "A", AlightStation: "C", DepartureUnix: 3000, ArrivalUnix: 2000}}},
			want: "before it departs",
		},
		{
			name: "gap between legs",
			path: model.Path{Legs: []model.Leg{
				{Kind: model.LegTransit, TripID: "T1", BoardStation: "A", AlightStation: "B", DepartureUnix: 1000, ArrivalUnix: 2000},
				{Kind: model.LegTransit, TripID: "T2", BoardStation: "X", AlightStation: "C", DepartureUnix: 2100, ArrivalUnix: 3000},
			}},
			want: "gap between leg",
		},
		{
			name: "boards a leg before the previous one arrives",
			path: model.Path{Legs: []model.Leg{
				{Kind: model.LegTransit, TripID: "T1", BoardStation: "A", AlightStation: "B", DepartureUnix: 1000, ArrivalUnix: 2000},
				{Kind: model.LegTransit, TripID: "T2", BoardStation: "B", AlightStation: "C", DepartureUnix: 1500, ArrivalUnix: 3000},
			}},
			want: "departs",
		},
		{
			name: "transit leg with no trip id",
			path: model.Path{Legs: []model.Leg{{Kind: model.LegTransit, BoardStation: "A", AlightStation: "C", DepartureUnix: 1000, ArrivalUnix: 2000}}},
			want: "no trip id",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePath(tc.path, params); err == nil {
				t.Fatalf("want an error mentioning %q, got nil", tc.want)
			}
		})
	}
}

// newValidator builds a Validator with test-friendly defaults.
func newValidator(rdb *redis.Client, strict bool, maxResults int) *Validator {
	return NewValidator(rdb, ValidatorConfig{
		Strict: strict, MaxResults: maxResults,
		BreakerThreshold: 3, BreakerCooldown: 50 * time.Millisecond,
	})
}

func TestValidate_NoCandidates(t *testing.T) {
	if got := newValidator(nil, false, 5).Validate(context.Background(), nil, "lower", 1); got != nil {
		t.Fatalf("want nil for no candidates, got %v", got)
	}
}

func TestValidate_NoRedisClient(t *testing.T) {
	candidates := []model.Path{transitPath("T1"), transitPath("T2"), transitPath("T3")}

	// Strict mode is for booking flows: without a way to confirm seats, hand
	// out nothing rather than something that might not exist.
	if got := newValidator(nil, true, 5).Validate(context.Background(), candidates, "lower", 1); got != nil {
		t.Errorf("strict mode should reject everything when validation is impossible, got %d", len(got))
	}
	// Read-only search prefers degraded results to no service.
	got := newValidator(nil, false, 2).Validate(context.Background(), candidates, "lower", 1)
	if len(got) != 2 {
		t.Errorf("lenient mode should return truncated candidates, got %d", len(got))
	}
}

func TestValidate_WalkOnlyPathNeedsNoSeats(t *testing.T) {
	walkOnly := model.Path{Legs: []model.Leg{{
		Kind: model.LegWalk, RouteID: model.WalkRouteID, BoardStation: "A", AlightStation: "B",
	}}}
	got := newValidator(nil, true, 5).Validate(context.Background(), []model.Path{walkOnly}, "lower", 1)
	if len(got) != 1 {
		t.Fatalf("a walking journey needs no seat validation, got %d paths", len(got))
	}
}

// A dependency that is already failing must not receive every search's
// validation MGET. Once the breaker trips, calls are answered from the Strict
// policy without touching Redis at all.
func TestValidate_CircuitBreakerStopsHammeringRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // nothing listening
		DialTimeout: 20 * time.Millisecond,
		ReadTimeout: 20 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	v := newValidator(rdb, false, 5)
	candidates := []model.Path{transitPath("T1")}
	ctx := context.Background()

	if v.BreakerState() != breaker.StateClosed {
		t.Fatalf("breaker should start closed, got %s", v.BreakerState())
	}
	for i := 0; i < 3; i++ {
		v.Validate(ctx, candidates, "lower", 1)
	}
	if v.BreakerState() != breaker.StateOpen {
		t.Fatalf("three consecutive failures should open the breaker, got %s", v.BreakerState())
	}
	if v.Healthy() {
		t.Error("an open breaker is not healthy")
	}

	// With the breaker open the call must return immediately rather than
	// waiting out another dial timeout.
	start := time.Now()
	got := v.Validate(ctx, candidates, "lower", 1)
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("an open breaker should fail fast, took %v", elapsed)
	}
	if len(got) != 1 {
		t.Errorf("lenient mode should still return unvalidated candidates, got %d", len(got))
	}

	// Strict mode makes the same situation a rejection, not a degradation.
	strict := newValidator(rdb, true, 5)
	for i := 0; i < 3; i++ {
		strict.Validate(ctx, candidates, "lower", 1)
	}
	if got := strict.Validate(ctx, candidates, "lower", 1); got != nil {
		t.Errorf("strict mode with an open breaker must reject everything, got %d", len(got))
	}
}

// Recovery: after the cooldown one probe gets through, and a healthy Redis
// closes the breaker again.
func TestValidate_BreakerRecovers(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()

	key := model.SeatMapKey("V_RECOVER", testDate)
	if err := rdb.Set(ctx, key, `{"lower":5}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Del(ctx, key) })

	v := newValidator(rdb, false, 5)
	candidates := []model.Path{transitPath("V_RECOVER")}

	// Force the breaker open without a real failure, then wait out the cooldown.
	for i := 0; i < 3; i++ {
		v.cb.Failure()
	}
	if v.BreakerState() != breaker.StateOpen {
		t.Fatalf("breaker should be open, got %s", v.BreakerState())
	}

	time.Sleep(80 * time.Millisecond)
	got := v.Validate(ctx, candidates, "lower", 1)
	if len(got) != 1 {
		t.Fatalf("the probe should have validated against a healthy redis, got %d", len(got))
	}
	if v.BreakerState() != breaker.StateClosed {
		t.Errorf("a successful probe must close the breaker, got %s", v.BreakerState())
	}
}

// ── Integration: run against a real Redis by setting REDIS_TEST_ADDR ─────────

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR (e.g. 127.0.0.1:6379) to run seat validation integration tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis at %s not reachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestValidate_AgainstRedis(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()

	set := func(trip, json string) {
		key := model.SeatMapKey(trip, testDate)
		if err := rdb.Set(ctx, key, json, 0).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { rdb.Del(ctx, key) })
	}
	set("V_PLENTY", `{"lower":10,"upper":10}`)
	set("V_ONE", `{"lower":1}`)
	set("V_NONE", `{"lower":0}`)
	// V_MISSING is deliberately never written.

	candidates := []model.Path{
		transitPath("V_PLENTY"),
		transitPath("V_ONE"),
		transitPath("V_NONE"),
		transitPath("V_MISSING"),
		transitPath("V_PLENTY", "V_NONE"), // one bad leg poisons the path
	}

	t.Run("single passenger", func(t *testing.T) {
		got := newValidator(rdb, false, 10).Validate(ctx, candidates, "lower", 1)
		if len(got) != 2 {
			t.Fatalf("want V_PLENTY and V_ONE to survive, got %d: %v", len(got), summaries(got))
		}
	})

	t.Run("group of two drops the single-seat trip", func(t *testing.T) {
		got := newValidator(rdb, false, 10).Validate(ctx, candidates, "lower", 2)
		if len(got) != 1 || got[0].Legs[0].TripID != "V_PLENTY" {
			t.Fatalf("want only V_PLENTY, got %v", summaries(got))
		}
	})

	t.Run("a missing redis key is treated as unconfirmed", func(t *testing.T) {
		got := newValidator(rdb, false, 10).Validate(ctx, []model.Path{transitPath("V_MISSING")}, "lower", 1)
		if len(got) != 0 {
			t.Fatalf("an absent seat record must not pass validation, got %d", len(got))
		}
	})

	t.Run("results are truncated", func(t *testing.T) {
		got := newValidator(rdb, false, 1).Validate(ctx, candidates, "lower", 1)
		if len(got) != 1 {
			t.Fatalf("want 1 result, got %d", len(got))
		}
	})

	t.Run("unknown seat class fails validation", func(t *testing.T) {
		got := newValidator(rdb, false, 10).Validate(ctx, candidates, "business", 1)
		if len(got) != 0 {
			t.Fatalf("no trip offers business class, got %d", len(got))
		}
	})
}

// More candidate keys than a single MGET chunk, to exercise the chunking path.
func TestValidate_ChunksLargeKeySets(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()

	total := DefaultMGetChunk*2 + 7
	candidates := make([]model.Path, 0, total)
	for i := 0; i < total; i++ {
		trip := "V_BULK_" + itoa(i)
		key := model.SeatMapKey(trip, testDate)
		if err := rdb.Set(ctx, key, `{"lower":5}`, 0).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { rdb.Del(ctx, key) })
		candidates = append(candidates, transitPath(trip))
	}

	got := newValidator(rdb, false, total).Validate(ctx, candidates, "lower", 1)
	if len(got) != total {
		t.Fatalf("want all %d candidates to survive across chunks, got %d", total, len(got))
	}
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
