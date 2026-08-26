package schedule

import (
	"context"
	"fmt"
	"os"
	"testing"

	"axentra/internal/migrate"
	"axentra/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to the database named by PG_TEST_DSN and applies the
// repository's own migrations to a throwaway schema.
//
// Skipped unless PG_TEST_DSN is set, so `go test ./...` stays runnable with no
// infrastructure while the real query paths are still covered in CI.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("set PG_TEST_DSN (e.g. postgres://user:pass@localhost:5432/db?sslmode=disable) to run schedule integration tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("cannot connect to PG_TEST_DSN: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PG_TEST_DSN not reachable: %v", err)
	}

	schema := fmt.Sprintf("axentra_test_%d", os.Getpid())
	mustExec(t, pool, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	mustExec(t, pool, `CREATE SCHEMA `+schema)
	mustExec(t, pool, `SET search_path TO `+schema)

	// Every pooled connection needs the same search_path, not just this one.
	pool.Close()
	pool, err = pgxpool.New(ctx, dsn+"&search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.Up(ctx, pool, "../../migrations"); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}

	t.Cleanup(func() {
		mustExec(t, pool, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		pool.Close()
	})
	ResetManifest()
	return pool
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// insertTrip writes a trip and its stop times. times holds arrival/departure
// pairs per stop.
func insertTrip(t *testing.T, pool *pgxpool.Pool, routeID, tripID, date string, stops []string, times [][2]int64) {
	t.Helper()
	mustExec(t, pool, `INSERT INTO trips (trip_id, date, route_id, departure_unix) VALUES ($1,$2,$3,$4)`,
		tripID, date, routeID, times[0][1])
	for i, s := range stops {
		mustExec(t, pool,
			`INSERT INTO stop_times (trip_id, date, stop_seq, station_id, arrival_unix, departure_unix)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			tripID, date, i, s, times[i][0], times[i][1])
	}
}

func seedBasics(t *testing.T, pool *pgxpool.Pool, stations []string, routes []string) {
	t.Helper()
	for _, s := range stations {
		mustExec(t, pool, `INSERT INTO stations (id, name, city) VALUES ($1,$1,$1)`, s)
	}
	for _, r := range routes {
		mustExec(t, pool, `INSERT INTO routes (route_id, name, mode) VALUES ($1,$1,'rail')`, r)
	}
}

func TestReloadRouteArrays_Integration(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const date = "2026-08-23"

	seedBasics(t, pool, []string{"S1", "S2", "S3"}, []string{"R_FIFO", "R_MIXED"})

	// A FIFO route: the later service stays later at every stop.
	insertTrip(t, pool, "R_FIFO", "F1", date, []string{"S1", "S2", "S3"},
		[][2]int64{{100, 100}, {200, 260}, {400, 400}})
	insertTrip(t, pool, "R_FIFO", "F2", date, []string{"S1", "S2", "S3"},
		[][2]int64{{500, 500}, {600, 660}, {800, 800}})

	// A non-FIFO route: the second service overtakes the first before S2.
	insertTrip(t, pool, "R_MIXED", "M1", date, []string{"S1", "S2", "S3"},
		[][2]int64{{100, 100}, {900, 960}, {1800, 1800}})
	insertTrip(t, pool, "R_MIXED", "M2", date, []string{"S1", "S2", "S3"},
		[][2]int64{{200, 200}, {300, 360}, {500, 500}})

	mustExec(t, pool, `INSERT INTO footpaths (station_id, neighbour_id, walk_seconds) VALUES ('S1','S2',300)`)
	mustExec(t, pool, `INSERT INTO footpaths (station_id, neighbour_id, walk_seconds) VALUES ('S2','S3',300)`)

	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	buf := LiveRoutes()

	if got := len(buf.Routes); got != 2 {
		t.Fatalf("loaded %d routes, want 2", got)
	}
	if got := len(buf.TripIndex); got != 4 {
		t.Fatalf("loaded %d trips, want 4", got)
	}

	byID := map[string]int{}
	for i, r := range buf.Routes {
		byID[r.RouteID] = i
	}
	if !buf.IsFIFO(byID["R_FIFO"]) {
		t.Error("R_FIFO should be detected as FIFO")
	}
	if buf.IsFIFO(byID["R_MIXED"]) {
		t.Error("R_MIXED overtakes itself and must not be treated as FIFO")
	}

	// Arrivals must survive the round trip as distinct values, not be
	// overwritten by departures.
	loc := buf.TripIndex[model.TripKey{TripID: "F1", Date: date}]
	tst := buf.StopTimes[loc.RouteIdx][loc.TripIdx]
	if tst.Arrivals[1] != 200 || tst.Departures[1] != 260 {
		t.Errorf("dwell time lost: arrivals=%v departures=%v", tst.Arrivals, tst.Departures)
	}

	// The walk graph must arrive transitively closed.
	var found bool
	for _, e := range buf.Footpaths["S1"] {
		if e.NeighbourStop == "S3" {
			found = true
			if e.WalkSeconds != 600 {
				t.Errorf("S1→S3 should close to 600s, got %d", e.WalkSeconds)
			}
		}
	}
	if !found {
		t.Error("footpath closure did not reach S3 from S1")
	}
}

// Reloading unchanged data must not publish; changing an arrival time or a
// footpath must. The second half is the regression: the old manifest hash
// covered neither, so those reloads were silently dropped.
func TestReloadRouteArrays_PublishesOnlyRealChanges(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const date = "2026-08-23"

	seedBasics(t, pool, []string{"S1", "S2"}, []string{"R1"})
	insertTrip(t, pool, "R1", "T1", date, []string{"S1", "S2"}, [][2]int64{{100, 100}, {200, 200}})

	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatal(err)
	}
	first := LiveRoutes()

	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatal(err)
	}
	if LiveRoutes() != first {
		t.Error("an unchanged reload should not republish the buffer")
	}

	mustExec(t, pool, `UPDATE stop_times SET arrival_unix = 195 WHERE trip_id = 'T1' AND stop_seq = 1`)
	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatal(err)
	}
	second := LiveRoutes()
	if second == first {
		t.Fatal("an arrival-time change must republish the buffer")
	}

	mustExec(t, pool, `INSERT INTO footpaths (station_id, neighbour_id, walk_seconds) VALUES ('S1','S2',120)`)
	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatal(err)
	}
	if LiveRoutes() == second {
		t.Fatal("a footpath change must republish the buffer")
	}
}

// A trip whose stop pattern does not match its route cannot share the route's
// positional arrays. It must be dropped, not indexed out of bounds.
func TestReloadRouteArrays_SkipsMismatchedTrips(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const date = "2026-08-23"

	seedBasics(t, pool, []string{"S1", "S2", "S3"}, []string{"R1"})
	insertTrip(t, pool, "R1", "T_FULL", date, []string{"S1", "S2", "S3"},
		[][2]int64{{100, 100}, {200, 200}, {300, 300}})
	insertTrip(t, pool, "R1", "T_SHORT", date, []string{"S1", "S2"},
		[][2]int64{{400, 400}, {500, 500}})

	if err := ReloadRouteArrays(ctx, pool, DefaultLoadOptions()); err != nil {
		t.Fatal(err)
	}
	buf := LiveRoutes()
	if _, ok := buf.TripIndex[model.TripKey{TripID: "T_SHORT", Date: date}]; ok {
		t.Error("a short-turn trip must not be folded into a longer route")
	}
	if _, ok := buf.TripIndex[model.TripKey{TripID: "T_FULL", Date: date}]; !ok {
		t.Error("the conforming trip should still load")
	}
}

func TestIngestBatch_Integration(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const date = "2026-08-23"

	seedBasics(t, pool, []string{"S1", "S2"}, []string{"R1"})

	trips := []IngestTrip{{
		TripID: "T1", Date: date, RouteID: "R1", DepUnix: 100,
		StopTimes: []model.StopTime{
			{TripID: "T1", Date: date, StopSeq: 0, StationID: "S1", ArrivalUnix: 100, DepartureUnix: 100},
			{TripID: "T1", Date: date, StopSeq: 1, StationID: "S2", ArrivalUnix: 200, DepartureUnix: 260},
		},
	}}
	if err := IngestBatch(ctx, pool, trips); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Re-ingesting the same trip must replace it, not duplicate its stops.
	if err := IngestBatch(ctx, pool, trips); err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	var stops int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM stop_times WHERE trip_id = 'T1'`).Scan(&stops); err != nil {
		t.Fatal(err)
	}
	if stops != 2 {
		t.Errorf("re-ingest left %d stop rows, want 2", stops)
	}

	// A batch that fails validation must not touch the database at all.
	bad := []IngestTrip{{
		TripID: "T2", Date: date, RouteID: "R1",
		StopTimes: []model.StopTime{{TripID: "T2", Date: date, StationID: "S1", ArrivalUnix: 500, DepartureUnix: 100}},
	}}
	if err := IngestBatch(ctx, pool, bad); err == nil {
		t.Fatal("want a validation error")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM trips WHERE trip_id = 'T2'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("a rejected batch left %d rows behind", count)
	}
}
