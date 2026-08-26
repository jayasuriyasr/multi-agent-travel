package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"axentra/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	// daysToSeed is how many calendar days of trips to generate, so
	// advance-booking searches have something to find.
	daysToSeed = 30
	// stopDwellSec is the halt time at an intermediate stop: arrival + dwell
	// = departure. Modelling it is what makes arrival and departure genuinely
	// different values rather than two names for the same number.
	stopDwellSec = 300
	// seatPipelineBatch is how many trips' seat writes go in one Redis pipeline.
	seatPipelineBatch = 200
)

type stationRow struct {
	id, name, city string
	lat, lon       float64
}

type routeDef struct {
	routeID, name, mode string
	stopIDs             []string
	// departures lists each trip's first-stop departure hour, in local time on
	// the operating date. Fractional hours are allowed.
	departures []float64
	// hopSeconds is the run time between consecutive stops.
	hopSeconds int64
	// speedup scales run time per trip index, so trip i can overtake trip i-1.
	// 1.0 for every trip means the route is FIFO.
	speedups []float64
}

type footpathRow struct {
	stationID, neighbourID string
	walkSeconds            int
}

// SeedDatabase truncates the domain tables and inserts a small but deliberately
// varied demo network: direct and stopping services, a walk transfer, an
// overnight service that crosses midnight, and one route whose express
// overtakes its local — which exercises the engine's non-FIFO trip lookup
// rather than leaving that path untested.
func SeedDatabase(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client) error {
	slog.Info("seeding database")

	if _, err := pool.Exec(ctx,
		`TRUNCATE TABLE footpaths, stop_times, trips, routes, stations RESTART IDENTITY CASCADE`); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	stations := []stationRow{
		{"STA-001", "New York", "New York", 40.7128, -74.0060},
		{"STA-002", "Philadelphia", "Philadelphia", 39.9526, -75.1652},
		{"STA-003", "Baltimore", "Baltimore", 39.2904, -76.6122},
		{"STA-004", "Washington DC", "Washington DC", 38.9072, -77.0369},
		{"STA-005", "Boston", "Boston", 42.3601, -71.0589},
		{"STA-006", "Providence", "Providence", 41.8240, -71.4128},
		{"STA-007", "New Haven", "New Haven", 41.3083, -72.9279},
		{"STA-008", "Stamford", "Stamford", 41.0534, -73.5387},
		{"STA-009", "Newark", "Newark", 40.7357, -74.1724},
		{"STA-010", "Trenton", "Trenton", 40.2171, -74.7429},
	}

	batch := &pgx.Batch{}
	for _, s := range stations {
		batch.Queue(`INSERT INTO stations (id, name, city, lat, lon) VALUES ($1,$2,$3,$4,$5)`,
			s.id, s.name, s.city, s.lat, s.lon)
	}
	if err := sendBatch(ctx, pool, batch); err != nil {
		return fmt.Errorf("insert stations: %w", err)
	}

	routes := []routeDef{
		{
			routeID: "ROUTE-EXP-1", name: "Northeast Express", mode: "rail",
			stopIDs:    []string{"STA-001", "STA-004"}, // NY → DC nonstop
			departures: []float64{8, 10, 12, 14, 16},
			hopSeconds: 3 * 3600,
			speedups:   []float64{1, 1, 1, 1, 1},
		},
		{
			routeID: "ROUTE-LOC-1", name: "Northeast Regional", mode: "rail",
			stopIDs:    []string{"STA-001", "STA-002", "STA-003", "STA-004"}, // NY → PHL → BAL → DC
			departures: []float64{7, 9, 11, 13, 15},
			hopSeconds: 3600,
			speedups:   []float64{1, 1, 1, 1, 1},
		},
		{
			routeID: "ROUTE-NOR-1", name: "Northern Local", mode: "rail",
			stopIDs:    []string{"STA-005", "STA-006", "STA-007", "STA-001"}, // BOS → PVD → NHV → NY
			departures: []float64{6, 8, 10, 12, 14},
			hopSeconds: 3600,
			speedups:   []float64{1, 1, 1, 1, 1},
		},
		{
			// Deliberately non-FIFO: the 09:00 express covers the same track in
			// half the time and passes the 08:00 local before Trenton. Binary
			// search over trips is invalid on a route like this, so the loader
			// detects it and the engine falls back to a linear scan.
			routeID: "ROUTE-MIX-1", name: "Shore Line (mixed express/local)", mode: "rail",
			stopIDs:    []string{"STA-008", "STA-010", "STA-002"}, // STM → TRE → PHL
			departures: []float64{8, 9, 11, 13},
			hopSeconds: 5400,
			speedups:   []float64{1, 0.4, 1, 0.5},
		},
		{
			// Departs late evening and arrives after midnight, so its stop
			// times genuinely cross a calendar boundary.
			routeID: "ROUTE-OVN-1", name: "Overnight Sleeper", mode: "rail",
			stopIDs:    []string{"STA-005", "STA-001", "STA-004"}, // BOS → NY → DC
			departures: []float64{22, 23.5},
			hopSeconds: 4 * 3600,
			speedups:   []float64{1, 1},
		},
	}

	batch = &pgx.Batch{}
	for _, r := range routes {
		batch.Queue(`INSERT INTO routes (route_id, name, mode) VALUES ($1,$2,$3)`, r.routeID, r.name, r.mode)
	}
	if err := sendBatch(ctx, pool, batch); err != nil {
		return fmt.Errorf("insert routes: %w", err)
	}

	baseDate := time.Now().UTC().Truncate(24 * time.Hour)
	tripBatch := &pgx.Batch{}
	stopBatch := &pgx.Batch{}
	var seatKeys []model.TripKey
	totalTrips, totalStops := 0, 0

	for d := 0; d < daysToSeed; d++ {
		date := baseDate.AddDate(0, 0, d)
		dateStr := date.Format(model.DateLayout)

		for _, route := range routes {
			for t, depHour := range route.departures {
				tripID := fmt.Sprintf("%s_%s_T%02d", route.routeID, dateStr, t+1)
				startUnix := date.Add(time.Duration(depHour * float64(time.Hour))).Unix()

				speed := 1.0
				if t < len(route.speedups) && route.speedups[t] > 0 {
					speed = route.speedups[t]
				}
				hop := int64(float64(route.hopSeconds) * speed)

				tripBatch.Queue(
					`INSERT INTO trips (trip_id, date, route_id, departure_unix) VALUES ($1,$2,$3,$4)`,
					tripID, dateStr, route.routeID, startUnix)
				totalTrips++

				for seq, stationID := range route.stopIDs {
					// Arrival is the running time from the origin; departure adds
					// dwell at every stop except the first, where the service
					// originates and there is nothing to wait for.
					arr := startUnix + int64(seq)*hop
					dep := arr
					if seq > 0 {
						dep = arr + stopDwellSec
					}
					// Shift subsequent arrivals by accumulated dwell so the
					// timetable stays monotonic.
					arr += int64(seq) * stopDwellSec
					dep += int64(seq) * stopDwellSec

					stopBatch.Queue(
						`INSERT INTO stop_times (trip_id, date, stop_seq, station_id, arrival_unix, departure_unix)
						 VALUES ($1,$2,$3,$4,$5,$6)`,
						tripID, dateStr, seq, stationID, arr, dep)
					totalStops++
				}

				seatKeys = append(seatKeys, model.TripKey{TripID: tripID, Date: dateStr})
			}
		}
	}

	if err := sendBatch(ctx, pool, tripBatch); err != nil {
		return fmt.Errorf("insert trips: %w", err)
	}
	if err := sendBatch(ctx, pool, stopBatch); err != nil {
		return fmt.Errorf("insert stop_times: %w", err)
	}

	footpaths := []footpathRow{
		{"STA-009", "STA-001", 600}, {"STA-001", "STA-009", 600}, // Newark ↔ New York
		{"STA-010", "STA-002", 720}, {"STA-002", "STA-010", 720}, // Trenton ↔ Philadelphia
		{"STA-008", "STA-007", 480}, {"STA-007", "STA-008", 480}, // Stamford ↔ New Haven
	}
	batch = &pgx.Batch{}
	for _, fp := range footpaths {
		batch.Queue(
			`INSERT INTO footpaths (station_id, neighbour_id, walk_seconds) VALUES ($1,$2,$3)
			 ON CONFLICT (station_id, neighbour_id) DO UPDATE SET walk_seconds = EXCLUDED.walk_seconds`,
			fp.stationID, fp.neighbourID, fp.walkSeconds)
	}
	if err := sendBatch(ctx, pool, batch); err != nil {
		return fmt.Errorf("insert footpaths: %w", err)
	}

	if err := seedSeats(ctx, rdb, seatKeys); err != nil {
		return fmt.Errorf("seed seat data: %w", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE schema_version SET updated_at = NOW() WHERE id = 1`); err != nil {
		return fmt.Errorf("bump schema_version: %w", err)
	}

	slog.Info("seed complete",
		"stations", len(stations), "routes", len(routes), "trips", totalTrips,
		"stop_times", totalStops, "footpaths", len(footpaths), "days", daysToSeed)
	return nil
}

// seedSeats writes availability for every seeded trip.
//
// Batched into pipelines of seatPipelineBatch: one pipeline per trip is an N+1
// that costs a round trip per trip, and a month of schedule is thousands of them.
func seedSeats(ctx context.Context, rdb *redis.Client, keys []model.TripKey) error {
	const seatJSON = `{"lower":10,"upper":10,"seater":20}`

	for start := 0; start < len(keys); start += seatPipelineBatch {
		end := start + seatPipelineBatch
		if end > len(keys) {
			end = len(keys)
		}
		ts := fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1e9)

		pipe := rdb.Pipeline()
		for _, k := range keys[start:end] {
			pipe.Set(ctx, model.SeatMapKey(k.TripID, k.Date), seatJSON, 0)
			pipe.Set(ctx, model.SeatTSKey(k.TripID, k.Date), ts, 0)
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: model.DirtyStreamKey,
				MaxLen: 200000,
				Approx: true,
				Values: map[string]interface{}{
					"trip":       model.SeatTripDate(k.TripID, k.Date),
					"changed_at": ts,
				},
			})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("seat pipeline [%d:%d]: %w", start, end, err)
		}
	}
	slog.Info("seat data seeded", "trips", len(keys))
	return nil
}

// sendBatch executes a pgx batch and surfaces the first failing statement.
func sendBatch(ctx context.Context, pool *pgxpool.Pool, batch *pgx.Batch) error {
	if batch.Len() == 0 {
		return nil
	}
	results := pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch item %d of %d: %w", i, batch.Len(), err)
		}
	}
	return nil
}
