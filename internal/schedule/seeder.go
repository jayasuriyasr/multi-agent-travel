package schedule

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// SeedDatabase truncates domain tables and inserts specific mock data
// for stations, routes, trips, and stop times, and seeds Redis with seat data.
func SeedDatabase(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client) {
	log.Println("[seeder] starting database seed...")

	_, err := pool.Exec(ctx, `TRUNCATE TABLE stop_times, trips, routes, stations RESTART IDENTITY CASCADE`)	// identity means id stars from 0 not from rows which is existing cascade means delete every table whose takle connected with foriegn key
	if err != nil {
		log.Fatalf("[seeder] truncate failed: %v", err)
	}
	log.Println("[seeder] tables truncated")

	// 1. Seed Stations
	type stationRow struct {
		id   string
		name string
		city string
		lat  float64
		lon  float64
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

	stationBatch := &pgx.Batch{}
	for _, s := range stations {
		stationBatch.Queue(
			`INSERT INTO stations (id, name, city, lat, lon) VALUES ($1, $2, $3, $4, $5)`,
			s.id, s.name, s.city, s.lat, s.lon,
		)
	}
	if err := sendBatch(ctx, pool, stationBatch); err != nil {
		log.Fatalf("[seeder] station insert failed: %v", err)
	}
	log.Printf("[seeder] inserted %d stations", len(stations))

	// 2. Seed Routes & Legs
	type routeDef struct {
		routeID string
		name    string
		mode    string
		stopIDs []string
	}

	routes := []routeDef{
		{
			routeID: "ROUTE-EXP-1",
			name:    "Express Route",
			mode:    "rail",
			stopIDs: []string{"STA-001", "STA-004"}, // NY, DC
		},
		{
			routeID: "ROUTE-LOC-1",
			name:    "Local Route",
			mode:    "rail",
			stopIDs: []string{"STA-001", "STA-002", "STA-003", "STA-004"}, // NY, PHL, BAL, DC
		},
		{
			routeID: "ROUTE-NOR-1",
			name:    "Northern Local",
			mode:    "rail",
			stopIDs: []string{"STA-005", "STA-006", "STA-007", "STA-001"}, // BOS, PVD, NHV, NY
		},
	}

	routeBatch := &pgx.Batch{}
	for _, r := range routes {
		routeBatch.Queue(
			`INSERT INTO routes (route_id, name, mode) VALUES ($1, $2, $3)`,
			r.routeID, r.name, r.mode,
		)
	}
	if err := sendBatch(ctx, pool, routeBatch); err != nil {
		log.Fatalf("[seeder] route insert failed: %v", err)
	}
	log.Printf("[seeder] inserted %d routes", len(routes))

	// 3. Seed Trips (Schedules) spanning today and tomorrow
	baseDate := time.Now().UTC().Truncate(24 * time.Hour)
	tripBatch := &pgx.Batch{}
	stopBatch := &pgx.Batch{}
	totalTrips := 0
	totalStopTimes := 0
	
	const tripsPerDay = 5
	const stopIntervalSec = 3600 // 1 hour between stops
	const firstDepartureH = 8    // start at 8 AM
	const tripSpacingH = 2       // 2 hours between trips

	for d := 0; d < 2; d++ { // today and tomorrow
		date := baseDate.AddDate(0, 0, d)
		dateStr := date.Format("2006-01-02")

		for _, route := range routes {
			for t := 0; t < tripsPerDay; t++ {
				tripID := fmt.Sprintf("%s_%s_T%02d", route.routeID, dateStr, t+1)
				departureHour := firstDepartureH + t*tripSpacingH
				firstStop := time.Date(
					date.Year(), date.Month(), date.Day(),
					departureHour, 0, 0, 0, time.UTC,
				)
				baseUnix := firstStop.Unix()

				tripBatch.Queue(
					`INSERT INTO trips (trip_id, date, route_id, departure_unix)
					 VALUES ($1, $2, $3, $4)`,
					tripID, dateStr, route.routeID, baseUnix,
				)
				totalTrips++

				// Seed Redis seat availability for this trip directly to bypass import cycle
				tripDateStr := fmt.Sprintf("%s:%s", tripID, dateStr)
				tsStr := fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1e9)
				
				pipe := rdb.Pipeline()
				pipe.Set(ctx, fmt.Sprintf("seat:map:%s", tripDateStr), `{"lower":10,"upper":10,"seater":20}`, 0)
				pipe.Set(ctx, fmt.Sprintf("seat:ts:%s", tripDateStr), tsStr, 0)
				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: "seat:dirty_stream",
					MaxLen: 200000,
					Approx: true,
					Values: map[string]interface{}{
						"trip":       tripDateStr,
						"changed_at": tsStr,
					},
				})
				if _, err := pipe.Exec(ctx); err != nil {
					log.Printf("[seeder] warning: failed to seed seats in Redis for trip %s: %v", tripID, err)
				}

				for seq, stationID := range route.stopIDs {
					depUnix := baseUnix + int64(seq)*int64(stopIntervalSec)
					stopBatch.Queue(
						`INSERT INTO stop_times (trip_id, date, stop_seq, station_id, departure_unix)
						 VALUES ($1, $2, $3, $4, $5)`,
						tripID, dateStr, seq, stationID, depUnix,
					)
					totalStopTimes++
				}
			}
		}
	}

	if err := sendBatch(ctx, pool, tripBatch); err != nil {
		log.Fatalf("[seeder] trip insert failed: %v", err)
	}
	log.Printf("[seeder] inserted %d trips", totalTrips)

	if err := sendBatch(ctx, pool, stopBatch); err != nil {
		log.Fatalf("[seeder] stop_time insert failed: %v", err)
	}
	log.Printf("[seeder] inserted %d stop_time rows", totalStopTimes)

	// Bump schema_version so that watcher reloads routes
	_, err = pool.Exec(ctx, `UPDATE schema_version SET updated_at = NOW() WHERE id = 1`)
	if err != nil {
		log.Fatalf("[seeder] schema_version bump failed: %v", err)
	}

	log.Println("[seeder] ✅ database seed complete")
	log.Printf("[seeder] summary:")
	log.Printf("  stations   : %d", len(stations))
	log.Printf("  routes     : %d", len(routes))
	log.Printf("  trips      : %d", totalTrips)
	log.Printf("  stop_times : %d", totalStopTimes)
}

func sendBatch(ctx context.Context, pool *pgxpool.Pool, batch *pgx.Batch) error {
	if batch.Len() == 0 {
		return nil
	}
	results := pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch item %d: %w", i, err)
		}
	}
	return nil
}