package schedule

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"axentra/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// watcherInterval is the fixed poll cadence for schema_version checks.
// Exported as a constant so tests can verify it without magic numbers.
const watcherInterval = 2 * time.Minute

// WatcherLoop polls the schema_version table on a fixed 2-minute ticker.
// It triggers a full in-memory reload whenever the updated_at watermark
// advances. This is the ONLY goroutine that writes to the route buffers.
//
// Caller is responsible for cancelling ctx to stop the loop.
func WatcherLoop(ctx context.Context, pool *pgxpool.Pool) {
	ticker := time.NewTicker(watcherInterval)
	defer ticker.Stop()

	var lastTS time.Time

	log.Printf("[schedule] watcher: started (interval=%s)", watcherInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("[schedule] watcher: context cancelled, stopping")
			return
		case <-ticker.C:
		}

		var ts time.Time
		err := pool.QueryRow(ctx,
			`SELECT updated_at FROM schema_version WHERE id = 1`,
		).Scan(&ts)
		if err != nil {
			log.Printf("[schedule] watcher: failed to read schema_version: %v", err)
			continue
		}

		if ts.After(lastTS) {
			lastTS = ts
			log.Printf("[schedule] watcher: schema_version changed at %v, reloading", ts)
			if err := ReloadRouteArrays(ctx, pool); err != nil {
				log.Printf("[schedule] watcher: reload failed: %v", err)
			}
		}
	}
}

// ReloadRouteArrays performs a full reload of route arrays from Postgres
// into the STAGING buffer only, then atomically swaps it as the live buffer.
//
// Safety invariant: this function NEVER reads or writes routeBuffers[livePtr].
// It always targets the buffer at index (1 - livePtr), which RAPTOR goroutines
// are not currently reading.
//
// QUERY ORDER: ORDER BY t.route_id ASC, t.trip_id ASC, t.date ASC, s.stop_seq ASC
//   - t.date ASC is mandatory: without it, rows for the same trip_id on
//     different dates arrive non-deterministically, corrupting the TripIndex.
func ReloadRouteArrays(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT t.route_id, t.trip_id, t.date::text, s.stop_seq, s.station_id, s.departure_unix
		FROM   trips t
		JOIN   stop_times s ON s.trip_id = t.trip_id AND s.date = t.date
		ORDER BY t.route_id ASC, t.trip_id ASC, t.date ASC, s.stop_seq ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// ── Allocate a fresh staging buffer ──────────────────────────────────────
	// stagingIdx is the buffer NOT currently live — safe to write exclusively.
	stagingIdx := int32(1) - atomic.LoadInt32(&routeLivePtr)
	staging := &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
	}

	// ── Temporary in-memory grouping structures ───────────────────────────────
	type stopRecord struct {
		stationID     string
		departureUnix int64
	}
	type tripRecord struct {
		key     model.TripKey
		routeID string
		stops   []stopRecord
	}

	// routeID → ordered list of trips (order preserved from SQL ORDER BY)
	routeTrips := make(map[string][]*tripRecord)
	// routeOrder tracks insertion order so we can iterate deterministically
	var routeOrder []string
	routeSeen := make(map[string]bool)
	var currentTrip *tripRecord

	// ── Stream rows into grouped in-memory records ────────────────────────────
	for rows.Next() {
		var routeID, tripID, date, stationID string
		var stopSeq int
		var depUnix int64

		if err := rows.Scan(&routeID, &tripID, &date, &stopSeq, &stationID, &depUnix); err != nil {
			return err
		}

		key := model.TripKey{TripID: tripID, Date: date}

		// Start a new trip record whenever the key changes
		if currentTrip == nil || currentTrip.key != key {
			currentTrip = &tripRecord{key: key, routeID: routeID}
			routeTrips[routeID] = append(routeTrips[routeID], currentTrip)
			if !routeSeen[routeID] {
				routeSeen[routeID] = true
				routeOrder = append(routeOrder, routeID)
			}
		}

		currentTrip.stops = append(currentTrip.stops, stopRecord{
			stationID:     stationID,
			departureUnix: depUnix,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// ── Build staging buffer arrays in deterministic route order ─────────────
	for _, routeID := range routeOrder {
		trips := routeTrips[routeID]
		if len(trips) == 0 {
			continue
		}

		routeIdx := len(staging.Routes)

		// Derive canonical stop IDs from the first trip's stop sequence.
		// All trips on the same route share the same stop sequence.
		firstTrip := trips[0]
		stopIDs := make([]string, len(firstTrip.stops))
		for i, s := range firstTrip.stops {
			stopIDs[i] = s.stationID
		}

		// Collect trip keys for the RouteEntry
		tripKeys := make([]model.TripKey, len(trips))
		for i, t := range trips {
			tripKeys[i] = t.key
		}

		staging.Routes = append(staging.Routes, model.RouteEntry{
			RouteID:  routeID,
			StopIDs:  stopIDs,
			TripKeys: tripKeys,
		})

		// Build TripStopTimes for every trip on this route
		routeStopTimes := make([]model.TripStopTimes, len(trips))
		for ti, t := range trips {
			departures := make([]int64, len(t.stops))
			stationIDs := make([]string, len(t.stops))
			for si, s := range t.stops {
				departures[si] = s.departureUnix
				stationIDs[si] = s.stationID
			}
			routeStopTimes[ti] = model.TripStopTimes{
				Key:        t.key,
				Departures: departures,
				StationIDs: stationIDs,
			}

			// Index this trip: TripKey → (routeIdx, tripIdx) inside staging ONLY
			staging.TripIndex[t.key] = model.TripLocation{
				RouteIdx: routeIdx,
				TripIdx:  ti,
			}
		}
		staging.StopTimes = append(staging.StopTimes, routeStopTimes)

		// Build reverse index: stationID → [(routeIdx, stopPos)]
		for pos, sid := range stopIDs {
			staging.StopToRoutes[sid] = append(staging.StopToRoutes[sid],
				RouteStop{RouteIdx: routeIdx, StopPos: pos},
			)
		}
	}

	// ── Manifest check — skip atomic swap if data is identical ───────────────
	hash := manifestHash(staging)
	if hash == lastManifest {
		log.Println("[schedule] reload: manifest unchanged, skipping swap")
		return nil
	}
	lastManifest = hash

	// ── Atomic swap: install staging as the new live buffer ──────────────────
	// Write the fully-built staging pointer into the slot, then flip livePtr.
	// RAPTOR goroutines that captured the old LiveRoutes() pointer continue
	// reading stale-but-consistent data until their search completes.
	routeBuffers[stagingIdx] = staging
	atomic.StoreInt32(&routeLivePtr, stagingIdx)

	log.Printf("[schedule] reload: swapped in %d routes, %d trips (hash=%s…)",
		len(staging.Routes), len(staging.TripIndex), hash[:8])
	return nil
}
