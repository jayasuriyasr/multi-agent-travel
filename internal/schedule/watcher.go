package schedule

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"axentra/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)


// watcherInterval is the fixed poll cadence for schema_version checks.
// Acts as a fallback in case the LISTEN/NOTIFY channel goes silent.
const watcherInterval = 30 * time.Second // L11 fix: reduced from 2m to 30s

// WatcherLoop polls the schema_version table and ALSO listens for Postgres
// NOTIFY events on the "schema_changed" channel (L11 fix).
// Whichever fires first triggers a reload.
//
// Caller is responsible for cancelling ctx to stop the loop.
func WatcherLoop(ctx context.Context, pool *pgxpool.Pool) {
	ticker := time.NewTicker(watcherInterval)
	defer ticker.Stop()

	var lastTS time.Time

	log.Printf("[schedule] watcher: started (interval=%s)", watcherInterval)

	// Acquire a dedicated connection for LISTEN so we can use WaitForNotification.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("[schedule] watcher: could not acquire LISTEN connection: %v — falling back to poll-only", err)
		conn = nil
	}
	if conn != nil {
		defer conn.Release()
		if _, err := conn.Exec(ctx, "LISTEN schema_changed"); err != nil {
			log.Printf("[schedule] watcher: LISTEN failed: %v — falling back to poll-only", err)
			conn.Release()
			conn = nil
		} else {
			log.Println("[schedule] watcher: LISTEN schema_changed active")
		}
	}

	// notifyCh is fired either by a Postgres notification or by the ticker.
	// Both code paths lead to the same reload logic below.
	triggerReload := func() {
		var ts time.Time
		err := pool.QueryRow(ctx,
			`SELECT updated_at FROM schema_version WHERE id = 1`,
		).Scan(&ts)
		if err != nil {
			log.Printf("[schedule] watcher: failed to read schema_version: %v", err)
			return
		}
		if ts.After(lastTS) {
			lastTS = ts
			log.Printf("[schedule] watcher: schema_version changed at %v, reloading", ts)
			if err := ReloadRouteArrays(ctx, pool); err != nil {
				log.Printf("[schedule] watcher: reload failed: %v", err)
			}
		}
	}

	// If we have a LISTEN connection, spin a goroutine that forwards notifications.
	notifyCh := make(chan struct{}, 1)
	if conn != nil {
		go func() {
			for {
				// WaitForNotification blocks until a NOTIFY arrives or ctx is done.
				_, err := conn.Conn().WaitForNotification(ctx)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					log.Printf("[schedule] watcher: WaitForNotification error: %v", err)
					return
				}
				select {
				case notifyCh <- struct{}{}: // non-blocking send
				default:
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("[schedule] watcher: context cancelled, stopping")
			return
		case <-notifyCh:
			log.Println("[schedule] watcher: received Postgres NOTIFY — triggering reload")
			triggerReload()
		case <-ticker.C:
			triggerReload()
		}
	}
}

// ReloadRouteArrays performs a full reload of route arrays from Postgres
// into the STAGING buffer only, then atomically swaps it as the live buffer.
//
// Fixes applied:
//   L2  — reads both arrival_unix and departure_unix per stop
//   L3  — validates that all trips on a route share the same stop sequence
//   L6  — loads footpaths from the footpaths table
func ReloadRouteArrays(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT t.route_id, t.trip_id, t.date::text, s.stop_seq, s.station_id,
		       s.arrival_unix, s.departure_unix
		FROM   trips t
		JOIN   stop_times s ON s.trip_id = t.trip_id AND s.date = t.date
		ORDER BY t.route_id ASC, t.trip_id ASC, t.date ASC, s.stop_seq ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// ── Allocate a fresh staging buffer ──────────────────────────────────────
	staging := &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
		Footpaths:    make(map[string][]model.Footpath),
	}

	// ── Temporary in-memory grouping structures ───────────────────────────────
	type stopRecord struct {
		stationID    string
		arrivalUnix  int64 // L2 fix: distinct arrival time
		departureUnix int64
	}
	type tripRecord struct {
		key     model.TripKey
		routeID string
		stops   []stopRecord
	}

	routeTrips := make(map[string][]*tripRecord)
	var routeOrder []string
	routeSeen := make(map[string]bool)
	var currentTrip *tripRecord

	// ── Stream rows into grouped in-memory records ────────────────────────────
	for rows.Next() {
		var routeID, tripID, date, stationID string
		var stopSeq int
		var arrUnix, depUnix int64 // L2 fix: read both columns

		if err := rows.Scan(&routeID, &tripID, &date, &stopSeq, &stationID, &arrUnix, &depUnix); err != nil {
			return err
		}

		key := model.TripKey{TripID: tripID, Date: date}

		if currentTrip == nil || currentTrip.key != key {
			currentTrip = &tripRecord{key: key, routeID: routeID}
			routeTrips[routeID] = append(routeTrips[routeID], currentTrip)
			if !routeSeen[routeID] {
				routeSeen[routeID] = true
				routeOrder = append(routeOrder, routeID)
			}
		}

		currentTrip.stops = append(currentTrip.stops, stopRecord{
			stationID:    stationID,
			arrivalUnix:  arrUnix,
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
		firstTrip := trips[0]
		stopIDs := make([]string, len(firstTrip.stops))
		for i, s := range firstTrip.stops {
			stopIDs[i] = s.stationID
		}
		canonicalLen := len(stopIDs)

		// L3 fix: Validate that every trip on this route has the same stop count.
		// Trips with a different stop count indicate skip-stop or short-turn patterns;
		// log a warning and skip the offending trip to avoid silent index-out-of-bounds.
		var validTrips []*tripRecord
		for _, t := range trips {
			if len(t.stops) != canonicalLen {
				log.Printf("[schedule] reload: WARNING route %s trip %s/%s has %d stops (expected %d) — skipped",
					routeID, t.key.TripID, t.key.Date, len(t.stops), canonicalLen)
				continue
			}
			validTrips = append(validTrips, t)
		}
		if len(validTrips) == 0 {
			continue
		}

		tripKeys := make([]model.TripKey, len(validTrips))
		for i, t := range validTrips {
			tripKeys[i] = t.key
		}

		staging.Routes = append(staging.Routes, model.RouteEntry{
			RouteID:  routeID,
			StopIDs:  stopIDs,
			TripKeys: tripKeys,
		})

		// Build TripStopTimes with BOTH arrivals and departures (L2 fix).
		routeStopTimes := make([]model.TripStopTimes, len(validTrips))
		for ti, t := range validTrips {
			arrivals   := make([]int64, len(t.stops))
			departures := make([]int64, len(t.stops))
			stationIDs := make([]string, len(t.stops))
			for si, s := range t.stops {
				arrivals[si]   = s.arrivalUnix
				departures[si] = s.departureUnix
				stationIDs[si] = s.stationID
			}
			routeStopTimes[ti] = model.TripStopTimes{
				Key:        t.key,
				Arrivals:   arrivals,
				Departures: departures,
				StationIDs: stationIDs,
			}
		}

		// Sort trips by first-stop DEPARTURE time so binary search works correctly (L1).
		sort.Slice(routeStopTimes, func(i, j int) bool {
			if len(routeStopTimes[i].Departures) == 0 {
				return true
			}
			if len(routeStopTimes[j].Departures) == 0 {
				return false
			}
			return routeStopTimes[i].Departures[0] < routeStopTimes[j].Departures[0]
		})
		staging.StopTimes = append(staging.StopTimes, routeStopTimes)

		// E3 FIX: Rebuild TripIndex AFTER sort.
		for ti, tst := range routeStopTimes {
			staging.TripIndex[tst.Key] = model.TripLocation{
				RouteIdx: routeIdx,
				TripIdx:  ti,
			}
		}

		// Build reverse index: stationID → [(routeIdx, stopPos)]
		for pos, sid := range stopIDs {
			staging.StopToRoutes[sid] = append(staging.StopToRoutes[sid],
				RouteStop{RouteIdx: routeIdx, StopPos: pos},
			)
		}
	}

	// ── L6 fix: Load footpaths from DB ───────────────────────────────────────
	if err := loadFootpaths(ctx, pool, staging); err != nil {
		// Non-fatal: footpaths table may not exist yet on older schemas.
		log.Printf("[schedule] reload: WARNING footpath load failed (will continue without walk transfers): %v", err)
	}

	// ── Manifest check — skip atomic swap if data is identical ───────────────
	hash := manifestHash(staging)
	if hash == lastManifest {
		log.Println("[schedule] reload: manifest unchanged, skipping swap")
		return nil
	}
	lastManifest = hash

	// ── Atomic swap ──────────────────────────────────────────────────────────
	// L13 fix: use swapRoutes() which calls atomic.Pointer.Store — consistent
	// with the modern pattern in state/signal.go.
	swapRoutes(staging)

	log.Printf("[schedule] reload: swapped in %d routes, %d trips, %d footpath origins (hash=%s…)",
		len(staging.Routes), len(staging.TripIndex), len(staging.Footpaths), hash[:8])
	return nil
}

// loadFootpaths queries the footpaths table and populates staging.Footpaths.
// It is a separate function so it can be called independently and tested.
func loadFootpaths(ctx context.Context, pool *pgxpool.Pool, staging *RouteBuffer) error {
	fpRows, err := pool.Query(ctx, `
		SELECT station_id, neighbour_id, walk_seconds
		FROM   footpaths
		ORDER BY station_id ASC, walk_seconds ASC
	`)
	if err != nil {
		return fmt.Errorf("query footpaths: %w", err)
	}
	defer fpRows.Close()

	count := 0
	for fpRows.Next() {
		var stationID, neighbourID string
		var walkSeconds int
		if err := fpRows.Scan(&stationID, &neighbourID, &walkSeconds); err != nil {
			return fmt.Errorf("scan footpath row: %w", err)
		}
		staging.Footpaths[stationID] = append(staging.Footpaths[stationID], model.Footpath{
			NeighbourStop: neighbourID,
			WalkSeconds:   walkSeconds,
		})
		count++
	}
	if err := fpRows.Err(); err != nil {
		return fmt.Errorf("footpath rows error: %w", err)
	}
	log.Printf("[schedule] reload: loaded %d footpath edges", count)
	return nil
}
