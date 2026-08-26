package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"axentra/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReloadRouteArrays rebuilds the entire route buffer from Postgres into a
// fresh STAGING buffer and, if the content actually changed, publishes it with
// a single atomic pointer swap. Readers never observe a partially built buffer.
//
// The load also derives two properties the search depends on:
//   - RouteFIFO, measured per route rather than assumed, which decides whether
//     the engine may use binary search for trip lookup.
//   - The transitive closure of the footpath graph, which RAPTOR's single-pass
//     footpath relaxation requires in order to find multi-hop walks. opts bounds
//     both how far it walks and how many edges a station keeps.
func ReloadRouteArrays(ctx context.Context, pool *pgxpool.Pool, opts LoadOptions) error {
	rows, err := pool.Query(ctx, `
		SELECT t.route_id, t.trip_id, t.date::text, s.stop_seq, s.station_id,
		       s.arrival_unix, s.departure_unix
		FROM   trips t
		JOIN   stop_times s ON s.trip_id = t.trip_id AND s.date = t.date
		ORDER BY t.route_id ASC, t.trip_id ASC, t.date ASC, s.stop_seq ASC
	`)
	if err != nil {
		return fmt.Errorf("query schedule: %w", err)
	}
	defer rows.Close()

	staging := newRouteBuffer()

	type stopRecord struct {
		stationID     string
		arrivalUnix   int64
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

	for rows.Next() {
		var routeID, tripID, date, stationID string
		var stopSeq int
		var arrUnix, depUnix int64

		if err := rows.Scan(&routeID, &tripID, &date, &stopSeq, &stationID, &arrUnix, &depUnix); err != nil {
			return fmt.Errorf("scan schedule row: %w", err)
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
			stationID:     stationID,
			arrivalUnix:   arrUnix,
			departureUnix: depUnix,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schedule rows: %w", err)
	}

	skippedTrips := 0

	for _, routeID := range routeOrder {
		trips := routeTrips[routeID]
		if len(trips) == 0 {
			continue
		}

		// The first trip defines the route's canonical stop sequence.
		stopIDs := make([]string, len(trips[0].stops))
		for i, s := range trips[0].stops {
			stopIDs[i] = s.stationID
		}
		canonicalLen := len(stopIDs)
		if canonicalLen == 0 {
			continue
		}

		// Trips whose stop pattern differs (skip-stop, short-turn) cannot share
		// this route's positional arrays. Drop them loudly rather than indexing
		// out of bounds later. Modelling them properly means splitting them into
		// their own route at ingestion time.
		valid := make([]*tripRecord, 0, len(trips))
		for _, t := range trips {
			if len(t.stops) != canonicalLen {
				slog.Warn("trip skipped: stop count differs from route",
					"route", routeID, "trip", t.key.TripID, "date", t.key.Date,
					"stops", len(t.stops), "expected", canonicalLen)
				skippedTrips++
				continue
			}
			mismatch := false
			for i, s := range t.stops {
				if s.stationID != stopIDs[i] {
					mismatch = true
					break
				}
			}
			if mismatch {
				slog.Warn("trip skipped: stop sequence differs from route",
					"route", routeID, "trip", t.key.TripID, "date", t.key.Date)
				skippedTrips++
				continue
			}
			valid = append(valid, t)
		}
		if len(valid) == 0 {
			continue
		}

		routeIdx := len(staging.Routes)

		routeStopTimes := make([]model.TripStopTimes, len(valid))
		for ti, t := range valid {
			arrivals := make([]int64, canonicalLen)
			departures := make([]int64, canonicalLen)
			stationIDs := make([]string, canonicalLen)
			for si, s := range t.stops {
				arrivals[si] = s.arrivalUnix
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

		// Sort by first departure, then by trip key so the order is total and
		// stable — an unstable order would make the manifest hash flap.
		sort.Slice(routeStopTimes, func(i, j int) bool {
			a, b := routeStopTimes[i], routeStopTimes[j]
			if a.Departures[0] != b.Departures[0] {
				return a.Departures[0] < b.Departures[0]
			}
			if a.Key.Date != b.Key.Date {
				return a.Key.Date < b.Key.Date
			}
			return a.Key.TripID < b.Key.TripID
		})

		fifo := computeFIFO(routeStopTimes)
		if !fifo {
			slog.Info("route is non-FIFO (trips overtake); using linear trip lookup",
				"route", routeID, "trips", len(routeStopTimes))
		}

		tripKeys := make([]model.TripKey, len(routeStopTimes))
		for ti, tst := range routeStopTimes {
			tripKeys[ti] = tst.Key
			// Built after the sort — indexes must point at final positions.
			staging.TripIndex[tst.Key] = model.TripLocation{RouteIdx: routeIdx, TripIdx: ti}
		}

		staging.Routes = append(staging.Routes, model.RouteEntry{
			RouteID:  routeID,
			StopIDs:  stopIDs,
			TripKeys: tripKeys,
		})
		staging.StopTimes = append(staging.StopTimes, routeStopTimes)
		staging.RouteFIFO = append(staging.RouteFIFO, fifo)

		for pos, sid := range stopIDs {
			staging.StopToRoutes[sid] = append(staging.StopToRoutes[sid],
				RouteStop{RouteIdx: routeIdx, StopPos: pos})
		}
	}

	direct, err := loadFootpaths(ctx, pool)
	if err != nil {
		// Non-fatal: the schedule is still usable without walk transfers.
		slog.Warn("footpath load failed; continuing without walk transfers", "error", err)
	}
	staging.Footpaths = closeFootpaths(direct, opts)

	hash := manifestHash(staging)
	if !manifestChanged(hash) {
		slog.Debug("reload skipped: schedule content unchanged")
		return nil
	}

	SwapRoutes(staging)

	st := staging.Stats()
	slog.Info("schedule published",
		"routes", st.Routes, "trips", st.Trips, "stations", st.Stations,
		"footpath_origins", st.FootpathOrigin, "non_fifo_routes", st.NonFIFORoutes,
		"skipped_trips", skippedTrips, "hash", hash[:12])
	return nil
}

// loadFootpaths reads the direct (one-hop) walk edges. The transitive closure
// is computed separately by closeFootpaths.
func loadFootpaths(ctx context.Context, pool *pgxpool.Pool) (map[string][]model.Footpath, error) {
	out := make(map[string][]model.Footpath)

	rows, err := pool.Query(ctx, `
		SELECT station_id, neighbour_id, walk_seconds
		FROM   footpaths
		ORDER BY station_id ASC, walk_seconds ASC
	`)
	if err != nil {
		return out, fmt.Errorf("query footpaths: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var stationID, neighbourID string
		var walkSeconds int
		if err := rows.Scan(&stationID, &neighbourID, &walkSeconds); err != nil {
			return out, fmt.Errorf("scan footpath row: %w", err)
		}
		if walkSeconds <= 0 || stationID == neighbourID {
			continue
		}
		out[stationID] = append(out[stationID], model.Footpath{
			NeighbourStop: neighbourID,
			WalkSeconds:   walkSeconds,
		})
		count++
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("iterate footpath rows: %w", err)
	}
	slog.Debug("footpaths loaded", "edges", count, "origins", len(out))
	return out, nil
}
