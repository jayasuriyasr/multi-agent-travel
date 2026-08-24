// Package schedule owns the double-buffered route arrays and the schema
// version watcher. The live buffer is read-only for RAPTOR goroutines;
// the staging buffer is populated during reload and swapped atomically.
//
// G3: Go maps are NOT safe for concurrent read+write — the double-buffer
// pattern prevents a fatal panic by ensuring readers and writers never
// touch the same buffer.
package schedule

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"axentra/internal/model"
)

// RouteStop maps a station to its position within a specific route.
type RouteStop struct {
	RouteIdx int
	StopPos  int
}

// RouteBuffer holds the complete schedule snapshot used by RAPTOR searches.
type RouteBuffer struct {
	Routes       []model.RouteEntry
	StopTimes    [][]model.TripStopTimes // [routeIdx][tripIdx]
	TripIndex    map[model.TripKey]model.TripLocation
	StopToRoutes map[string][]RouteStop           // stationID → [(routeIdx, stopPos)]
	Footpaths    map[string][]model.Footpath       // stationID → walkable neighbours (L6)
}

// L13 fix: use atomic.Pointer[RouteBuffer] for type-safe, race-free pointer swaps,
// matching the pattern already used by state.SignalBuffer in state/signal.go.
var liveRoutePtr atomic.Pointer[RouteBuffer]

// lastManifest is the hash of the last successfully loaded manifest.
// Used to skip no-op reloads when data hasn't changed.
var lastManifest string


func init() {
	initial := &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
		Footpaths:    make(map[string][]model.Footpath),
	}
	liveRoutePtr.Store(initial)
}

// LiveRoutes returns the current read-only route buffer.
// RAPTOR goroutines capture this ONCE at the start of a search (G11).
func LiveRoutes() *RouteBuffer {
	return liveRoutePtr.Load()
}

// swapRoutes atomically installs a new staging buffer as the live buffer.
func swapRoutes(staging *RouteBuffer) {
	liveRoutePtr.Store(staging)
}

// manifestHash produces a deterministic hash of the buffer content
// to detect no-op ingestions and skip unnecessary swaps.
func manifestHash(buf *RouteBuffer) string {
	h := sha256.New()
	keys := make([]string, 0, len(buf.TripIndex))
	for k := range buf.TripIndex {
		keys = append(keys, fmt.Sprintf("%s:%s", k.TripID, k.Date))
	}
	sort.Strings(keys)
	h.Write([]byte(strings.Join(keys, "|")))

	for ri, routeTrips := range buf.StopTimes {
		for ti, tst := range routeTrips {
			fmt.Fprintf(h, "R%dT%d:", ri, ti)
			for _, d := range tst.Departures {
				fmt.Fprintf(h, "%d,", d)
			}
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// GetTripDeparture returns the first departure unix timestamp for a trip,
// using the in-memory route buffer. Returns 0 if the trip is not found.
func GetTripDeparture(tripID, date string) int64 {
	buf := LiveRoutes()
	key := model.TripKey{TripID: tripID, Date: date}
	loc, ok := buf.TripIndex[key]
	if !ok {
		return 0
	}
	if loc.RouteIdx < len(buf.StopTimes) && loc.TripIdx < len(buf.StopTimes[loc.RouteIdx]) {
		deps := buf.StopTimes[loc.RouteIdx][loc.TripIdx].Departures
		if len(deps) > 0 {
			return deps[0]
		}
	}
	return 0
}
