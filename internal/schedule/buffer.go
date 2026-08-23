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
	StopToRoutes map[string][]RouteStop // stationID → [(routeIdx, stopPos)]
	Footpaths    map[string][]model.Footpath // stationID → walkable neighbours (paper Section 3.1)
}

var (
	routeBuffers [2]*RouteBuffer
	routeLivePtr int32  // atomic: 0 or 1
	lastManifest string // hash of last successfully loaded manifest
)

func init() {
	routeBuffers[0] = &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
		Footpaths:    make(map[string][]model.Footpath),
	}
	routeBuffers[1] = &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
		Footpaths:    make(map[string][]model.Footpath),
	}
}

// LiveRoutes returns the current read-only route buffer.
// RAPTOR goroutines capture this ONCE at the start of a search (G11).
func LiveRoutes() *RouteBuffer {
	return routeBuffers[atomic.LoadInt32(&routeLivePtr)]
}

// swapRoutes atomically swaps in the staging buffer.
func swapRoutes(staging *RouteBuffer) {
	idx := 1 - atomic.LoadInt32(&routeLivePtr)
	routeBuffers[idx] = staging
	atomic.StoreInt32(&routeLivePtr, idx)
}

// manifestHash produces a deterministic hash of the buffer content
// to detect no-op ingestions and skip unnecessary swaps.
func manifestHash(buf *RouteBuffer) string {
	h := sha256.New()
	// Sort trip keys for deterministic hashing
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
