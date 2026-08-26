// Package schedule owns the double-buffered route arrays and the schema
// version watcher. The live buffer is read-only for RAPTOR goroutines; a
// staging buffer is built during reload and swapped in atomically.
//
// Go maps are NOT safe for concurrent read+write — a racing read and write is
// a fatal runtime panic that recover() cannot catch. The double-buffer pattern
// guarantees readers and writers never touch the same map.
package schedule

import (
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"axentra/internal/model"
)

// RouteStop maps a station to its position within a specific route.
type RouteStop struct {
	RouteIdx int
	StopPos  int
}

// RouteBuffer holds the complete schedule snapshot used by RAPTOR searches.
// Every field is immutable once the buffer has been published via SwapRoutes.
type RouteBuffer struct {
	Routes       []model.RouteEntry
	StopTimes    [][]model.TripStopTimes // [routeIdx][tripIdx]
	TripIndex    map[model.TripKey]model.TripLocation
	StopToRoutes map[string][]RouteStop // stationID → [(routeIdx, stopPos)]

	// Footpaths is the TRANSITIVELY CLOSED walk graph: Footpaths[a] contains
	// every station reachable from a on foot within MaxWalkSeconds, with the
	// shortest total walk time. Closure is required for RAPTOR's single-pass
	// footpath relaxation to be correct.
	Footpaths map[string][]model.Footpath

	// RouteFIFO[routeIdx] reports whether the route's trips are non-overtaking:
	// a trip that departs earlier at stop 0 also departs earlier at every later
	// stop. FIFO routes get a binary-search trip lookup; non-FIFO routes fall
	// back to a linear scan that makes no ordering assumption. Computed at load
	// time so correctness never depends on an unverified precondition.
	RouteFIFO []bool
}

// Stats summarises a buffer for logging and the readiness endpoint.
type Stats struct {
	Routes         int `json:"routes"`
	Trips          int `json:"trips"`
	Stations       int `json:"stations"`
	FootpathOrigin int `json:"footpath_origins"`
	NonFIFORoutes  int `json:"non_fifo_routes"`
}

// Stats returns a snapshot summary of the buffer.
func (b *RouteBuffer) Stats() Stats {
	s := Stats{
		Routes:         len(b.Routes),
		Trips:          len(b.TripIndex),
		Stations:       len(b.StopToRoutes),
		FootpathOrigin: len(b.Footpaths),
	}
	for _, ok := range b.RouteFIFO {
		if !ok {
			s.NonFIFORoutes++
		}
	}
	return s
}

// IsFIFO reports whether route routeIdx is safe for binary-search trip lookup.
// Unknown routes are treated as non-FIFO — the conservative answer.
func (b *RouteBuffer) IsFIFO(routeIdx int) bool {
	if routeIdx < 0 || routeIdx >= len(b.RouteFIFO) {
		return false
	}
	return b.RouteFIFO[routeIdx]
}

// newRouteBuffer allocates an empty buffer with every map initialised, so a
// nil-map write can never panic.
func newRouteBuffer() *RouteBuffer {
	return &RouteBuffer{
		TripIndex:    make(map[model.TripKey]model.TripLocation),
		StopToRoutes: make(map[string][]RouteStop),
		Footpaths:    make(map[string][]model.Footpath),
	}
}

// liveRoutePtr is the published buffer. atomic.Pointer gives type-safe,
// race-free swaps with zero locking on the read path.
var liveRoutePtr atomic.Pointer[RouteBuffer]

// manifest guards the last-loaded content hash. ReloadRouteArrays can be
// called from the boot path, the watcher goroutine and the ingestion path, so
// the hash needs its own mutex — a plain package-level string would race.
var manifest struct {
	sync.Mutex
	hash string
}

func init() { liveRoutePtr.Store(newRouteBuffer()) }

// LiveRoutes returns the current read-only route buffer.
// RAPTOR goroutines capture this ONCE at the start of a search.
func LiveRoutes() *RouteBuffer { return liveRoutePtr.Load() }

// SwapRoutes atomically installs a staging buffer as the live buffer.
// Exported so tests in other packages can inject a mocked schedule.
func SwapRoutes(staging *RouteBuffer) { liveRoutePtr.Store(staging) }

// ResetManifest clears the cached content hash, forcing the next reload to
// publish even if the data is unchanged. Used by tests and by SwapRoutes
// callers that bypass the normal reload path.
func ResetManifest() {
	manifest.Lock()
	manifest.hash = ""
	manifest.Unlock()
}

// manifestChanged compares hash against the last published hash, storing it
// when different. It returns true when the caller should publish.
func manifestChanged(hash string) bool {
	manifest.Lock()
	defer manifest.Unlock()
	if manifest.hash == hash {
		return false
	}
	manifest.hash = hash
	return true
}

// manifestHash produces a deterministic content hash of everything a search
// can observe, so an unchanged reload can skip the swap.
//
// Every field the engine reads must be hashed. The previous implementation
// covered only trip keys and departure times, which meant a reload that fixed
// arrival times or changed the footpath graph was silently discarded as a
// "no-op" and never reached memory.
func manifestHash(buf *RouteBuffer) string {
	h := sha256.New()
	var scratch [8]byte
	putInt := func(v int64) {
		binary.BigEndian.PutUint64(scratch[:], uint64(v))
		h.Write(scratch[:])
	}
	putStr := func(s string) {
		putInt(int64(len(s)))
		h.Write([]byte(s))
	}

	// Routes, their stop sequences, and every trip's arrivals AND departures,
	// walked in stored (deterministic) order.
	putInt(int64(len(buf.Routes)))
	for ri, route := range buf.Routes {
		putStr(route.RouteID)
		putInt(int64(len(route.StopIDs)))
		for _, s := range route.StopIDs {
			putStr(s)
		}
		if ri >= len(buf.StopTimes) {
			continue
		}
		putInt(int64(len(buf.StopTimes[ri])))
		for _, tst := range buf.StopTimes[ri] {
			putStr(tst.Key.TripID)
			putStr(tst.Key.Date)
			putInt(int64(len(tst.Arrivals)))
			for i := range tst.Arrivals {
				putInt(tst.Arrivals[i])
			}
			putInt(int64(len(tst.Departures)))
			for i := range tst.Departures {
				putInt(tst.Departures[i])
			}
		}
	}

	// Footpaths, in sorted key order (map iteration is randomised).
	fpKeys := make([]string, 0, len(buf.Footpaths))
	for k := range buf.Footpaths {
		fpKeys = append(fpKeys, k)
	}
	sort.Strings(fpKeys)
	putInt(int64(len(fpKeys)))
	for _, k := range fpKeys {
		putStr(k)
		edges := buf.Footpaths[k]
		putInt(int64(len(edges)))
		for _, e := range edges {
			putStr(e.NeighbourStop)
			putInt(int64(e.WalkSeconds))
		}
	}

	return string(hexBytes(h.Sum(nil)))
}

func hexBytes(b []byte) []byte {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return out
}

// GetTripDeparture returns the first departure unix timestamp for a trip from
// the in-memory buffer. Returns 0 when the trip is not loaded.
func GetTripDeparture(tripID, date string) int64 {
	return LiveRoutes().TripDeparture(model.TripKey{TripID: tripID, Date: date})
}

// TripDeparture returns a trip's first-stop departure time, or 0 if unknown.
func (b *RouteBuffer) TripDeparture(key model.TripKey) int64 {
	loc, ok := b.TripIndex[key]
	if !ok {
		return 0
	}
	if loc.RouteIdx >= len(b.StopTimes) || loc.TripIdx >= len(b.StopTimes[loc.RouteIdx]) {
		return 0
	}
	deps := b.StopTimes[loc.RouteIdx][loc.TripIdx].Departures
	if len(deps) == 0 {
		return 0
	}
	return deps[0]
}

// computeFIFO reports whether trips (already sorted by first departure) are
// non-overtaking at every stop position.
func computeFIFO(trips []model.TripStopTimes) bool {
	for i := 1; i < len(trips); i++ {
		prev, curr := trips[i-1].Departures, trips[i].Departures
		n := len(curr)
		if len(prev) < n {
			n = len(prev)
		}
		for pos := 0; pos < n; pos++ {
			if curr[pos] < prev[pos] {
				return false
			}
		}
	}
	return true
}

// ── Footpath closure ─────────────────────────────────────────────────────────

const (
	// DefaultMaxWalkSeconds bounds how far the transitive closure will walk.
	DefaultMaxWalkSeconds = 1800 // 30 minutes

	// DefaultMaxFootpathsPerStation caps how many closed walk edges a single
	// station keeps, nearest first.
	//
	// This is the bound that matters most. Closure output is
	// O(stations × neighbourhood size), and those edges are re-walked on every
	// footpath relaxation of every round of every search — so an unbounded
	// closure over a dense interchange district taxes each query, not just the
	// load. Generous enough never to fire on real transfer data; low enough
	// that pathological input cannot turn one search into thousands of edge
	// relaxations.
	DefaultMaxFootpathsPerStation = 64
)

// LoadOptions tunes how a schedule reload builds the buffer.
type LoadOptions struct {
	// MaxWalkSeconds bounds the transitive footpath closure.
	MaxWalkSeconds int
	// MaxFootpathsPerStation caps closed walk edges per station, nearest first.
	MaxFootpathsPerStation int
}

// DefaultLoadOptions returns the built-in tuning.
func DefaultLoadOptions() LoadOptions {
	return LoadOptions{
		MaxWalkSeconds:         DefaultMaxWalkSeconds,
		MaxFootpathsPerStation: DefaultMaxFootpathsPerStation,
	}
}

// withDefaults fills in any unset field, so a zero LoadOptions is usable.
func (o LoadOptions) withDefaults() LoadOptions {
	if o.MaxWalkSeconds <= 0 {
		o.MaxWalkSeconds = DefaultMaxWalkSeconds
	}
	if o.MaxFootpathsPerStation <= 0 {
		o.MaxFootpathsPerStation = DefaultMaxFootpathsPerStation
	}
	return o
}

// walkNode is one entry in the closure's priority queue.
type walkNode struct {
	stop string
	dist int
}

// walkQueue is a typed binary min-heap of walkNode.
//
// Two reasons it is hand-rolled rather than using container/heap:
//
//   - Dijkstra without a priority queue at all is O(V²) per source. On a sparse
//     suburban walk graph that is genuinely fine, which is exactly why it is
//     easy to ship — and then someone loads a transfer table for a city where
//     hundreds of stations sit inside one walking radius and the cost lands at
//     load time with no warning. Measured on a graph where 800 stations all
//     reach one interchange on foot: 12.3s with a scan for the minimum against
//     309ms with a heap.
//   - container/heap takes `any`, so every Push boxes the node onto the heap.
//     That was 1.3M allocations and 47MB of garbage on the same graph. A typed
//     heap is twenty lines and allocates nothing per push.
type walkQueue []walkNode

func (q *walkQueue) push(n walkNode) {
	*q = append(*q, n)
	i := len(*q) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if (*q)[parent].dist <= (*q)[i].dist {
			break
		}
		(*q)[parent], (*q)[i] = (*q)[i], (*q)[parent]
		i = parent
	}
}

func (q *walkQueue) pop() walkNode {
	old := *q
	top := old[0]
	last := len(old) - 1
	old[0] = old[last]
	*q = old[:last]

	i, n := 0, last
	for {
		left, right := 2*i+1, 2*i+2
		smallest := i
		if left < n && (*q)[left].dist < (*q)[smallest].dist {
			smallest = left
		}
		if right < n && (*q)[right].dist < (*q)[smallest].dist {
			smallest = right
		}
		if smallest == i {
			break
		}
		(*q)[i], (*q)[smallest] = (*q)[smallest], (*q)[i]
		i = smallest
	}
	return top
}

// closeFootpaths computes the transitive closure of the walk graph with
// shortest walk times, bounded by opts.MaxWalkSeconds and truncated to
// opts.MaxFootpathsPerStation edges per station.
//
// RAPTOR relaxes footpaths in a single pass per round, so the walk graph must
// already be transitively closed: without closure a two-hop walk (A→B→C) is
// simply never found. Closure is computed once at load rather than paid for on
// every search.
//
// Implemented as a bounded Dijkstra per source with a binary heap and lazy
// deletion — stale queue entries are skipped on pop rather than decreased in
// place, which keeps the code short without changing the complexity.
func closeFootpaths(direct map[string][]model.Footpath, opts LoadOptions) map[string][]model.Footpath {
	opts = opts.withDefaults()
	if len(direct) == 0 {
		return map[string][]model.Footpath{}
	}

	sources := make([]string, 0, len(direct))
	for s := range direct {
		sources = append(sources, s)
	}
	sort.Strings(sources)

	out := make(map[string][]model.Footpath, len(direct))

	// Reused across sources: the closure runs once per station, and these would
	// otherwise be two fresh maps and a fresh heap on every iteration.
	dist := make(map[string]int)
	settled := make(map[string]bool)
	pq := make(walkQueue, 0, 64)
	truncated := 0

	for _, src := range sources {
		clear(dist)
		clear(settled)
		pq = pq[:0]

		dist[src] = 0
		pq.push(walkNode{stop: src, dist: 0})

		for len(pq) > 0 {
			node := pq.pop()
			if settled[node.stop] {
				continue // a shorter route to this stop was already settled
			}
			if d, ok := dist[node.stop]; ok && node.dist > d {
				continue // stale entry left behind by lazy deletion
			}
			settled[node.stop] = true

			for _, e := range direct[node.stop] {
				nd := node.dist + e.WalkSeconds
				if nd > opts.MaxWalkSeconds {
					continue
				}
				if cur, ok := dist[e.NeighbourStop]; ok && nd >= cur {
					continue
				}
				dist[e.NeighbourStop] = nd
				pq.push(walkNode{stop: e.NeighbourStop, dist: nd})
			}
		}

		if len(dist) <= 1 {
			continue // only the source itself
		}

		edges := make([]model.Footpath, 0, len(dist)-1)
		for stop, d := range dist {
			if stop == src {
				continue // never walk to where you already are
			}
			edges = append(edges, model.Footpath{NeighbourStop: stop, WalkSeconds: d})
		}

		// Deterministic order: shortest walk first, then station ID.
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].WalkSeconds != edges[j].WalkSeconds {
				return edges[i].WalkSeconds < edges[j].WalkSeconds
			}
			return edges[i].NeighbourStop < edges[j].NeighbourStop
		})

		if len(edges) > opts.MaxFootpathsPerStation {
			edges = edges[:opts.MaxFootpathsPerStation]
			truncated++
		}
		out[src] = edges
	}

	if truncated > 0 {
		slog.Warn("footpath closure truncated: some stations reach more walk neighbours than the cap",
			"stations", truncated, "cap", opts.MaxFootpathsPerStation,
			"max_walk_seconds", opts.MaxWalkSeconds)
	}
	return out
}
