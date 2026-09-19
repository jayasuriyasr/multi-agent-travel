package raptor

import (
	"fmt"
	"math/rand"
	"sort"

	"axentra/internal/model"
)

// This file holds a deliberately naive reference router and a random network
// generator. Nothing here ships; it exists so differential_test.go can check
// the engine against something that shares no code with it.
//
// The reference has no rounds, no pruning, no marked stops, no route queue and
// no binary search. It relaxes every boarding at every labelled stop on every
// route at every level, to a fixed point. It is far too slow to use for real,
// which is exactly why agreement with it is worth something: the two
// implementations cannot be wrong in the same way by accident.

const refInfinity = int64(1) << 62

// refNet is a timetable in the shape both routers can consume.
type refNet struct {
	routes    []model.RouteEntry
	stopTimes [][]model.TripStopTimes
	walks     map[string][]model.Footpath // already transitively closed
}

// refWalkCap bounds the generated walk closure. Tests raise it when they want a
// metric (uncapped) closure, where no two-hop chain can ever beat a direct edge.
var refWalkCap = 1800

// refFrontier returns best[k] = the earliest arrival at dest using at most k
// transit legs, or refInfinity.
//
// It models the engine's SPECIFICATION, which differs from a textbook RAPTOR in
// two documented ways, so the comparison isolates implementation errors rather
// than re-litigating design choices:
//
//   - One label per stop, carrying how it was reached. The transfer buffer is
//     charged when that label came from a vehicle and not when it came from a
//     walk or the origin. A two-dimensional label (arrived-on-foot vs
//     arrived-by-vehicle) could sometimes leave earlier; the engine is
//     deliberately conservative and never proposes a connection tighter than
//     the operator's buffer.
//   - Footpaths are relaxed once per level, from the stops transit improved at
//     that level, using the labels as they stood before the pass. Walking twice
//     in a row is not allowed: the graph is closed, so a second hop is either
//     redundant or past MaxWalkSeconds.
//
// Everything about the SEARCH is independent: no rounds-as-optimisation, no
// route queue, no marked stops, no boarding-position restriction, no local or
// target pruning, no binary search. Every trip is tried at every position.
func refFrontier(n *refNet, origin, dest string, dep int64, rounds int, xfer int64) []int64 {
	type label struct {
		time    int64
		transit bool // reached on a vehicle, so a transfer buffer is owed
	}

	cur := map[string]label{origin: {time: dep}}
	for _, fp := range n.walks[origin] { // a journey may begin on foot
		v := dep + int64(fp.WalkSeconds)
		if l, ok := cur[fp.NeighbourStop]; !ok || v < l.time {
			cur[fp.NeighbourStop] = label{time: v}
		}
	}

	out := make([]int64, rounds+1)
	for i := range out {
		out[i] = refInfinity
	}
	if l, ok := cur[dest]; ok {
		out[0] = l.time
	}

	for k := 1; k <= rounds; k++ {
		prev := cur
		next := make(map[string]label, len(prev))
		for st, l := range prev {
			next[st] = l
		}
		transitImproved := map[string]bool{}

		// Every trip, boarded at every stop the passenger can reach, ridden to
		// every stop after it. Boarding always reads the frozen previous level.
		for ri, route := range n.routes {
			for _, trip := range n.stopTimes[ri] {
				for p := 0; p < len(route.StopIDs) && p < len(trip.Departures); p++ {
					l, ok := prev[route.StopIDs[p]]
					if !ok {
						continue
					}
					ready := l.time
					if l.transit {
						ready += xfer
					}
					if trip.Departures[p] < ready {
						continue
					}
					for q := p + 1; q < len(route.StopIDs) && q < len(trip.Arrivals); q++ {
						station := route.StopIDs[q]
						if c, ok := next[station]; !ok || trip.Arrivals[q] < c.time {
							next[station] = label{time: trip.Arrivals[q], transit: true}
							transitImproved[station] = true
						}
					}
				}
			}
		}

		// One walk hop, from the stops transit improved, off their labels as
		// they stood before any walk was applied.
		snapshot := make(map[string]int64, len(transitImproved))
		for st := range transitImproved {
			snapshot[st] = next[st].time
		}
		for st, depart := range snapshot {
			for _, fp := range n.walks[st] {
				w := depart + int64(fp.WalkSeconds)
				if c, ok := next[fp.NeighbourStop]; !ok || w < c.time {
					next[fp.NeighbourStop] = label{time: w, transit: false}
				}
			}
		}

		cur = next
		out[k] = out[k-1]
		if l, ok := cur[dest]; ok && l.time < out[k] {
			out[k] = l.time
		}
	}
	return out
}

// refPareto turns the per-level frontier into the (arrival, transfers) pairs
// the engine returns, in the engine's order.
func refPareto(f []int64) [][2]int64 {
	var out [][2]int64
	best := refInfinity
	for k, v := range f {
		if v >= best {
			continue
		}
		best = v
		transfers := int64(k - 1)
		if transfers < 0 {
			transfers = 0 // a walk-only journey has no transfers, not minus one
		}
		out = append(out, [2]int64{v, transfers})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	// A walk-only journey and a one-leg journey both report zero transfers, so
	// one of them can be dominated. The engine drops it; do the same.
	filtered := out[:0:0]
	bestTransfers := int64(1) << 30
	for _, e := range out {
		if e[1] < bestTransfers {
			bestTransfers = e[1]
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// trueFIFO is a full all-pairs overtaking check over BOTH arrivals and
// departures, written independently of computeFIFO so a test can catch that
// function mislabelling a route.
func trueFIFO(trips []model.TripStopTimes) bool {
	for i := range trips {
		for j := i + 1; j < len(trips); j++ {
			a, b := &trips[i], &trips[j]
			for p := 0; p < len(a.Departures) && p < len(b.Departures); p++ {
				if b.Departures[p] < a.Departures[p] {
					return false
				}
			}
			for p := 0; p < len(a.Arrivals) && p < len(b.Arrivals); p++ {
				if b.Arrivals[p] < a.Arrivals[p] {
					return false
				}
			}
		}
	}
	return true
}

// ─── Generators ─────────────────────────────────────────────────────────────

// randomFIFONet builds routes whose trips are one stopping pattern repeated at
// increasing headways: the non-overtaking precondition RAPTOR assumes, and what
// a loader produces once routes are split into patterns.
func randomFIFONet(rng *rand.Rand, nStations, nRoutes, maxTrips int, withWalks bool) (*refNet, []string) {
	return randomNetWith(rng, nStations, nRoutes, maxTrips, withWalks, true)
}

// randomOvertakingNet gives every trip independent times, so routes freely
// overtake themselves — the case the per-trip scan exists for.
func randomOvertakingNet(rng *rand.Rand, nStations, nRoutes, maxTrips int, withWalks bool) (*refNet, []string) {
	return randomNetWith(rng, nStations, nRoutes, maxTrips, withWalks, false)
}

func randomNetWith(rng *rand.Rand, nStations, nRoutes, maxTrips int, withWalks, fifo bool) (*refNet, []string) {
	stations := make([]string, nStations)
	for i := range stations {
		stations[i] = fmt.Sprintf("S%02d", i)
	}

	n := &refNet{walks: map[string][]model.Footpath{}}
	for r := 0; r < nRoutes; r++ {
		perm := rng.Perm(nStations)
		size := 2 + rng.Intn(nStations-1)
		if size > nStations {
			size = nStations
		}
		stops := make([]string, size)
		for i := range stops {
			stops[i] = stations[perm[i]]
		}

		nTrips := 1 + rng.Intn(maxTrips)
		trips := make([]model.TripStopTimes, 0, nTrips)

		if fifo {
			hop := make([]int64, size)
			dwell := make([]int64, size)
			for i := 0; i < size; i++ {
				hop[i] = int64(300 + rng.Intn(3600))
				dwell[i] = int64(rng.Intn(300))
			}
			base := at(6, 0) + int64(rng.Intn(3*3600))
			headway := int64(0)
			for tIdx := 0; tIdx < nTrips; tIdx++ {
				arr, dep := make([]int64, size), make([]int64, size)
				cur := base + headway
				for i := 0; i < size; i++ {
					arr[i] = cur
					dep[i] = cur + dwell[i]
					cur = dep[i] + hop[i]
				}
				trips = append(trips, makeTST(
					model.TripKey{TripID: fmt.Sprintf("R%dT%d", r, tIdx), Date: testDate}, stops, arr, dep))
				headway += int64(600 + rng.Intn(3600))
			}
		} else {
			for tIdx := 0; tIdx < nTrips; tIdx++ {
				arr, dep := make([]int64, size), make([]int64, size)
				cur := at(6, 0) + int64(rng.Intn(6*3600))
				for i := 0; i < size; i++ {
					arr[i] = cur
					dep[i] = cur + int64(rng.Intn(300))
					cur = dep[i] + int64(300+rng.Intn(3600))
				}
				trips = append(trips, makeTST(
					model.TripKey{TripID: fmt.Sprintf("R%dT%d", r, tIdx), Date: testDate}, stops, arr, dep))
			}
			// The loader always stores trips sorted by departure at stop 0.
			sort.SliceStable(trips, func(i, j int) bool {
				return trips[i].Departures[0] < trips[j].Departures[0]
			})
		}

		n.routes = append(n.routes, model.RouteEntry{RouteID: fmt.Sprintf("R%d", r), StopIDs: stops})
		n.stopTimes = append(n.stopTimes, trips)
	}

	if withWalks {
		direct := map[string][]model.Footpath{}
		for i := 0; i < nStations; i++ {
			for j := 0; j < nStations; j++ {
				if i != j && rng.Intn(10) == 0 {
					direct[stations[i]] = append(direct[stations[i]],
						model.Footpath{NeighbourStop: stations[j], WalkSeconds: 120 + rng.Intn(600)})
				}
			}
		}
		n.walks = refCloseWalks(direct, stations)
	}
	return n, stations
}

// refCloseWalks is a plain Floyd-Warshall closure, independent of the loader's.
func refCloseWalks(direct map[string][]model.Footpath, stations []string) map[string][]model.Footpath {
	d := map[string]map[string]int{}
	for _, s := range stations {
		d[s] = map[string]int{s: 0}
	}
	for from, es := range direct {
		for _, e := range es {
			if cur, ok := d[from][e.NeighbourStop]; !ok || e.WalkSeconds < cur {
				d[from][e.NeighbourStop] = e.WalkSeconds
			}
		}
	}
	for _, k := range stations {
		for _, i := range stations {
			ik, ok := d[i][k]
			if !ok {
				continue
			}
			for _, j := range stations {
				kj, ok := d[k][j]
				if !ok {
					continue
				}
				if cur, ok := d[i][j]; !ok || ik+kj < cur {
					d[i][j] = ik + kj
				}
			}
		}
	}
	out := map[string][]model.Footpath{}
	for _, i := range stations {
		for _, j := range stations {
			if i == j {
				continue
			}
			if w, ok := d[i][j]; ok && w <= refWalkCap {
				out[i] = append(out[i], model.Footpath{NeighbourStop: j, WalkSeconds: w})
			}
		}
		sort.Slice(out[i], func(a, b int) bool { return out[i][a].NeighbourStop < out[i][b].NeighbourStop })
	}
	return out
}

// publishRefNet installs a generated network as the live schedule.
func publishRefNet(n *refNet) {
	g := newGraph()
	for ri, route := range n.routes {
		g.addRoute(route.RouteID, route.StopIDs, n.stopTimes[ri], trueFIFO(n.stopTimes[ri]))
	}
	for from, es := range n.walks {
		for _, e := range es {
			g.walk(from, e.NeighbourStop, e.WalkSeconds)
		}
	}
	g.publish(nil)
}
