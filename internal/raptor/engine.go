// Package raptor implements the seat-aware RAPTOR search algorithm.
// RAPTOR (Round-bAsed Public Transit Optimized Router) finds Pareto-optimal
// paths that minimize both arrival time and number of transfers.
package raptor

import (
	"log"
	"math"
	"sort"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// maxRounds returns the effective round limit for the given params.
// L4 fix: MaxRounds is no longer a hardcoded constant. Callers can set
// SearchParams.MaxRounds for per-query control; the package default is used
// when it is 0.
func maxRounds(params model.SearchParams) int {
	if params.MaxRounds > 0 {
		return params.MaxRounds
	}
	return model.DefaultMaxRounds
}

const infinity = int64(math.MaxInt64)

// canBoard checks seat availability using the in-memory snapshot.
//
// TWO-LAYER DESIGN — do not make this pessimistic without understanding both layers:
//
//	Layer 1 (this function): OPTIMISTIC.
//	  - Missing key (no seat data yet) → allow boarding.
//	    Reason: ColdStart may not have populated all trips (first boot, partial
//	    Redis failure). Blocking here causes 0-result searches while warming up.
//	  - Stale signal → allow boarding.
//	    Reason: ValidateAndTruncate (validator.go) is the authoritative gate;
//	    this is only a fast pre-filter in RAM.
//
//	Layer 2 (ValidateAndTruncate in validator.go): PESSIMISTIC.
//	  - Missing key in Redis → REJECT path.
//	  - Insufficient seats in Redis → REJECT path.
//	  - This runs after RAPTOR and does a fresh MGET before returning results.
//
// The asymmetry is intentional. Do not "fix" layer 1 to be strict without
// understanding that layer 2 is the real enforcement point.
func canBoard(buf *state.SignalBuffer, key model.TripKey, class string, count int) bool {
	sig, ok := (*buf)[key]
	if !ok {
		return true // optimistic: no seat data yet (warming up)
	}
	if sig.Stale {
		return true // stale: validator will re-check via Redis MGET
	}
	return sig.ByClass[class] >= count
}

// journalEntry records a boarding/walking event for path reconstruction.
type journalEntry struct {
	round        int
	station      string
	tripKey      model.TripKey
	routeID      string
	boardStop    string
	boardDepUnix int64
	arrivalUnix  int64
}

// RaptorSearch performs a seat-aware RAPTOR traversal and returns up to topK
// Pareto-optimal paths from origin to destination.
//
// Fixes applied:
//   L1  — inner trip scan replaced with binary search (O(log T) per stop)
//   L2  — uses Arrivals[pos] for alighting, Departures[pos] for boarding only
//   L4  — MaxRounds read from SearchParams (runtime configurable)
//   L5  — date filter applied inside trip scan loop
//   L9  — path deduplication by leg fingerprint before return
//   L14 — per-search latency logged
func RaptorSearch(params model.SearchParams, topK int) []model.Path {
	start := time.Now() // L14: latency tracking

	// CAPTURE ONCE — the snapshot rule (G11)
	buf := state.LiveSignal()
	routes := schedule.LiveRoutes()

	if routes == nil || len(routes.Routes) == 0 {
		log.Printf("[raptor] search: no routes in buffer, returning nil")
		return nil
	}

	rounds := maxRounds(params) // L4 fix: configurable rounds

	log.Printf("[raptor] search: origin=%s dest=%s date=%s depTime=%d rounds=%d routes=%d trips=%d signals=%d",
		params.Origin, params.Destination, params.Date,
		params.DepTime, rounds, len(routes.Routes), len(routes.TripIndex), len(*buf))

	// tau[round][stationID] = earliest known arrival time at that station in that round
	tau := make([]map[string]int64, rounds+1)
	for r := 0; r <= rounds; r++ {
		tau[r] = make(map[string]int64)
	}

	// Best known arrival time across all rounds (τ* in the RAPTOR paper)
	bestArrival := make(map[string]int64)

	// Journal for path reconstruction (per round to maintain Pareto frontier)
	journal := make([]map[string]journalEntry, rounds+1)
	for r := 0; r <= rounds; r++ {
		journal[r] = make(map[string]journalEntry)
	}

	// Initialize: origin departs at the requested time
	tau[0][params.Origin] = params.DepTime
	bestArrival[params.Origin] = params.DepTime

	// Marked stops that were improved in the previous round
	marked := map[string]bool{params.Origin: true}

	for round := 1; round <= rounds; round++ {
		// Collect (route, earliest boarding position) for routes touching marked stops
		queue := make(map[int]int) // routeIdx → earliest stop position

		for station := range marked {
			for _, rs := range routes.StopToRoutes[station] {
				if existing, ok := queue[rs.RouteIdx]; !ok || rs.StopPos < existing {
					queue[rs.RouteIdx] = rs.StopPos
				}
			}
		}

		newMarked := make(map[string]bool)

		// ── Ride-forward scan ─────────────────────────────────────────────────
		// L1 fix: At each stop, find the earliest eligible trip using binary
		// search instead of a linear scan over all trips.
		// L2 fix: Use tst.Arrivals[pos] (not tst.Departures[pos]) for alighting.
		// L5 fix: Date filter applied inside findEarliestTrip.
		for routeIdx, boardPos := range queue {
			if routeIdx >= len(routes.StopTimes) {
				continue
			}
			routeTrips := routes.StopTimes[routeIdx]
			route := routes.Routes[routeIdx]

			if len(routeTrips) == 0 || len(route.StopIDs) == 0 {
				continue
			}

			var currentTrip *model.TripStopTimes
			var boardStation string
			var boardDep int64

			for pos := boardPos; pos < len(route.StopIDs); pos++ {
				station := route.StopIDs[pos]

				// Step A: Propagate arrival from the trip we are currently riding.
				// L2 fix: use Arrivals[pos] — the physical arrival time at this stop.
				if currentTrip != nil && pos < len(currentTrip.Arrivals) {
					arrivalAtStop := currentTrip.Arrivals[pos]
					currentBest, hasBest := bestArrival[station]
					if !hasBest || arrivalAtStop < currentBest {
						bestArrival[station] = arrivalAtStop
						tau[round][station] = arrivalAtStop
						newMarked[station] = true
						journal[round][station] = journalEntry{
							round:        round,
							station:      station,
							tripKey:      currentTrip.Key,
							routeID:      route.RouteID,
							boardStop:    boardStation,
							boardDepUnix: boardDep,
							arrivalUnix:  arrivalAtStop,
						}
					}
				}

				// Step B: Can we board / switch to an earlier eligible trip here?
				// L5 fix: findEarliestTrip applies the date filter.
				arrivalHere, arrived := bestArrival[station]
				if !arrived {
					continue
				}
				currentDep := infinity
				if currentTrip != nil && pos < len(currentTrip.Departures) {
					currentDep = currentTrip.Departures[pos]
				}

				// L1 fix: binary search for the earliest catchable trip at this stop.
				candidate := findEarliestTrip(routeTrips, pos, arrivalHere, currentDep, params, buf)
				if candidate != nil {
					currentTrip = candidate
					boardStation = station
					boardDep = currentTrip.Departures[pos]
					// tighten currentDep so we only switch if an even earlier trip exists
				}
			}
		}

		// ── Footpath relaxation ───────────────────────────────────────────────
		// Snapshot transit-improved stops BEFORE the walk pass so walk-reached
		// stops don't expand their own footpaths within the same round.
		transitImproved := make([]string, 0, len(newMarked))
		for s := range newMarked {
			transitImproved = append(transitImproved, s)
		}
		for _, station := range transitImproved {
			for _, fp := range routes.Footpaths[station] {
				arrViaWalk := tau[round][station] + int64(fp.WalkSeconds)
				cur, hasCur := bestArrival[fp.NeighbourStop]
				if !hasCur || arrViaWalk < cur {
					bestArrival[fp.NeighbourStop] = arrViaWalk
					tau[round][fp.NeighbourStop] = arrViaWalk
					newMarked[fp.NeighbourStop] = true
					journal[round][fp.NeighbourStop] = journalEntry{
						round:        round,
						station:      fp.NeighbourStop,
						routeID:      "WALK",
						boardStop:    station,
						boardDepUnix: tau[round][station],
						arrivalUnix:  arrViaWalk,
					}
				}
			}
		}

		marked = newMarked
		if len(marked) == 0 {
			break // no improvements — early exit
		}
	}

	// Check if destination was reached
	if _, ok := bestArrival[params.Destination]; !ok {
		log.Printf("[raptor] search: no path found (%s→%s) in %v", params.Origin, params.Destination, time.Since(start))
		return nil
	}

	// Reconstruct paths from journal
	paths := reconstructPaths(journal, params, rounds)

	// Sort: fastest arrival first, then fewest transfers
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].ArrivalUnix != paths[j].ArrivalUnix {
			return paths[i].ArrivalUnix < paths[j].ArrivalUnix
		}
		return paths[i].Transfers < paths[j].Transfers
	})

	// L9 fix: deduplicate paths by leg fingerprint (same physical journey can
	// appear in multiple rounds as RAPTOR discovers it at different transfer counts).
	paths = deduplicatePaths(paths)

	if len(paths) > topK {
		paths = paths[:topK]
	}

	// L14: log per-search latency
	log.Printf("[raptor] search: found %d paths (%s→%s date=%s) in %v",
		len(paths), params.Origin, params.Destination, params.Date, time.Since(start))

	return paths
}

// findEarliestTrip uses binary search to find the earliest trip on a route
// that can be caught at stop position `pos` (L1 fix).
//
// Preconditions:
//   - routeTrips is sorted by Departures[0] ascending (enforced by ReloadRouteArrays)
//   - The NON-OVERTAKING property must hold: if trip A departs before trip B at
//     stop 0, it also departs before trip B at every later stop. This is enforced
//     for real-world data by L3 (same stop count) but not validated explicitly.
//     Express trains that genuinely overtake locals on the same route will cause
//     incorrect binary-search results. Guard against this in future data loading.
//
// L5 fix: only trips whose TripKey.Date matches params.Date OR params.Date+1
// (for overnight journeys that cross midnight) are considered.
func findEarliestTrip(
	routeTrips []model.TripStopTimes,
	pos int,
	arrivalHere, currentDep int64,
	params model.SearchParams,
	buf *state.SignalBuffer,
) *model.TripStopTimes {

	// Pre-compute the allowed dates: search date and the following day
	// (for overnight trains that cross midnight).
	nextDate := overnightDate(params.Date)

	// Binary search: find the first trip whose departure at `pos` >= arrivalHere.
	// Relies on the non-overtaking property (see preconditions above).
	lo := sort.Search(len(routeTrips), func(i int) bool {
		if pos >= len(routeTrips[i].Departures) {
			return false
		}
		return routeTrips[i].Departures[pos] >= arrivalHere
	})

	// Scan forward from `lo` to find the earliest trip that passes date,
	// improvement threshold, and seat check.
	// NOTE: break on dep >= bestDep is valid ONLY under the non-overtaking
	// assumption. If that property fails, remove the break and use a full scan.
	var best *model.TripStopTimes
	bestDep := currentDep

	for i := lo; i < len(routeTrips); i++ {
		tst := &routeTrips[i]
		if pos >= len(tst.Departures) {
			continue
		}
		dep := tst.Departures[pos]
		if dep >= bestDep {
			// Non-overtaking: no later trip in the sorted list can depart earlier.
			break
		}
		// L5 fix: accept search date OR next calendar day (overnight crossings).
		if tst.Key.Date != params.Date && tst.Key.Date != nextDate {
			continue
		}
		if !canBoard(buf, tst.Key, params.SeatClass, params.Passengers) {
			continue
		}
		best = tst
		bestDep = dep
	}
	return best
}

// reconstructPaths backtracks from the destination through the journal
// to build complete path objects.
func reconstructPaths(journal []map[string]journalEntry, params model.SearchParams, rounds int) []model.Path {
	var paths []model.Path

	for r := 1; r <= rounds; r++ {
		entry, ok := journal[r][params.Destination]
		if !ok {
			continue
		}

		var legs []model.Leg
		current := params.Destination
		currRound := r

		// Backtrack from destination to origin.
		visited := make(map[string]bool)
		for currRound > 0 && current != params.Origin {
			if visited[current] {
				break // cycle detected — discard
			}
			visited[current] = true
			e, exists := journal[currRound][current]
			if !exists {
				// Station not found in current round — find the earliest round
				// where we actually arrived here (waited across round boundary).
				for pr := currRound - 1; pr > 0; pr-- {
					if pe, pExists := journal[pr][current]; pExists {
						e = pe
						currRound = pr
						exists = true
						break
					}
				}
				if !exists {
					break
				}
			}

			leg := model.Leg{
				TripID:        e.tripKey.TripID,
				Date:          e.tripKey.Date,
				RouteID:       e.routeID,
				BoardStation:  e.boardStop,
				AlightStation: current,
				DepartureUnix: e.boardDepUnix,
				ArrivalUnix:   e.arrivalUnix,
			}

			// L8 fix: leg-continuity guard.
			// When building backwards, legs[0] is the leg that immediately FOLLOWS
			// the current leg in forward direction. Its BoardStation must equal the
			// current leg's AlightStation.
			if len(legs) > 0 && legs[0].BoardStation != leg.AlightStation {
				legs = nil // gap — discard this path
				break
			}

			legs = append([]model.Leg{leg}, legs...)
			current = e.boardStop

			// D5 fix: if boardStop has a journal entry in the SAME round, we took
			// multiple legs in one round (transit+walk). Do NOT decrement currRound.
			if _, foundHere := journal[currRound][e.boardStop]; !foundHere {
				currRound--
			}
		}

		if len(legs) > 0 && current == params.Origin {
			paths = append(paths, model.Path{
				Legs:        legs,
				TotalTime:   entry.arrivalUnix - params.DepTime,
				Transfers:   len(legs) - 1,
				ArrivalUnix: entry.arrivalUnix,
			})
		}
	}

	return paths
}

// deduplicatePaths removes paths that represent the exact same physical journey.
// L9 fix: RAPTOR can find the same journey in multiple rounds (e.g. round 1 for
// a direct train AND round 2 for the same direct train + a skipped transfer).
// We fingerprint by the sequence of TripID+BoardStation+AlightStation.
// Uses a fresh allocation (not paths[:0]) to avoid sharing the underlying array
// with the caller's slice, which can cause subtle overwrites if paths is reused.
func deduplicatePaths(paths []model.Path) []model.Path {
	seen := make(map[string]bool, len(paths))
	out := make([]model.Path, 0, len(paths)) // fresh allocation — safe for callers
	for _, p := range paths {
		fp := pathFingerprint(p)
		if seen[fp] {
			continue
		}
		seen[fp] = true
		out = append(out, p)
	}
	return out
}

func pathFingerprint(p model.Path) string {
	b := make([]byte, 0, len(p.Legs)*40)
	for _, leg := range p.Legs {
		b = append(b, leg.TripID...)
		b = append(b, '|')
		b = append(b, leg.BoardStation...)
		b = append(b, '|')
		b = append(b, leg.AlightStation...)
		b = append(b, ';')
	}
	return string(b)
}

// overnightDate returns the YYYY-MM-DD string for the calendar day after `date`.
// Used by findEarliestTrip to accept trips that were seeded for the next day
// as part of an overnight journey crossing midnight.
// If date is malformed, returns date unchanged (safe fallback — no match occurs).
func overnightDate(date string) string {
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		return date // malformed date: no overnight match will happen
	}
	return t.AddDate(0, 0, 1).Format("2006-01-02")
}

