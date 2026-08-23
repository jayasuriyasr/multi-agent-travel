// Package raptor implements the seat-aware RAPTOR search algorithm.
// RAPTOR (Round-bAsed Public Transit Optimized Router) finds Pareto-optimal
// paths that minimize both arrival time and number of transfers.
package raptor

import (
	"log"
	"math"
	"sort"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

const (
	// MaxRounds controls maximum transfers + 1 (4 rounds = up to 3 transfers).
	MaxRounds = 4
	infinity  = int64(math.MaxInt64)
)

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
		// No seat data for this trip — allow boarding optimistically.
		// This prevents 0-result searches when ColdStart hasn't populated all trips.
		return true
	}
	if sig.Stale {
		// Stale data — allow boarding but treat as uncertain.
		// Validation step (MGET) will verify freshness before returning to user.
		return true
	}
	return sig.ByClass[class] >= count
}

// journalEntry records a boarding event for path reconstruction.
type journalEntry struct {
	round       int
	station     string
	tripKey     model.TripKey
	routeID     string
	boardStop   string
	boardDepUnix int64
	arrivalUnix int64
}

// RaptorSearch performs a seat-aware RAPTOR traversal and returns up to topK
// Pareto-optimal paths from origin to destination.
//
// G11: buf and routes are captured ONCE here. Every canBoard call and route
// traversal uses these references. Never re-read LiveSignal() or LiveRoutes()
// inside the loop — a concurrent swap mid-search would mix snapshot generations.
func RaptorSearch(params model.SearchParams, topK int) []model.Path {
	// CAPTURE ONCE — the snapshot rule (G11)
	buf := state.LiveSignal()
	routes := schedule.LiveRoutes()

	if routes == nil || len(routes.Routes) == 0 {
		log.Printf("[raptor] search: no routes in buffer, returning nil")
		return nil
	}

	log.Printf("[raptor] search: origin=%s dest=%s date=%s depTime=%d routes=%d trips=%d signals=%d",
		params.Origin, params.Destination, params.Date,
		params.DepTime, len(routes.Routes), len(routes.TripIndex), len(*buf))

	// tau[round][stationID] = earliest known arrival time at that station in that round
	tau := make([]map[string]int64, MaxRounds+1)
	for r := 0; r <= MaxRounds; r++ {
		tau[r] = make(map[string]int64)
	}

	// Best known arrival time across all rounds
	bestArrival := make(map[string]int64)

	// Journal for path reconstruction (per round to maintain Pareto frontier)
	journal := make([]map[string]journalEntry, MaxRounds+1)
	for r := 0; r <= MaxRounds; r++ {
		journal[r] = make(map[string]journalEntry)
	}

	// Initialize: origin departs at the requested time
	tau[0][params.Origin] = params.DepTime
	bestArrival[params.Origin] = params.DepTime

	// Marked stops that were improved in the previous round
	marked := map[string]bool{params.Origin: true}

	for round := 1; round <= MaxRounds; round++ {
		// Collect (route, earliest boarding position) for routes touching marked stops
		type routeQueue struct {
			routeIdx int
			minPos   int
		}
		queue := make(map[int]int) // routeIdx → earliest stop position

		for station := range marked {
			for _, rs := range routes.StopToRoutes[station] {
				if existing, ok := queue[rs.RouteIdx]; !ok || rs.StopPos < existing {
					queue[rs.RouteIdx] = rs.StopPos
				}
			}
		}

		// Clear marked for this round
		newMarked := make(map[string]bool)

		// E1 FIX: Paper Algorithm 1 — ride-forward scan.
		// Outer loop iterates stops (not trips). At each stop we find the
		// earliest eligible trip (date + seat check). We ride that trip
		// forward, switching to a better trip whenever one is found at a
		// later stop. This is O(stops + trips) per route, not O(trips × stops).
		for routeIdx, boardPos := range queue {
			if routeIdx >= len(routes.StopTimes) {
				continue
			}
			routeTrips := routes.StopTimes[routeIdx]
			route := routes.Routes[routeIdx]

			if len(routeTrips) == 0 || len(route.StopIDs) == 0 {
				continue
			}

			// currentTrip is the trip we are currently riding (nil = not boarded yet).
			// boardStation/boardDep record where we boarded currentTrip.
			var currentTrip *model.TripStopTimes
			var boardStation string
			var boardDep int64

			for pos := boardPos; pos < len(route.StopIDs); pos++ {
				station := route.StopIDs[pos]

				// Step A: Propagate arrival from the trip we are currently riding.
				// Compare against bestArrival (τ* in the paper) for pruning.
				if currentTrip != nil && pos < len(currentTrip.Departures) {
					arrivalAtStop := currentTrip.Departures[pos]
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

				// Step B: Can we board / switch to an earlier eligible trip at
				// this stop? Uses bestArrival[station] = τ*(p), the minimum
				// arrival across all rounds — standard τ* optimisation from the
				// paper (Section 3.1). Departure of the current trip at this
				// position is our improvement threshold (infinity if not boarded).
				arrivalHere, arrived := bestArrival[station]
				if !arrived {
					continue
				}
				currentDep := int64(math.MaxInt64)
				if currentTrip != nil && pos < len(currentTrip.Departures) {
					currentDep = currentTrip.Departures[pos]
				}

				// Scan all trips for the earliest one that:
				//   (a) departs at or after our arrival at this stop,
				//   (b) departs strictly before the current trip (improvement),
				//   (c) is on the right calendar date,
				//   (d) has sufficient available seats (canBoard).
				// Trips are sorted by first-stop departure; since stop times within
				// a trip are monotonically increasing, this ordering holds at every
				// intermediate stop for the routes in this system.
				for ti := range routeTrips {
					tst := &routeTrips[ti]
					if pos >= len(tst.Departures) {
						continue
					}
					dep := tst.Departures[pos]
					if dep < arrivalHere {
						continue // cannot catch this trip at this stop
					}
					if dep >= currentDep {
						continue // not an improvement over what we are already riding
					}
					if !canBoard(buf, tst.Key, params.SeatClass, params.Passengers) {
						continue
					}
					// This trip is earlier and eligible — board / switch to it.
					currentTrip = tst
					boardStation = station
					boardDep = dep
					currentDep = dep // tighten threshold for remaining trips
				}
			}
		}

		// ── Footpath relaxation — RAPTOR paper Section 3.1 transfer step ──────
		// Snapshot the transit-improved stops BEFORE the walk pass.
		// Walk-reached stops (fp.NeighbourStop) are written into newMarked so
		// they become transit candidates in the next round, but they must NOT
		// expand their own footpaths within this same round — the paper only
		// allows one walk hop per round boundary.
		// Ranging over a snapshot (not newMarked directly) enforces this.
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
					newMarked[fp.NeighbourStop] = true // feeds NEXT round's transit step
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
			break // No improvements — early exit
		}
	}

	// Check if destination was reached
	if _, ok := bestArrival[params.Destination]; !ok {
		return nil
	}

	// Reconstruct paths from journal
	paths := reconstructPaths(journal, params)

	// Sort by arrival time, then by number of transfers
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].ArrivalUnix != paths[j].ArrivalUnix {
			return paths[i].ArrivalUnix < paths[j].ArrivalUnix
		}
		return paths[i].Transfers < paths[j].Transfers
	})

	if len(paths) > topK {
		paths = paths[:topK]
	}
	return paths
}

// reconstructPaths backtracks from the destination through the journal
// to build complete path objects.
func reconstructPaths(journal []map[string]journalEntry, params model.SearchParams) []model.Path {
	var paths []model.Path

	for r := 1; r <= MaxRounds; r++ {
		entry, ok := journal[r][params.Destination]
		if !ok {
			continue
		}

		var legs []model.Leg
		current := params.Destination
		currRound := r

		// Backtrack from destination to origin.
		// visited guards against infinite loops if a journal entry's boardStop
		// points back to a station already processed (data error or circular route).
		visited := make(map[string]bool)
		for currRound > 0 && current != params.Origin {
			if visited[current] {
				break // cycle detected — discard this malformed path
			}
			visited[current] = true
			e, exists := journal[currRound][current]
			if !exists {
				// If not found in current round, it means we waited at this station.
				// Find the earliest round where we actually reached this station.
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

			// E7 FIX: Leg-continuity guard.
			// The new leg alight point must equal the current station we are
			// backtracking from, and the next leg (if any) must board at the
			// same station the new leg alights. A mismatch means the journal
			// has an inconsistency (e.g. a circular route data error), so we
			// discard this path entirely rather than emit a "teleportation" leg.
			if len(legs) > 0 && legs[0].BoardStation != leg.AlightStation {
				legs = nil // discard — gap in the reconstructed path
				break
			}

			legs = append([]model.Leg{leg}, legs...)
			current = e.boardStop
			// D5 FIX: If the boardStop has a journal entry in the current round,
			// it means we took multiple legs within the same round (e.g. transit
			// then walk). We must NOT decrement currRound so that the next loop
			// iteration processes the preceding leg in this same round.
			// If it is not found here, it was reached in an earlier round, so we step back.
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
