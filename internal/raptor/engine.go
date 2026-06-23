// Package raptor implements the seat-aware RAPTOR search algorithm.
// RAPTOR (Round-bAsed Public Transit Optimized Router) finds Pareto-optimal
// paths that minimize both arrival time and number of transfers.
package raptor

import (
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

// canBoard checks whether a trip has sufficient non-stale seats for boarding.
func canBoard(buf *state.SignalBuffer, key model.TripKey, class string, count int) bool {
	sig, ok := (*buf)[key]
	if !ok {
		// If the trip doesn't exist in the seat buffer, we cannot board (pessimistic check).
		return false
	}
	return !sig.Stale && sig.ByClass[class] >= count
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
		return nil
	}

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

		for routeIdx, boardPos := range queue {
			if routeIdx >= len(routes.StopTimes) {
				continue
			}
			routeTrips := routes.StopTimes[routeIdx]
			route := routes.Routes[routeIdx]

			if len(routeTrips) == 0 || len(route.StopIDs) == 0 {
				continue
			}

			// For each trip on this route (sorted by departure time)
			// Try to find the earliest trip we can board at each stop
			for _, tst := range routeTrips {
				if len(tst.Departures) == 0 {
					continue
				}

				// Check if this trip can be boarded (seat availability)
				if !canBoard(buf, tst.Key, params.SeatClass, params.Passengers) {
					continue
				}

				// Only consider trips departing on the right date
				if tst.Key.Date != params.Date {
					continue
				}

				// Try boarding at each stop from boardPos onward
				boarded := false
				var boardStation string
				var boardDep int64

				for pos := boardPos; pos < len(tst.Departures) && pos < len(tst.StationIDs); pos++ {
					station := tst.StationIDs[pos]
					dep := tst.Departures[pos]

					if !boarded {
						// Can we board here? We need to have arrived before departure
						arrivalHere, arrived := bestArrival[station]
						if arrived && arrivalHere <= dep {
							boarded = true
							boardStation = station
							boardDep = dep
						}
						continue
					}

					// We're on the trip — check if this arrival improves our best
					arrivalAtStop := dep // departure time at this stop = arrival time
					currentBest, hasBest := bestArrival[station]

					if !hasBest || arrivalAtStop < currentBest {
						// Improvement found
						bestArrival[station] = arrivalAtStop
						tau[round][station] = arrivalAtStop
						newMarked[station] = true

						journal[round][station] = journalEntry{
							round:        round,
							station:      station,
							tripKey:      tst.Key,
							routeID:      route.RouteID,
							boardStop:    boardStation,
							boardDepUnix: boardDep,
							arrivalUnix:  arrivalAtStop,
						}
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

		// Backtrack from destination to origin
		for currRound > 0 && current != params.Origin {
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

			legs = append([]model.Leg{{
				TripID:        e.tripKey.TripID,
				Date:          e.tripKey.Date,
				RouteID:       e.routeID,
				BoardStation:  e.boardStop,
				AlightStation: current,
				DepartureUnix: e.boardDepUnix,
				ArrivalUnix:   e.arrivalUnix,
			}}, legs...)

			current = e.boardStop
			currRound--
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
