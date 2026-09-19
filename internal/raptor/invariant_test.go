package raptor

import (
	"fmt"
	"math/rand"
	"testing"

	"axentra/internal/model"
	"axentra/internal/schedule"
)

// Every journey the engine returns is checked back against the timetable it
// came from. A label can be right while the itinerary rebuilt from it is
// fiction, and only this direction catches that: the leg times must be times
// that actually appear in a trip's stop times, and the walk legs must be edges
// that actually exist in the footpath graph.

// checkJourney returns a description of the first invariant a path breaks.
func checkJourney(p model.Path, origin, dest string, depTime int64, rounds int, xfer int64) string {
	buf := schedule.LiveRoutes()
	if len(p.Legs) == 0 {
		return "journey has no legs"
	}

	if p.Legs[0].BoardStation != origin {
		return fmt.Sprintf("starts at %s, not the requested origin %s", p.Legs[0].BoardStation, origin)
	}
	if last := p.Legs[len(p.Legs)-1]; last.AlightStation != dest {
		return fmt.Sprintf("ends at %s, not the requested destination %s", last.AlightStation, dest)
	}
	if p.Legs[0].DepartureUnix < depTime {
		return fmt.Sprintf("first leg departs %d, before the requested %d", p.Legs[0].DepartureUnix, depTime)
	}

	transit, walkSeconds := 0, int64(0)
	var lastTransitArrival int64 = -1

	for i, leg := range p.Legs {
		if leg.DepartureUnix > leg.ArrivalUnix {
			return fmt.Sprintf("leg %d departs %d and arrives %d — backwards in time", i, leg.DepartureUnix, leg.ArrivalUnix)
		}
		if i > 0 {
			prev := p.Legs[i-1]
			if prev.AlightStation != leg.BoardStation {
				return fmt.Sprintf("leg %d alights at %s but leg %d boards at %s", i-1, prev.AlightStation, i, leg.BoardStation)
			}
			if leg.DepartureUnix < prev.ArrivalUnix {
				return fmt.Sprintf("leg %d departs %d before leg %d arrives %d", i, leg.DepartureUnix, i-1, prev.ArrivalUnix)
			}
			if prev.Kind == model.LegWalk && leg.Kind == model.LegWalk {
				return fmt.Sprintf("legs %d and %d are both walks — the footpath graph is closed, so this cannot be right", i-1, i)
			}
		}

		switch leg.Kind {
		case model.LegWalk:
			walkSeconds += leg.ArrivalUnix - leg.DepartureUnix
			found := false
			for _, fp := range buf.Footpaths[leg.BoardStation] {
				if fp.NeighbourStop == leg.AlightStation {
					found = true
					if got, want := leg.ArrivalUnix-leg.DepartureUnix, int64(fp.WalkSeconds); got != want {
						return fmt.Sprintf("leg %d walks %s→%s in %ds, but the footpath is %ds", i, leg.BoardStation, leg.AlightStation, got, want)
					}
				}
			}
			if !found {
				return fmt.Sprintf("leg %d walks %s→%s, which is not an edge in the footpath graph", i, leg.BoardStation, leg.AlightStation)
			}

		case model.LegTransit:
			transit++
			if lastTransitArrival >= 0 && leg.DepartureUnix < lastTransitArrival+xfer {
				// Only enforced when the previous leg was also transit; a walk
				// in between covers the change.
				if p.Legs[i-1].Kind == model.LegTransit {
					return fmt.Sprintf("leg %d departs %d, less than the %ds buffer after arriving %d", i, leg.DepartureUnix, xfer, lastTransitArrival)
				}
			}
			lastTransitArrival = leg.ArrivalUnix

			loc, ok := buf.TripIndex[model.TripKey{TripID: leg.TripID, Date: leg.Date}]
			if !ok {
				return fmt.Sprintf("leg %d names trip %s on %s, which is not in the schedule", i, leg.TripID, leg.Date)
			}
			route := buf.Routes[loc.RouteIdx]
			trip := buf.StopTimes[loc.RouteIdx][loc.TripIdx]
			if route.RouteID != leg.RouteID {
				return fmt.Sprintf("leg %d says route %s but trip %s runs on %s", i, leg.RouteID, leg.TripID, route.RouteID)
			}

			boardPos := -1
			for pos := range route.StopIDs {
				if pos < len(trip.Departures) &&
					route.StopIDs[pos] == leg.BoardStation &&
					trip.Departures[pos] == leg.DepartureUnix {
					boardPos = pos
					break
				}
			}
			if boardPos < 0 {
				return fmt.Sprintf("leg %d boards %s at %d, but trip %s does not depart there at that time", i, leg.BoardStation, leg.DepartureUnix, leg.TripID)
			}
			alightPos := -1
			for pos := boardPos + 1; pos < len(route.StopIDs); pos++ {
				if pos < len(trip.Arrivals) &&
					route.StopIDs[pos] == leg.AlightStation &&
					trip.Arrivals[pos] == leg.ArrivalUnix {
					alightPos = pos
					break
				}
			}
			if alightPos < 0 {
				return fmt.Sprintf("leg %d alights %s at %d, but trip %s does not arrive there at that time after position %d", i, leg.AlightStation, leg.ArrivalUnix, leg.TripID, boardPos)
			}

		default:
			return fmt.Sprintf("leg %d has unknown kind %q", i, leg.Kind)
		}
	}

	if transit > rounds {
		return fmt.Sprintf("journey uses %d vehicles, over the %d-round limit", transit, rounds)
	}
	wantTransfers := 0
	if transit > 0 {
		wantTransfers = transit - 1
	}
	if p.Transfers != wantTransfers {
		return fmt.Sprintf("reports %d transfers for %d transit legs, want %d", p.Transfers, transit, wantTransfers)
	}
	if p.ArrivalUnix != p.Legs[len(p.Legs)-1].ArrivalUnix {
		return fmt.Sprintf("path arrival %d does not match its last leg %d", p.ArrivalUnix, p.Legs[len(p.Legs)-1].ArrivalUnix)
	}
	if p.DepartureUnix != p.Legs[0].DepartureUnix {
		return fmt.Sprintf("path departure %d does not match its first leg %d", p.DepartureUnix, p.Legs[0].DepartureUnix)
	}
	if p.TotalTimeSeconds != p.ArrivalUnix-p.DepartureUnix {
		return fmt.Sprintf("total time %d does not match arrival minus departure %d", p.TotalTimeSeconds, p.ArrivalUnix-p.DepartureUnix)
	}
	if p.WalkSeconds != walkSeconds {
		return fmt.Sprintf("reports %ds of walking, legs add up to %ds", p.WalkSeconds, walkSeconds)
	}
	return ""
}

// checkFrontier verifies the result set is a strict Pareto frontier.
func checkFrontier(paths []model.Path) string {
	for i := 1; i < len(paths); i++ {
		if paths[i].ArrivalUnix <= paths[i-1].ArrivalUnix {
			return fmt.Sprintf("journey %d arrives %d, not after journey %d at %d", i, paths[i].ArrivalUnix, i-1, paths[i-1].ArrivalUnix)
		}
		if paths[i].Transfers >= paths[i-1].Transfers {
			return fmt.Sprintf("journey %d uses %d transfers, not fewer than journey %d at %d — it is dominated", i, paths[i].Transfers, i-1, paths[i-1].Transfers)
		}
	}
	return ""
}

// TestInvariants_EveryJourneyIsReal is the broadest single check in the suite.
func TestInvariants_EveryJourneyIsReal(t *testing.T) {
	iterations := 8000
	if testing.Short() {
		iterations = 500
	}
	rng := rand.New(rand.NewSource(31337))
	checked, journeys := 0, 0

	for it := 0; it < iterations; it++ {
		fifo := rng.Intn(2) == 0
		var n *refNet
		var stations []string
		nStations, nRoutes, nTrips := 3+rng.Intn(14), 1+rng.Intn(8), 1+rng.Intn(5)
		if fifo {
			n, stations = randomFIFONet(rng, nStations, nRoutes, nTrips, rng.Intn(2) == 0)
		} else {
			n, stations = randomOvertakingNet(rng, nStations, nRoutes, nTrips, rng.Intn(2) == 0)
		}
		publishRefNet(n)

		origin := stations[rng.Intn(len(stations))]
		dest := stations[rng.Intn(len(stations))]
		if origin == dest {
			continue
		}
		checked++

		rounds := 1 + rng.Intn(5)
		xfer := int64(rng.Intn(5) * 240)
		dep := at(0, 0) + int64(rng.Intn(24*3600))

		paths := query(origin, dest, dep, func(p *model.SearchParams) {
			p.MaxRounds = rounds
			p.MinTransferSeconds = int(xfer)
		})
		journeys += len(paths)

		if bad := checkFrontier(paths); bad != "" {
			t.Fatalf("iteration %d, %s → %s (rounds=%d, buffer=%ds): %s", it, origin, dest, rounds, xfer, bad)
		}
		for _, p := range paths {
			if bad := checkJourney(p, origin, dest, dep, rounds, xfer); bad != "" {
				t.Fatalf("iteration %d, %s → %s (rounds=%d, buffer=%ds): %s\n  legs: %s",
					it, origin, dest, rounds, xfer, bad, legSummary(p))
			}
		}
	}
	t.Logf("%d queries, %d journeys, every leg checked against the timetable", checked, journeys)
}

// More rounds can only ever help, and a later departure can only ever hurt.
// Both are properties of the algorithm, not of any particular network.
func TestInvariants_Monotonicity(t *testing.T) {
	iterations := 3000
	if testing.Short() {
		iterations = 200
	}
	// Departure monotonicity holds only when the footpath graph is metric.
	// The loader's MaxWalkSeconds cap deliberately truncates the closure, and a
	// truncated graph breaks it: a stop reached ON FOOT cannot walk onward (one
	// hop per round, by design), while the same stop reached BY TRAIN can. So
	// leaving later, taking a train to that stop instead of walking to it, can
	// unlock a walk that the earlier departure could not use. Measured at ~1 in
	// 2500 random queries; the independent reference reproduces it exactly, so
	// it is a property of the model, not of this implementation.
	defer func(prev int) { refWalkCap = prev }(refWalkCap)
	refWalkCap = 1 << 30

	rng := rand.New(rand.NewSource(4711))
	checkedRounds, checkedDep := 0, 0

	for it := 0; it < iterations; it++ {
		n, stations := randomFIFONet(rng, 4+rng.Intn(10), 1+rng.Intn(7), 1+rng.Intn(5), rng.Intn(2) == 0)
		publishRefNet(n)
		origin := stations[rng.Intn(len(stations))]
		dest := stations[rng.Intn(len(stations))]
		if origin == dest {
			continue
		}
		dep := at(6, 0)

		best := func(rounds int, d int64) int64 {
			paths := query(origin, dest, d, func(p *model.SearchParams) { p.MaxRounds = rounds })
			if len(paths) == 0 {
				return refInfinity
			}
			return paths[0].ArrivalUnix
		}

		// Raising the round limit must never push the best arrival later.
		checkedRounds++
		prev := best(1, dep)
		for r := 2; r <= 5; r++ {
			cur := best(r, dep)
			if cur > prev {
				t.Fatalf("iteration %d, %s → %s: max_rounds=%d arrives %d but max_rounds=%d arrived %d — more rounds made it worse",
					it, origin, dest, r, cur, r-1, prev)
			}
			prev = cur
		}

		// Leaving later must never get you there earlier.
		checkedDep++
		earlier := best(4, dep)
		later := best(4, dep+3600)
		if later < earlier {
			t.Fatalf("iteration %d, %s → %s: departing an hour later arrives %d, earlier than %d",
				it, origin, dest, later, earlier)
		}
	}
	t.Logf("%d round-monotonicity checks, %d departure-monotonicity checks", checkedRounds, checkedDep)
}

// The same query on the same data must give the same answer, every time.
// Go randomises map iteration, so this is a real risk, not a formality.
func TestInvariants_Determinism(t *testing.T) {
	rng := rand.New(rand.NewSource(8080))
	for it := 0; it < 300; it++ {
		n, stations := randomFIFONet(rng, 10, 6, 4, true)
		publishRefNet(n)
		origin := stations[rng.Intn(len(stations))]
		dest := stations[rng.Intn(len(stations))]
		if origin == dest {
			continue
		}

		first := query(origin, dest, at(6, 0), func(p *model.SearchParams) { p.MaxRounds = 4 })
		want := make([]string, len(first))
		for i, p := range first {
			want[i] = fmt.Sprintf("%d/%d/%s", p.ArrivalUnix, p.Transfers, legSummary(p))
		}
		for rep := 0; rep < 12; rep++ {
			again := query(origin, dest, at(6, 0), func(p *model.SearchParams) { p.MaxRounds = 4 })
			if len(again) != len(want) {
				t.Fatalf("iteration %d repeat %d: %d journeys, first run gave %d", it, rep, len(again), len(want))
			}
			for i, p := range again {
				got := fmt.Sprintf("%d/%d/%s", p.ArrivalUnix, p.Transfers, legSummary(p))
				if got != want[i] {
					t.Fatalf("iteration %d repeat %d journey %d:\n got  %s\n want %s", it, rep, i, got, want[i])
				}
			}
		}
	}
}
