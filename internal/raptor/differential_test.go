package raptor

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"axentra/internal/model"
)

// Differential tests: generate a timetable, ask the engine and the brute-force
// reference in reference_test.go the same question, and require identical
// Pareto frontiers. This is the check that catches an error a hand-written
// fixture would not think to look for.

func engineFrontier(origin, dest string, dep int64, rounds int, xfer int64) [][2]int64 {
	paths := RaptorSearch(context.Background(), model.SearchParams{
		Origin: origin, Destination: dest, Date: testDate, DepTime: dep,
		SeatClass: "lower", Passengers: 1,
		MaxRounds: rounds, MinTransferSeconds: int(xfer),
	}, 100)
	out := make([][2]int64, 0, len(paths))
	for _, p := range paths {
		out = append(out, [2]int64{p.ArrivalUnix, int64(p.Transfers)})
	}
	return out
}

func formatFrontier(f [][2]int64) string {
	s := "["
	for i, v := range f {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("(arr=%d, transfers=%d)", v[0], v[1])
	}
	return s + "]"
}

func sameFrontier(a, b [][2]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDifferential_FIFONetworks is the main correctness argument for the
// engine: thousands of randomised queries over the network shape RAPTOR
// assumes, every frontier compared entry by entry.
func TestDifferential_FIFONetworks(t *testing.T) {
	iterations := 6000
	if testing.Short() {
		iterations = 400
	}
	// Sweep the walk-distance cap. A tight cap truncates the closure, so the
	// footpath graph stops being metric and a two-hop walk is no longer implied
	// by a one-hop edge — which is exactly the condition that used to hide the
	// chaining defect. The reference models the same one-hop rule, so the two
	// must still agree at every cap.
	defer func(prev int) { refWalkCap = prev }(refWalkCap)
	caps := []int{600, 1800, 1 << 30}

	rng := rand.New(rand.NewSource(20260907))

	checked, withJourney, mismatches := 0, 0, 0
	for it := 0; it < iterations; it++ {
		refWalkCap = caps[it%len(caps)]
		n, stations := randomFIFONet(rng,
			4+rng.Intn(16), // stations
			1+rng.Intn(10), // routes
			1+rng.Intn(6),  // trips per route
			rng.Intn(2) == 0)
		publishRefNet(n)

		origin := stations[rng.Intn(len(stations))]
		dest := stations[rng.Intn(len(stations))]
		if origin == dest {
			continue
		}
		checked++

		rounds := 1 + rng.Intn(5)
		xfer := int64(rng.Intn(4) * 300)
		dep := at(6, 0)

		want := refPareto(refFrontier(n, origin, dest, dep, rounds, xfer))
		got := engineFrontier(origin, dest, dep, rounds, xfer)
		if len(want) > 0 {
			withJourney++
		}
		if !sameFrontier(want, got) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("iteration %d, %s → %s (rounds=%d, buffer=%ds, walk cap=%d)\n  reference %s\n  engine    %s",
					it, origin, dest, rounds, xfer, refWalkCap, formatFrontier(want), formatFrontier(got))
			}
		}
	}
	t.Logf("%d queries compared (%d returned at least one journey), %d mismatches", checked, withJourney, mismatches)
}

// TestDifferential_OvertakingNetworks covers routes where trips overtake each
// other. A single forward pass carrying one trip index boards the earliest
// DEPARTING trip, which on these routes can arrive last; scanRoute sends them
// to the per-trip scan instead, and this test is what holds that in place.
func TestDifferential_OvertakingNetworks(t *testing.T) {
	iterations := 3000
	if testing.Short() {
		iterations = 300
	}
	// A metric closure, so a chained walk can never beat a direct edge and the
	// only thing under test is the trip-selection strategy.
	defer func(prev int) { refWalkCap = prev }(refWalkCap)
	refWalkCap = 1 << 30

	rng := rand.New(rand.NewSource(99))
	checked, mismatches := 0, 0
	for it := 0; it < iterations; it++ {
		n, stations := randomOvertakingNet(rng, 8, 4, 4, rng.Intn(2) == 0)
		publishRefNet(n)

		origin := stations[rng.Intn(len(stations))]
		dest := stations[rng.Intn(len(stations))]
		if origin == dest {
			continue
		}
		checked++

		dep := at(6, 0)
		want := refPareto(refFrontier(n, origin, dest, dep, 4, 0))
		got := engineFrontier(origin, dest, dep, 4, 0)
		if !sameFrontier(want, got) {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("iteration %d, %s → %s\n  reference %s\n  engine    %s",
					it, origin, dest, formatFrontier(want), formatFrontier(got))
			}
		}
	}
	t.Logf("%d queries over overtaking routes, %d mismatches", checked, mismatches)
}

// TestDifferential_TransferBuffers pins the buffer rules: it is charged when
// changing vehicles, and not when arriving on foot or starting the journey.
func TestDifferential_TransferBuffers(t *testing.T) {
	defer func(prev int) { refWalkCap = prev }(refWalkCap)
	refWalkCap = 1 << 30

	for _, xfer := range []int64{0, 180, 300, 600, 900} {
		t.Run(fmt.Sprintf("buffer=%ds", xfer), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(xfer) + 1))
			checked, mismatches := 0, 0
			for it := 0; it < 400; it++ {
				n, stations := randomFIFONet(rng, 10, 6, 4, true)
				publishRefNet(n)
				origin := stations[rng.Intn(len(stations))]
				dest := stations[rng.Intn(len(stations))]
				if origin == dest {
					continue
				}
				checked++
				dep := at(6, 0)
				want := refPareto(refFrontier(n, origin, dest, dep, 4, xfer))
				got := engineFrontier(origin, dest, dep, 4, xfer)
				if !sameFrontier(want, got) {
					mismatches++
					if mismatches <= 2 {
						t.Errorf("iteration %d, %s → %s\n  reference %s\n  engine    %s",
							it, origin, dest, formatFrontier(want), formatFrontier(got))
					}
				}
			}
			t.Logf("%d queries, %d mismatches", checked, mismatches)
		})
	}
}
