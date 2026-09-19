package raptor

import (
	"context"
	"testing"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// Degenerate and malformed input. None of these may panic, and each has a
// specific right answer rather than "whatever happens".

func query(origin, dest string, dep int64, tweak func(*model.SearchParams)) []model.Path {
	p := model.SearchParams{
		Origin: origin, Destination: dest, Date: testDate, DepTime: dep,
		SeatClass: "lower", Passengers: 1,
	}
	if tweak != nil {
		tweak(&p)
	}
	return RaptorSearch(context.Background(), p, 100)
}

func TestEdge_EmptyAndMissingInputs(t *testing.T) {
	twoStop := func() *graph {
		g := newGraph()
		g.addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, true)
		return g
	}

	t.Run("no routes published at all", func(t *testing.T) {
		schedule.SwapRoutes(&schedule.RouteBuffer{
			TripIndex:    map[model.TripKey]model.TripLocation{},
			StopToRoutes: map[string][]schedule.RouteStop{},
			Footpaths:    map[string][]model.Footpath{},
		})
		state.SwapSignal(make(state.SignalBuffer))
		if got := query("A", "B", at(8, 0), nil); got != nil {
			t.Errorf("empty buffer should return nil, got %d paths", len(got))
		}
	})

	t.Run("origin equals destination", func(t *testing.T) {
		twoStop().publish(nil)
		if got := query("A", "A", at(8, 0), nil); got != nil {
			t.Errorf("A→A should return nil, got %d paths", len(got))
		}
	})

	t.Run("empty origin", func(t *testing.T) {
		twoStop().publish(nil)
		if got := query("", "B", at(8, 0), nil); got != nil {
			t.Errorf("empty origin should return nil, got %d paths", len(got))
		}
	})

	t.Run("empty destination", func(t *testing.T) {
		twoStop().publish(nil)
		if got := query("A", "", at(8, 0), nil); got != nil {
			t.Errorf("empty destination should return nil, got %d paths", len(got))
		}
	})

	t.Run("origin unknown to the network", func(t *testing.T) {
		twoStop().publish(nil)
		if got := query("NOWHERE", "B", at(8, 0), nil); len(got) != 0 {
			t.Errorf("unknown origin should find nothing, got %d paths", len(got))
		}
	})

	t.Run("destination unknown to the network", func(t *testing.T) {
		twoStop().publish(nil)
		if got := query("A", "NOWHERE", at(8, 0), nil); len(got) != 0 {
			t.Errorf("unknown destination should find nothing, got %d paths", len(got))
		}
	})

	t.Run("route with no trips", func(t *testing.T) {
		newGraph().addRoute("R", []string{"A", "B"}, nil, true).publish(nil)
		if got := query("A", "B", at(8, 0), nil); len(got) != 0 {
			t.Errorf("a route with no trips cannot carry anyone, got %d paths", len(got))
		}
	})

	t.Run("route with a single stop", func(t *testing.T) {
		newGraph().addRoute("R", []string{"A"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A"}, []int64{at(9, 0)}),
		}, true).publish(nil)
		if got := query("A", "B", at(8, 0), nil); len(got) != 0 {
			t.Errorf("a one-stop route goes nowhere, got %d paths", len(got))
		}
	})

	t.Run("trip shorter than its route", func(t *testing.T) {
		// The loader should not produce this, but a partial ingest could.
		newGraph().addRoute("R", []string{"A", "B", "C"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, false).publish(nil)
		for _, p := range query("A", "C", at(8, 0), nil) {
			t.Errorf("a trip that stops short must not reach C: %s", legSummary(p))
		}
		if got := query("A", "B", at(8, 0), nil); len(got) != 1 {
			t.Errorf("A→B is still served, want 1 path, got %d", len(got))
		}
	})

	t.Run("trip with more arrivals than departures", func(t *testing.T) {
		trip := makeTST(model.TripKey{TripID: "T", Date: testDate},
			[]string{"A", "B", "C"},
			[]int64{at(9, 0), at(10, 0), at(11, 0)},
			[]int64{at(9, 0), at(10, 0)}) // departures truncated
		newGraph().addRoute("R", []string{"A", "B", "C"}, []model.TripStopTimes{trip}, false).publish(nil)
		got := query("A", "C", at(8, 0), nil)
		for _, p := range got {
			if p.ArrivalUnix != at(11, 0) {
				t.Errorf("arrival %d, want %d", p.ArrivalUnix, at(11, 0))
			}
		}
	})
}

// A route that calls at the same station twice is legal (loop and out-and-back
// services do it) and must not confuse the boarding position bookkeeping.
func TestEdge_RouteRevisitsAStation(t *testing.T) {
	stops := []string{"A", "B", "C", "B", "D"}
	newGraph().addRoute("LOOP", stops, []model.TripStopTimes{
		simpleTST("T", testDate, stops,
			[]int64{at(9, 0), at(9, 30), at(10, 0), at(10, 30), at(11, 0)}),
	}, true).publish(nil)

	got := query("A", "D", at(8, 0), nil)
	if len(got) == 0 {
		t.Fatal("A→D should be reachable on the loop")
	}
	if got[0].ArrivalUnix != at(11, 0) {
		t.Errorf("arrival %d, want %d", got[0].ArrivalUnix, at(11, 0))
	}
	if err := ValidatePath(got[0], model.SearchParams{
		Origin: "A", Destination: "D", DepTime: at(8, 0)}); err != nil {
		t.Errorf("returned path does not validate: %v (%s)", err, legSummary(got[0]))
	}

	// B → D must use the SECOND call at B, not loop backwards.
	got = query("B", "D", at(8, 0), nil)
	if len(got) == 0 {
		t.Fatal("B→D should be reachable")
	}
	if got[0].ArrivalUnix != at(11, 0) {
		t.Errorf("B→D arrival %d, want %d", got[0].ArrivalUnix, at(11, 0))
	}
}

func TestEdge_RoundAndResultLimits(t *testing.T) {
	// A→B→C→D, one leg per route, so k transfers need k+1 rounds.
	g := newGraph()
	g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)})}, true)
	g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
		simpleTST("T2", testDate, []string{"B", "C"}, []int64{at(10, 30), at(11, 0)})}, true)
	g.addRoute("R3", []string{"C", "D"}, []model.TripStopTimes{
		simpleTST("T3", testDate, []string{"C", "D"}, []int64{at(11, 30), at(12, 0)})}, true)
	g.publish(nil)

	for _, tc := range []struct {
		rounds int
		want   int // journeys expected
	}{
		{1, 0}, // A→D needs three vehicles
		{2, 0},
		{3, 1},
		{4, 1},
	} {
		got := query("A", "D", at(8, 0), func(p *model.SearchParams) { p.MaxRounds = tc.rounds })
		if len(got) != tc.want {
			t.Errorf("max_rounds=%d: got %d journeys, want %d", tc.rounds, len(got), tc.want)
		}
		for _, p := range got {
			transit := 0
			for _, l := range p.Legs {
				if l.Kind == model.LegTransit {
					transit++
				}
			}
			if transit > tc.rounds {
				t.Errorf("max_rounds=%d: journey uses %d vehicles", tc.rounds, transit)
			}
		}
	}

	t.Run("zero and negative rounds fall back to the default", func(t *testing.T) {
		for _, r := range []int{0, -1, -100} {
			got := query("A", "D", at(8, 0), func(p *model.SearchParams) { p.MaxRounds = r })
			if len(got) != 1 {
				t.Errorf("max_rounds=%d: got %d journeys, want 1 (default applies)", r, len(got))
			}
		}
	})

	t.Run("topK bounds the result set", func(t *testing.T) {
		p := model.SearchParams{Origin: "A", Destination: "D", Date: testDate,
			DepTime: at(8, 0), SeatClass: "lower", Passengers: 1}
		for _, k := range []int{0, -1, 1, 1000} {
			got := RaptorSearch(context.Background(), p, k)
			if k > 0 && len(got) > k {
				t.Errorf("topK=%d returned %d journeys", k, len(got))
			}
		}
	})
}
