package raptor

import (
	"testing"

	"axentra/internal/model"
	"axentra/internal/state"
)

// Exact-equality boundaries. Every one of these is a place where > and >= give
// different answers, and where an off-by-one is invisible in normal fixtures.

func TestEdge_TimeBoundaries(t *testing.T) {
	build := func() {
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, true).publish(nil)
	}

	t.Run("departing exactly at the trip's departure is catchable", func(t *testing.T) {
		build()
		if got := query("A", "B", at(9, 0), nil); len(got) != 1 {
			t.Fatalf("dep_time == trip departure should catch it, got %d journeys", len(got))
		}
	})

	t.Run("departing one second later misses it", func(t *testing.T) {
		build()
		if got := query("A", "B", at(9, 0)+1, nil); len(got) != 0 {
			t.Errorf("one second late should miss the train, got %s", legSummary(got[0]))
		}
	})

	t.Run("a zero-duration hop is still a journey", func(t *testing.T) {
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(9, 0)}),
		}, true).publish(nil)
		got := query("A", "B", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 journey, got %d", len(got))
		}
		if got[0].TotalTimeSeconds != 0 {
			t.Errorf("total time %d, want 0", got[0].TotalTimeSeconds)
		}
	})

	t.Run("dwell time is honoured: alight on arrival, board on departure", func(t *testing.T) {
		// Arrives B at 10:00 but does not leave until 10:30. A passenger going
		// to B is there at 10:00; the connection at B departs 10:15 and must
		// therefore be catchable.
		g := newGraph()
		g.addRoute("R1", []string{"A", "B", "C"}, []model.TripStopTimes{
			makeTST(model.TripKey{TripID: "T1", Date: testDate}, []string{"A", "B", "C"},
				[]int64{at(9, 0), at(10, 0), at(12, 0)},  // arrivals
				[]int64{at(9, 0), at(10, 30), at(12, 0)}, // departures — 30 min dwell at B
			)}, true)
		g.addRoute("R2", []string{"B", "D"}, []model.TripStopTimes{
			simpleTST("T2", testDate, []string{"B", "D"}, []int64{at(10, 15), at(11, 0)})}, true)
		g.publish(nil)

		got := query("A", "B", at(8, 0), nil)
		if len(got) != 1 || got[0].ArrivalUnix != at(10, 0) {
			t.Fatalf("A→B should arrive on the 10:00 arrival, got %v", got)
		}
		got = query("A", "D", at(8, 0), nil)
		if len(got) == 0 {
			t.Fatal("A→D via the 10:15 connection should exist — alighting uses arrivals, not departures")
		}
		if got[0].ArrivalUnix != at(11, 0) {
			t.Errorf("A→D arrival %d, want %d (%s)", got[0].ArrivalUnix, at(11, 0), legSummary(got[0]))
		}
	})
}

func TestEdge_DateWindow(t *testing.T) {
	// DefaultDateWindowDays is 2, and the day before is always allowed, so
	// operating days -1..+2 are usable and -2 and +3 are not.
	for _, tc := range []struct {
		dayOffset int
		usable    bool
	}{
		{-2, false},
		{-1, true},
		{0, true},
		{1, true},
		{2, true},
		{3, false},
	} {
		trip := simpleTST("T", dayString(tc.dayOffset), []string{"A", "B"},
			[]int64{atDay(tc.dayOffset, 9, 0), atDay(tc.dayOffset, 10, 0)})
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{trip}, true).publish(nil)

		got := query("A", "B", atDay(-2, 0, 0), nil)
		if tc.usable && len(got) == 0 {
			t.Errorf("a trip on day %+d should be inside the window", tc.dayOffset)
		}
		if !tc.usable && len(got) != 0 {
			t.Errorf("a trip on day %+d should be outside the window, got %s", tc.dayOffset, legSummary(got[0]))
		}
	}
}

func TestEdge_MidnightCrossing(t *testing.T) {
	// Departs 23:00 on the operating date and arrives 03:00 the next calendar
	// day. The stop times cross midnight while the trip keeps one date.
	newGraph().addRoute("OVN", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T", testDate, []string{"A", "B"}, []int64{at(23, 0), atDay(1, 3, 0)}),
	}, true).publish(nil)

	got := query("A", "B", at(22, 0), nil)
	if len(got) != 1 {
		t.Fatalf("the overnight service should be found, got %d journeys", len(got))
	}
	if got[0].ArrivalUnix != atDay(1, 3, 0) {
		t.Errorf("arrival %d, want %d", got[0].ArrivalUnix, atDay(1, 3, 0))
	}
	if got[0].TotalTimeSeconds != 4*3600 {
		t.Errorf("total time %ds, want %ds", got[0].TotalTimeSeconds, 4*3600)
	}
}

func TestEdge_TransferBufferBoundaries(t *testing.T) {
	// Arrive B at 10:00, connection departs 10:10 — a 600-second gap.
	build := func() {
		g := newGraph()
		g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
			simpleTST("T2", testDate, []string{"B", "C"}, []int64{at(10, 10), at(11, 0)})}, true)
		g.publish(nil)
	}

	for _, tc := range []struct {
		buffer int
		ok     bool
	}{
		{0, true},
		{599, true},
		{600, true},  // exactly equal: ready 10:10, departs 10:10
		{601, false}, // one second too tight
		{3600, false},
	} {
		build()
		got := query("A", "C", at(8, 0), func(p *model.SearchParams) { p.MinTransferSeconds = tc.buffer })
		if tc.ok && len(got) == 0 {
			t.Errorf("buffer=%ds: the connection should hold", tc.buffer)
		}
		if !tc.ok && len(got) != 0 {
			t.Errorf("buffer=%ds: the connection should be too tight, got %s", tc.buffer, legSummary(got[0]))
		}
	}

	t.Run("the buffer is not charged at the origin", func(t *testing.T) {
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)})}, true).publish(nil)
		got := query("A", "B", at(9, 0), func(p *model.SearchParams) { p.MinTransferSeconds = 3600 })
		if len(got) != 1 {
			t.Error("starting a journey is not a transfer; the 09:00 departure should still be catchable")
		}
	})

	t.Run("the buffer is not charged after walking", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R1", []string{"A", "X"}, []model.TripStopTimes{
			simpleTST("T1", testDate, []string{"A", "X"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.addRoute("R2", []string{"Y", "C"}, []model.TripStopTimes{
			simpleTST("T2", testDate, []string{"Y", "C"}, []int64{at(10, 5), at(11, 0)})}, true)
		g.walk("X", "Y", 300) // arrive Y at 10:05 on foot
		g.publish(nil)

		got := query("A", "C", at(8, 0), func(p *model.SearchParams) { p.MinTransferSeconds = 3600 })
		if len(got) == 0 {
			t.Fatal("the walk itself covers the change, so a 1-hour buffer must not block the 10:05 departure")
		}
		if got[0].ArrivalUnix != at(11, 0) {
			t.Errorf("arrival %d, want %d (%s)", got[0].ArrivalUnix, at(11, 0), legSummary(got[0]))
		}
	})
}

func TestEdge_Footpaths(t *testing.T) {
	t.Run("a walk-only journey needs no vehicle", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R", []string{"A", "Z"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "Z"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.walk("A", "B", 900)
		g.publish(nil)

		got := query("A", "B", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("A→B on foot should be one journey, got %d", len(got))
		}
		if got[0].Transfers != 0 {
			t.Errorf("a walk-only journey has 0 transfers, got %d", got[0].Transfers)
		}
		if got[0].ArrivalUnix != at(8, 15) {
			t.Errorf("arrival %d, want %d", got[0].ArrivalUnix, at(8, 15))
		}
		if got[0].WalkSeconds != 900 {
			t.Errorf("walk seconds %d, want 900", got[0].WalkSeconds)
		}
	})

	t.Run("a zero-second footpath is allowed", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R", []string{"B", "C"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"B", "C"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.walk("A", "B", 0)
		g.publish(nil)
		got := query("A", "C", at(8, 0), nil)
		if len(got) != 1 || got[0].ArrivalUnix != at(10, 0) {
			t.Errorf("a same-platform interchange should work, got %v", got)
		}
	})

	t.Run("a footpath to a station on no route is harmless", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R", []string{"A", "C"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "C"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.walk("A", "ORPHAN", 300)
		g.walk("ORPHAN", "A", 300)
		g.publish(nil)
		if got := query("A", "C", at(8, 0), nil); len(got) != 1 {
			t.Errorf("want 1 journey, got %d", len(got))
		}
		if got := query("A", "ORPHAN", at(8, 0), nil); len(got) != 1 {
			t.Errorf("the orphan is still walk-reachable, got %d journeys", len(got))
		}
	})

	t.Run("a two-way footpath does not loop", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R", []string{"A", "C"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "C"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.walk("A", "B", 300)
		g.walk("B", "A", 300)
		g.publish(nil)
		got := query("A", "C", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 journey, got %d", len(got))
		}
		if len(got[0].Legs) != 1 {
			t.Errorf("the journey should be one ride, got %s", legSummary(got[0]))
		}
	})
}

func TestEdge_Seats(t *testing.T) {
	full := model.TripKey{TripID: "T_FULL", Date: testDate}
	build := func(signals state.SignalBuffer) {
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T_FULL", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, true).publish(signals)
	}

	t.Run("a full train is not offered", func(t *testing.T) {
		build(state.SignalBuffer{full: {Total: 0, ByClass: map[string]int{"lower": 0}}})
		if got := query("A", "B", at(8, 0), nil); len(got) != 0 {
			t.Errorf("a train with no seats should not be offered, got %s", legSummary(got[0]))
		}
	})

	t.Run("a stale signal is optimistically allowed", func(t *testing.T) {
		build(state.SignalBuffer{full: {Total: 0, ByClass: map[string]int{"lower": 0}, Stale: true}})
		if got := query("A", "B", at(8, 0), nil); len(got) != 1 {
			t.Error("a stale signal must fall through to the strict validator, not block the search")
		}
	})

	t.Run("missing seat data is optimistically allowed", func(t *testing.T) {
		build(nil)
		if got := query("A", "B", at(8, 0), nil); len(got) != 1 {
			t.Error("no seat data yet must not mean no results")
		}
	})

	t.Run("the party size is respected", func(t *testing.T) {
		build(state.SignalBuffer{full: {Total: 3, ByClass: map[string]int{"lower": 3}}})
		for _, n := range []int{1, 3} {
			if got := query("A", "B", at(8, 0), func(p *model.SearchParams) { p.Passengers = n }); len(got) != 1 {
				t.Errorf("%d passengers should fit in 3 seats", n)
			}
		}
		if got := query("A", "B", at(8, 0), func(p *model.SearchParams) { p.Passengers = 4 }); len(got) != 0 {
			t.Errorf("4 passengers do not fit in 3 seats, got %s", legSummary(got[0]))
		}
	})

	t.Run("an unknown seat class is not silently allowed", func(t *testing.T) {
		build(state.SignalBuffer{full: {Total: 9, ByClass: map[string]int{"lower": 9}}})
		got := query("A", "B", at(8, 0), func(p *model.SearchParams) { p.SeatClass = "no_such_class" })
		if len(got) != 0 {
			t.Errorf("a class the trip does not sell has zero seats, got %s", legSummary(got[0]))
		}
	})
}
