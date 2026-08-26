package raptor

import (
	"context"
	"testing"

	"axentra/internal/model"
	"axentra/internal/state"
)

// ─────────────────────────────────────────────────────────────────────────────
// canBoard — the optimistic in-memory pre-filter (layer 1 of two)
// ─────────────────────────────────────────────────────────────────────────────

func TestCanBoard(t *testing.T) {
	key := model.TripKey{TripID: "T1", Date: testDate}

	cases := []struct {
		name  string
		buf   state.SignalBuffer
		count int
		want  bool
	}{
		{
			name:  "missing key boards optimistically",
			buf:   state.SignalBuffer{},
			count: 1,
			want:  true,
		},
		{
			name:  "stale signal boards optimistically",
			buf:   state.SignalBuffer{key: {ByClass: map[string]int{"lower": 0}, Stale: true}},
			count: 1,
			want:  true,
		},
		{
			name:  "fresh signal with no seats blocks",
			buf:   state.SignalBuffer{key: {ByClass: map[string]int{"lower": 0}}},
			count: 1,
			want:  false,
		},
		{
			name:  "fresh signal with enough seats boards",
			buf:   state.SignalBuffer{key: {ByClass: map[string]int{"lower": 5}}},
			count: 3,
			want:  true,
		},
		{
			name:  "fresh signal with too few seats for the group blocks",
			buf:   state.SignalBuffer{key: {ByClass: map[string]int{"lower": 2}}},
			count: 3,
			want:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := tc.buf
			if got := canBoard(&buf, key, "lower", tc.count); got != tc.want {
				t.Fatalf("canBoard = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Trip lookup
// ─────────────────────────────────────────────────────────────────────────────

func newTestState(t *testing.T, p model.SearchParams, signals state.SignalBuffer) *searchState {
	t.Helper()
	if signals == nil {
		signals = make(state.SignalBuffer)
	}
	return &searchState{
		params:  p,
		rounds:  p.Rounds(),
		xferSec: p.TransferBuffer(),
		dest:    p.Destination,
		signals: &signals,
		allowed: computeAllowedDates(p.Date, p.DateWindow()),
		best:    map[string]int64{},
	}
}

func TestEarliestTrip_PicksEarliestCatchable(t *testing.T) {
	trips := []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		simpleTST("T2", testDate, []string{"A", "B"}, []int64{at(11, 0), at(12, 0)}),
		simpleTST("T3", testDate, []string{"A", "B"}, []int64{at(13, 0), at(14, 0)}),
	}
	s := newTestState(t, model.SearchParams{Date: testDate, SeatClass: "lower", Passengers: 1}, nil)

	// Ready at 10:00: T1 (09:00) has gone, T2 (11:00) is the earliest catchable.
	for _, fifo := range []bool{true, false} {
		idx := s.earliestTrip(trips, 0, at(10, 0), infinity, fifo)
		if idx < 0 {
			t.Fatalf("fifo=%v: expected T2, got no trip", fifo)
		}
		if got := trips[idx].Key.TripID; got != "T2" {
			t.Errorf("fifo=%v: got %s, want T2", fifo, got)
		}
	}
}

func TestEarliestTrip_RejectsDatesOutsideWindow(t *testing.T) {
	s := newTestState(t, model.SearchParams{Date: testDate, SeatClass: "lower", Passengers: 1}, nil)

	trips := []model.TripStopTimes{
		// 10 days out: far beyond the calendar window.
		simpleTST("T_FAR", dayString(10), []string{"A", "B"}, []int64{atDay(10, 9, 0), atDay(10, 10, 0)}),
	}
	if idx := s.earliestTrip(trips, 0, at(8, 0), infinity, true); idx >= 0 {
		t.Fatalf("trip on %s should be outside the date window, got index %d", dayString(10), idx)
	}
}

func TestEarliestTrip_AcceptsNextDayAndPreviousDay(t *testing.T) {
	s := newTestState(t, model.SearchParams{Date: testDate, SeatClass: "lower", Passengers: 1}, nil)

	// Tomorrow's early service is reachable from a late-night search today.
	next := []model.TripStopTimes{
		simpleTST("T_NEXT", dayString(1), []string{"A", "B"}, []int64{atDay(1, 6, 0), atDay(1, 7, 0)}),
	}
	if idx := s.earliestTrip(next, 0, at(23, 0), infinity, true); idx < 0 {
		t.Error("next-day trip should be inside the date window")
	}

	// So is an overnight service that departed yesterday and is still running.
	prev := []model.TripStopTimes{
		simpleTST("T_PREV", dayString(-1), []string{"A", "B"}, []int64{atDay(-1, 23, 0), at(3, 0)}),
	}
	if idx := s.earliestTrip(prev, 0, atDay(-1, 22, 0), infinity, true); idx < 0 {
		t.Error("previous-day overnight trip should be inside the date window")
	}
}

func TestEarliestTrip_SkipsFullTrains(t *testing.T) {
	full := model.TripKey{TripID: "T_FULL", Date: testDate}
	signals := state.SignalBuffer{full: {ByClass: map[string]int{"lower": 0}}}

	trips := []model.TripStopTimes{
		simpleTST("T_FULL", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		simpleTST("T_OPEN", testDate, []string{"A", "B"}, []int64{at(11, 0), at(12, 0)}),
	}
	s := newTestState(t, model.SearchParams{Date: testDate, SeatClass: "lower", Passengers: 1}, signals)

	for _, fifo := range []bool{true, false} {
		idx := s.earliestTrip(trips, 0, at(8, 0), infinity, fifo)
		if idx < 0 {
			t.Fatalf("fifo=%v: expected T_OPEN, got no trip", fifo)
		}
		if got := trips[idx].Key.TripID; got != "T_OPEN" {
			t.Errorf("fifo=%v: got %s, want T_OPEN (T_FULL has no seats)", fifo, got)
		}
	}
}

// Regression: on a route where an express overtakes a local, binary search
// walks past the genuinely earliest-arriving trip. The loader marks such routes
// non-FIFO precisely so the linear scan handles them.
func TestEarliestTrip_NonFIFORouteFindsOvertakingExpress(t *testing.T) {
	// Sorted by departure at stop 0. The express leaves later but reaches
	// stop 1 first, so at stop 1 it departs *before* the local.
	trips := []model.TripStopTimes{
		simpleTST("T_LOCAL", testDate, []string{"A", "B", "C"}, []int64{at(8, 0), at(11, 0), at(14, 0)}),
		simpleTST("T_EXPRESS", testDate, []string{"A", "B", "C"}, []int64{at(9, 0), at(10, 0), at(11, 0)}),
	}
	s := newTestState(t, model.SearchParams{Date: testDate, SeatClass: "lower", Passengers: 1}, nil)

	idx := s.earliestTrip(trips, 1, at(9, 30), infinity, false)
	if idx < 0 {
		t.Fatal("linear scan should find the express at stop B")
	}
	if got := trips[idx].Key.TripID; got != "T_EXPRESS" {
		t.Errorf("got %s, want T_EXPRESS", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Date window
// ─────────────────────────────────────────────────────────────────────────────

func TestComputeAllowedDates(t *testing.T) {
	got := computeAllowedDates(testDate, 2)
	for _, want := range []string{dayString(-1), dayString(0), dayString(1), dayString(2)} {
		if !got[want] {
			t.Errorf("date %s should be allowed", want)
		}
	}
	for _, notWant := range []string{dayString(-2), dayString(3)} {
		if got[notWant] {
			t.Errorf("date %s should not be allowed", notWant)
		}
	}
}

func TestComputeAllowedDates_MalformedInputDegradesToSameDay(t *testing.T) {
	got := computeAllowedDates("23-08-2026", 2)
	if len(got) != 1 || !got["23-08-2026"] {
		t.Fatalf("malformed date should degrade to a single-date set, got %v", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Path post-processing
// ─────────────────────────────────────────────────────────────────────────────

func path(arrival int64, transfers int, trips ...string) model.Path {
	p := model.Path{ArrivalUnix: arrival, Transfers: transfers}
	for _, id := range trips {
		p.Legs = append(p.Legs, model.Leg{Kind: model.LegTransit, TripID: id, BoardStation: "A", AlightStation: "B"})
	}
	return p
}

func TestDeduplicatePaths(t *testing.T) {
	in := []model.Path{path(100, 0, "T1"), path(100, 0, "T1"), path(200, 1, "T2")}
	out := deduplicatePaths(in)
	if len(out) != 2 {
		t.Fatalf("got %d paths, want 2", len(out))
	}
}

func TestParetoFilter_DropsDominatedPaths(t *testing.T) {
	// Sorted by arrival. The third arrives later AND transfers more — dominated.
	in := []model.Path{
		{ArrivalUnix: 100, Transfers: 2},
		{ArrivalUnix: 200, Transfers: 0},
		{ArrivalUnix: 300, Transfers: 3},
	}
	out := paretoFilter(in)
	if len(out) != 2 {
		t.Fatalf("got %d paths, want 2: %+v", len(out), out)
	}
	if out[0].Transfers != 2 || out[1].Transfers != 0 {
		t.Errorf("unexpected frontier: %+v", out)
	}
	for i := 1; i < len(out); i++ {
		if out[i].Transfers >= out[i-1].Transfers {
			t.Errorf("frontier must strictly reduce transfers: %+v", out)
		}
	}
}

func TestSearchParams_Clamping(t *testing.T) {
	if got := (model.SearchParams{}).Rounds(); got != model.DefaultMaxRounds {
		t.Errorf("zero MaxRounds should fall back to the default, got %d", got)
	}
	if got := (model.SearchParams{MaxRounds: 3}).Rounds(); got != 3 {
		t.Errorf("explicit MaxRounds should be honoured, got %d", got)
	}
	if got := (model.SearchParams{MaxRounds: 999}).Rounds(); got != model.MaxAllowedRounds {
		t.Errorf("MaxRounds should clamp to %d, got %d", model.MaxAllowedRounds, got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Guard rails
// ─────────────────────────────────────────────────────────────────────────────

func TestSearch_EmptyBufferReturnsNothing(t *testing.T) {
	newGraph().publish(nil)
	if got := search(t, "A", "B", at(8, 0), nil); got != nil {
		t.Fatalf("empty schedule should yield no paths, got %d", len(got))
	}
}

func TestSearch_SameOriginAndDestination(t *testing.T) {
	newGraph().
		addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, true).
		publish(nil)

	if got := search(t, "A", "A", at(8, 0), nil); len(got) != 0 {
		t.Fatalf("origin == destination should yield no paths, got %d", len(got))
	}
}

func TestSearch_CancelledContextAborts(t *testing.T) {
	newGraph().
		addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		}, true).
		publish(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := RaptorSearch(ctx, model.SearchParams{
		Origin: "A", Destination: "B", Date: testDate,
		DepTime: at(8, 0), SeatClass: "lower", Passengers: 1,
	}, 100)
	if got != nil {
		t.Fatalf("cancelled search should return nil, got %d paths", len(got))
	}
}
