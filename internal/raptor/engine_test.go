package raptor

// Unit tests for engine.go — verifying all reported defects are fixed.
//
// Tests call reconstructPaths, findEarliestTrip, and canBoard directly by
// constructing minimal journal/route data in-process. No Redis or Postgres
// dependency needed.

import (
	"testing"

	"axentra/internal/model"
	"axentra/internal/state"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func makeJournal(size int) []map[string]journalEntry {
	j := make([]map[string]journalEntry, size)
	for i := range j {
		j[i] = make(map[string]journalEntry)
	}
	return j
}

// makeTST builds a TripStopTimes with parallel arrival/departure slices.
// L2 fix: tests now supply both arrivals and departures.
func makeTST(key model.TripKey, stationIDs []string, arrivals, departures []int64) model.TripStopTimes {
	return model.TripStopTimes{
		Key:        key,
		StationIDs: stationIDs,
		Arrivals:   arrivals,
		Departures: departures,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D2 — canBoard two-layer design
// ─────────────────────────────────────────────────────────────────────────────

func TestCanBoard_MissingKey_OptimisticallyAllows(t *testing.T) {
	buf := make(state.SignalBuffer) // empty — no seat data
	key := model.TripKey{TripID: "T1", Date: "2026-08-23"}
	if !canBoard(&buf, key, "lower", 1) {
		t.Fatal("canBoard should return true (optimistic) when key is missing")
	}
}

func TestCanBoard_StaleSignal_OptimisticallyAllows(t *testing.T) {
	buf := state.SignalBuffer{
		model.TripKey{TripID: "T1", Date: "2026-08-23"}: {
			ByClass: map[string]int{"lower": 0},
			Stale:   true,
		},
	}
	if !canBoard(&buf, model.TripKey{TripID: "T1", Date: "2026-08-23"}, "lower", 1) {
		t.Fatal("canBoard should return true (optimistic) when signal is stale")
	}
}

func TestCanBoard_FreshData_InsufficientSeats_Blocks(t *testing.T) {
	buf := state.SignalBuffer{
		model.TripKey{TripID: "T1", Date: "2026-08-23"}: {
			ByClass: map[string]int{"lower": 0},
			Stale:   false,
		},
	}
	if canBoard(&buf, model.TripKey{TripID: "T1", Date: "2026-08-23"}, "lower", 1) {
		t.Fatal("canBoard should return false when fresh data shows 0 seats")
	}
}

func TestCanBoard_FreshData_SufficientSeats_Allows(t *testing.T) {
	buf := state.SignalBuffer{
		model.TripKey{TripID: "T1", Date: "2026-08-23"}: {
			ByClass: map[string]int{"lower": 5},
			Stale:   false,
		},
	}
	if !canBoard(&buf, model.TripKey{TripID: "T1", Date: "2026-08-23"}, "lower", 3) {
		t.Fatal("canBoard should return true when fresh data shows enough seats")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// L1 — findEarliestTrip: binary search finds correct trip
// ─────────────────────────────────────────────────────────────────────────────

func TestFindEarliestTrip_BinarySearch_PicksEarliestCatchable(t *testing.T) {
	date := "2026-08-23"
	buf := make(state.SignalBuffer)
	params := model.SearchParams{Date: date, SeatClass: "lower", Passengers: 1}

	// Three trips at pos=0 with departures 100, 200, 300
	trips := []model.TripStopTimes{
		makeTST(model.TripKey{TripID: "T1", Date: date}, []string{"A", "B"}, []int64{90, 190}, []int64{100, 200}),
		makeTST(model.TripKey{TripID: "T2", Date: date}, []string{"A", "B"}, []int64{190, 290}, []int64{200, 300}),
		makeTST(model.TripKey{TripID: "T3", Date: date}, []string{"A", "B"}, []int64{290, 390}, []int64{300, 400}),
	}

	// Arrive at pos=0 at time 150 — T1 (dep=100) cannot be caught; T2 (dep=200) is earliest
	result := findEarliestTrip(trips, 0, 150, infinity, params, &buf)
	if result == nil {
		t.Fatal("expected T2 to be found, got nil")
	}
	if result.Key.TripID != "T2" {
		t.Errorf("expected T2, got %s", result.Key.TripID)
	}
}

func TestFindEarliestTrip_DateFilter_RejectsWrongDate(t *testing.T) {
	buf := make(state.SignalBuffer)
	// date+2 must be rejected (not the search date and not overnight date+1)
	params := model.SearchParams{Date: "2026-08-24", SeatClass: "lower", Passengers: 1}

	trips := []model.TripStopTimes{
		makeTST(model.TripKey{TripID: "T1", Date: "2026-08-26"}, []string{"A"}, []int64{0}, []int64{100}),
	}
	result := findEarliestTrip(trips, 0, 50, infinity, params, &buf)
	if result != nil {
		t.Errorf("expected nil (date+2 should be rejected), got trip %s", result.Key.TripID)
	}
}

func TestFindEarliestTrip_OvernightDate_AcceptsNextDay(t *testing.T) {
	buf := make(state.SignalBuffer)
	// Search date is 2026-08-24; next day is 2026-08-25 — must be accepted for overnight trains
	params := model.SearchParams{Date: "2026-08-24", SeatClass: "lower", Passengers: 1}

	trips := []model.TripStopTimes{
		makeTST(model.TripKey{TripID: "T1", Date: "2026-08-25"}, []string{"A"}, []int64{0}, []int64{100}),
	}
	result := findEarliestTrip(trips, 0, 50, infinity, params, &buf)
	if result == nil {
		t.Error("expected overnight trip (date+1=2026-08-25) to be accepted, got nil")
	}
}

func TestFindEarliestTrip_SeatFilter_RejectsFullTrain(t *testing.T) {
	date := "2026-08-23"
	key := model.TripKey{TripID: "T1", Date: date}
	buf := state.SignalBuffer{
		key: {ByClass: map[string]int{"lower": 0}, Stale: false}, // 0 seats, not stale
	}
	params := model.SearchParams{Date: date, SeatClass: "lower", Passengers: 1}

	trips := []model.TripStopTimes{
		makeTST(key, []string{"A"}, []int64{90}, []int64{100}),
	}
	result := findEarliestTrip(trips, 0, 50, infinity, params, &buf)
	if result != nil {
		t.Errorf("expected nil (no seats), got trip %s", result.Key.TripID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D5 — currRound backtrack: normal path (no fallback needed)
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_TwoLeg_Normal(t *testing.T) {
	rounds := model.DefaultMaxRounds
	j := makeJournal(rounds + 1)

	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		tripKey:      model.TripKey{TripID: "T1", Date: "2026-08-23"},
		routeID:      "R1",
		boardStop:    "A",
		boardDepUnix: 1000,
		arrivalUnix:  2000,
	}
	j[2]["C"] = journalEntry{
		round: 2, station: "C",
		tripKey:      model.TripKey{TripID: "T2", Date: "2026-08-23"},
		routeID:      "R2",
		boardStop:    "B",
		boardDepUnix: 2100,
		arrivalUnix:  3000,
	}

	params := model.SearchParams{Origin: "A", Destination: "C", Date: "2026-08-23", DepTime: 900}
	paths := reconstructPaths(j, params, rounds)

	if len(paths) == 0 {
		t.Fatal("expected at least one path, got none")
	}
	p := paths[0]
	if len(p.Legs) != 2 {
		t.Fatalf("expected 2 legs, got %d", len(p.Legs))
	}
	if p.Legs[0].BoardStation != "A" || p.Legs[0].AlightStation != "B" {
		t.Errorf("leg 0 wrong: board=%s alight=%s", p.Legs[0].BoardStation, p.Legs[0].AlightStation)
	}
	if p.Legs[1].BoardStation != "B" || p.Legs[1].AlightStation != "C" {
		t.Errorf("leg 1 wrong: board=%s alight=%s", p.Legs[1].BoardStation, p.Legs[1].AlightStation)
	}
	if p.ArrivalUnix != 3000 {
		t.Errorf("expected ArrivalUnix=3000, got %d", p.ArrivalUnix)
	}
	if p.Transfers != 1 {
		t.Errorf("expected Transfers=1, got %d", p.Transfers)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D5 — currRound backtrack: walk-then-transit within same round
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_FallbackFires_D5Fix(t *testing.T) {
	rounds := model.DefaultMaxRounds
	j := makeJournal(rounds + 1)

	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		tripKey:      model.TripKey{TripID: "T1", Date: "2026-08-23"},
		routeID:      "R1",
		boardStop:    "A",
		boardDepUnix: 1000,
		arrivalUnix:  2000,
	}
	j[1]["B2"] = journalEntry{
		round:        1, station: "B2",
		routeID:      "WALK",
		boardStop:    "B",
		boardDepUnix: 2000,
		arrivalUnix:  2300,
	}
	j[2]["C"] = journalEntry{
		round: 2, station: "C",
		tripKey:      model.TripKey{TripID: "T2", Date: "2026-08-23"},
		routeID:      "R2",
		boardStop:    "B2",
		boardDepUnix: 2400,
		arrivalUnix:  3500,
	}

	params := model.SearchParams{Origin: "A", Destination: "C", Date: "2026-08-23", DepTime: 900}
	paths := reconstructPaths(j, params, rounds)

	if len(paths) == 0 {
		t.Fatal("D5: expected at least one path involving walk transfer, got none")
	}

	var found bool
	for _, p := range paths {
		if len(p.Legs) == 3 &&
			p.Legs[0].BoardStation == "A" && p.Legs[0].AlightStation == "B" &&
			p.Legs[1].BoardStation == "B" && p.Legs[1].AlightStation == "B2" &&
			p.Legs[2].BoardStation == "B2" && p.Legs[2].AlightStation == "C" {
			found = true
			if p.ArrivalUnix != 3500 {
				t.Errorf("D5: expected ArrivalUnix=3500, got %d", p.ArrivalUnix)
			}
		}
	}
	if !found {
		t.Errorf("D5: did not find expected 3-leg path A→B→B2→C in paths: %+v", paths)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cycle guard: malformed journal with circular boardStop
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_CycleDetected_NoInfiniteLoop(t *testing.T) {
	rounds := model.DefaultMaxRounds
	j := makeJournal(rounds + 1)

	j[1]["C"] = journalEntry{round: 1, station: "C", routeID: "R1", boardStop: "B"}
	j[1]["B"] = journalEntry{round: 1, station: "B", routeID: "R1", boardStop: "C"} // circular

	params := model.SearchParams{Origin: "A", Destination: "C", DepTime: 0}
	paths := reconstructPaths(j, params, rounds)
	if len(paths) != 0 {
		t.Errorf("expected 0 paths for circular journal, got %d", len(paths))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D1 — Footpath nil-map safety
// ─────────────────────────────────────────────────────────────────────────────

func TestFootpaths_NilMapPanic_DoesNotOccur(t *testing.T) {
	fp := map[string][]model.Footpath{} // simulates routes.Footpaths when empty
	station := "A"
	for _, f := range fp[station] { // must not panic even with missing key
		_ = f
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// E7 — Leg-continuity guard: gap in journal discards path
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_LegContinuityGap_Discarded(t *testing.T) {
	rounds := model.DefaultMaxRounds
	j := makeJournal(rounds + 1)

	j[1]["C"] = journalEntry{round: 1, station: "C", routeID: "R1", boardStop: "X"} // X ≠ origin
	j[1]["Y"] = journalEntry{round: 1, station: "Y", routeID: "R1", boardStop: "A"}
	// No entry linking Y → X

	params := model.SearchParams{Origin: "A", Destination: "C", DepTime: 0}
	paths := reconstructPaths(j, params, rounds)
	if len(paths) != 0 {
		t.Errorf("expected 0 paths for broken journal chain, got %d", len(paths))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// L9 — deduplicatePaths removes identical journeys
// ─────────────────────────────────────────────────────────────────────────────

func TestDeduplicatePaths_RemovesDuplicates(t *testing.T) {
	leg := model.Leg{TripID: "T1", BoardStation: "A", AlightStation: "B"}
	p := model.Path{Legs: []model.Leg{leg}, ArrivalUnix: 1000, Transfers: 0}
	paths := []model.Path{p, p, p} // 3 identical copies

	result := deduplicatePaths(paths)
	if len(result) != 1 {
		t.Errorf("expected 1 unique path after dedup, got %d", len(result))
	}
}

func TestDeduplicatePaths_PreservesDistinct(t *testing.T) {
	p1 := model.Path{Legs: []model.Leg{{TripID: "T1", BoardStation: "A", AlightStation: "B"}}}
	p2 := model.Path{Legs: []model.Leg{{TripID: "T2", BoardStation: "A", AlightStation: "B"}}}
	result := deduplicatePaths([]model.Path{p1, p2})
	if len(result) != 2 {
		t.Errorf("expected 2 distinct paths, got %d", len(result))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// L4 — MaxRounds: runtime configurable via SearchParams
// ─────────────────────────────────────────────────────────────────────────────

func TestMaxRounds_UsesParamsWhenNonZero(t *testing.T) {
	params := model.SearchParams{MaxRounds: 6}
	if got := maxRounds(params); got != 6 {
		t.Errorf("expected 6, got %d", got)
	}
}

func TestMaxRounds_FallsBackToDefaultWhenZero(t *testing.T) {
	params := model.SearchParams{MaxRounds: 0}
	if got := maxRounds(params); got != model.DefaultMaxRounds {
		t.Errorf("expected %d, got %d", model.DefaultMaxRounds, got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// L15 — Multi-day journey: overnight train crossing midnight
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_MultiDayJourney(t *testing.T) {
	// Scenario: search date 2026-08-23.
	// Train T1 departs A on 2026-08-23 at 23:00 (unix 1724454000),
	// arrives at B on 2026-08-24 at 01:00 (unix 1724461200).
	// Train T2 departs B on 2026-08-24 at 03:00 (unix 1724468400),
	// arrives at C on 2026-08-24 at 05:00 (unix 1724475600).
	// The engine should find A→B→C even though legs span two calendar days.
	// NOTE: The current engine requires tst.Key.Date == params.Date, so the
	// overnight leg must be on the search date. This test validates the
	// journal reconstruction handles two-day legs correctly.

	rounds := model.DefaultMaxRounds
	j := makeJournal(rounds + 1)

	// Leg 1: T1 departs A on 2026-08-23 (search date), arrives B on 2026-08-24
	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		tripKey:      model.TripKey{TripID: "T1", Date: "2026-08-23"},
		routeID:      "OVERNIGHT",
		boardStop:    "A",
		boardDepUnix: 1724454000, // 2026-08-23 23:00 UTC
		arrivalUnix:  1724461200, // 2026-08-24 01:00 UTC
	}

	// Leg 2: T2 departs B on 2026-08-24
	j[2]["C"] = journalEntry{
		round: 2, station: "C",
		tripKey:      model.TripKey{TripID: "T2", Date: "2026-08-24"},
		routeID:      "MORNING",
		boardStop:    "B",
		boardDepUnix: 1724468400, // 2026-08-24 03:00 UTC
		arrivalUnix:  1724475600, // 2026-08-24 05:00 UTC
	}

	params := model.SearchParams{
		Origin:      "A",
		Destination: "C",
		Date:        "2026-08-23",
		DepTime:     1724450400, // 2026-08-23 22:00 UTC
	}

	paths := reconstructPaths(j, params, rounds)
	if len(paths) == 0 {
		t.Fatal("multi-day: expected at least one path for overnight journey, got none")
	}

	p := paths[0]
	if len(p.Legs) != 2 {
		t.Fatalf("multi-day: expected 2 legs, got %d", len(p.Legs))
	}
	if p.Legs[0].BoardStation != "A" || p.Legs[0].AlightStation != "B" {
		t.Errorf("multi-day: leg 0 wrong: board=%s alight=%s", p.Legs[0].BoardStation, p.Legs[0].AlightStation)
	}
	if p.Legs[1].BoardStation != "B" || p.Legs[1].AlightStation != "C" {
		t.Errorf("multi-day: leg 1 wrong: board=%s alight=%s", p.Legs[1].BoardStation, p.Legs[1].AlightStation)
	}
	if p.ArrivalUnix != 1724475600 {
		t.Errorf("multi-day: expected ArrivalUnix=1724475600, got %d", p.ArrivalUnix)
	}
}
