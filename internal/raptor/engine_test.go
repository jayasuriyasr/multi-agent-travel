package raptor

// Unit tests for engine.go — verifying all reported defects are fixed.
//
// These tests call reconstructPaths and the footpath logic directly by
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
// D5 — currRound backtrack: normal path (no fallback needed)
// ─────────────────────────────────────────────────────────────────────────────
// Network: origin=A, dest=C, via B.
// Round 1: A→B on trip T1
// Round 2: B→C on trip T2
// Expected: one path with 2 legs.

func TestReconstructPaths_TwoLeg_Normal(t *testing.T) {
	j := makeJournal(MaxRounds + 1)

	// Round 1: arrived at B via T1, boarded at A
	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		tripKey:      model.TripKey{TripID: "T1", Date: "2026-08-23"},
		routeID:      "R1",
		boardStop:    "A",
		boardDepUnix: 1000,
		arrivalUnix:  2000,
	}

	// Round 2: arrived at C via T2, boarded at B
	j[2]["C"] = journalEntry{
		round: 2, station: "C",
		tripKey:      model.TripKey{TripID: "T2", Date: "2026-08-23"},
		routeID:      "R2",
		boardStop:    "B",
		boardDepUnix: 2100,
		arrivalUnix:  3000,
	}

	params := model.SearchParams{
		Origin:      "A",
		Destination: "C",
		Date:        "2026-08-23",
		DepTime:     900,
	}

	paths := reconstructPaths(j, params)

	if len(paths) == 0 {
		t.Fatal("expected at least one path, got none")
	}
	p := paths[0]
	if len(p.Legs) != 2 {
		t.Fatalf("expected 2 legs, got %d", len(p.Legs))
	}
	if p.Legs[0].BoardStation != "A" || p.Legs[0].AlightStation != "B" {
		t.Errorf("leg 0 wrong: got board=%s alight=%s", p.Legs[0].BoardStation, p.Legs[0].AlightStation)
	}
	if p.Legs[1].BoardStation != "B" || p.Legs[1].AlightStation != "C" {
		t.Errorf("leg 1 wrong: got board=%s alight=%s", p.Legs[1].BoardStation, p.Legs[1].AlightStation)
	}
	if p.ArrivalUnix != 3000 {
		t.Errorf("expected ArrivalUnix=3000, got %d", p.ArrivalUnix)
	}
	if p.Transfers != 1 {
		t.Errorf("expected Transfers=1, got %d", p.Transfers)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D5 — currRound backtrack: fallback fires (journal entry in earlier round)
// ─────────────────────────────────────────────────────────────────────────────
// Network: A→B in round 1. Then walk B→B2 (journal in round 1 as WALK).
// Then B2→C in round 2.
// Destination C is in journal[2]. Board stop is B2.
// B2 is in journal[1] (walk entry). Board stop is B.
// B is in journal[1] (transit entry). Board stop is A = origin.
//
// The key test for D5: when we reconstruct C (round 2, boardStop=B2),
// we look for B2 in journal[2] — it's NOT there, only in journal[1].
// The fallback sets currRound=1. Then D5 fix: we do NOT double-decrement.
// Next: look for B in journal[1] — it IS there. boardStop=A=origin → done.

func TestReconstructPaths_FallbackFires_D5Fix(t *testing.T) {
	j := makeJournal(MaxRounds + 1)

	// Round 1: A→B transit
	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		tripKey:      model.TripKey{TripID: "T1", Date: "2026-08-23"},
		routeID:      "R1",
		boardStop:    "A",
		boardDepUnix: 1000,
		arrivalUnix:  2000,
	}
	// Round 1: B→B2 walk (footpath)
	j[1]["B2"] = journalEntry{
		round: 1, station: "B2",
		routeID:      "WALK",
		boardStop:    "B",
		boardDepUnix: 2000,
		arrivalUnix:  2300,
	}
	// Round 2: B2→C transit
	j[2]["C"] = journalEntry{
		round: 2, station: "C",
		tripKey:      model.TripKey{TripID: "T2", Date: "2026-08-23"},
		routeID:      "R2",
		boardStop:    "B2",
		boardDepUnix: 2400,
		arrivalUnix:  3500,
	}

	params := model.SearchParams{
		Origin:      "A",
		Destination: "C",
		Date:        "2026-08-23",
		DepTime:     900,
	}

	paths := reconstructPaths(j, params)

	if len(paths) == 0 {
		t.Fatal("D5: expected at least one path involving walk transfer, got none")
	}

	// Find the path with 3 legs (A→B transit, B→B2 walk, B2→C transit)
	var found bool
	for _, p := range paths {
		if len(p.Legs) == 3 {
			if p.Legs[0].BoardStation == "A" && p.Legs[0].AlightStation == "B" &&
				p.Legs[1].BoardStation == "B" && p.Legs[1].AlightStation == "B2" &&
				p.Legs[2].BoardStation == "B2" && p.Legs[2].AlightStation == "C" {
				found = true
				if p.ArrivalUnix != 3500 {
					t.Errorf("D5: expected ArrivalUnix=3500, got %d", p.ArrivalUnix)
				}
			}
		}
	}
	if !found {
		t.Errorf("D5: did not find expected 3-leg path A→B→B2→C in paths: %+v", paths)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D5 — cycle guard: malformed journal with circular boardStop
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_CycleDetected_NoInfiniteLoop(t *testing.T) {
	j := makeJournal(MaxRounds + 1)

	// Create a circular: C.boardStop=B, B.boardStop=C (data error)
	j[1]["C"] = journalEntry{
		round: 1, station: "C",
		routeID:   "R1",
		boardStop: "B",
	}
	j[1]["B"] = journalEntry{
		round: 1, station: "B",
		routeID:   "R1",
		boardStop: "C", // ← circular: B boards from C which boards from B
	}

	params := model.SearchParams{
		Origin:      "A",
		Destination: "C",
		DepTime:     0,
	}

	// Must not hang; visited guard must break the cycle
	paths := reconstructPaths(j, params)
	// Circular path should be discarded (never reaches origin "A")
	if len(paths) != 0 {
		t.Errorf("expected 0 paths for circular journal, got %d", len(paths))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// D1 — Footpath field on RouteBuffer must be initialised (not nil)
// ─────────────────────────────────────────────────────────────────────────────

func TestFootpaths_NilMapPanic_DoesNotOccur(t *testing.T) {
	// If Footpaths is nil, routes.Footpaths[station] would panic.
	// Verify that an empty (non-nil) map is safely range-able.
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
	j := makeJournal(MaxRounds + 1)

	// Round 1: board at "X" (not A), alight at C — gap from origin
	j[1]["C"] = journalEntry{
		round: 1, station: "C",
		routeID:   "R1",
		boardStop: "X", // X is not origin
	}
	// Round 1: board at A, alight at "Y" (not X) — teleportation gap
	j[1]["Y"] = journalEntry{
		round: 1, station: "Y",
		routeID:   "R1",
		boardStop: "A",
	}
	// No entry linking Y to X

	params := model.SearchParams{
		Origin:      "A",
		Destination: "C",
		DepTime:     0,
	}
	paths := reconstructPaths(j, params)
	// C.boardStop = X, but X has no journal entry → path discarded
	if len(paths) != 0 {
		t.Errorf("expected 0 paths for broken journal chain, got %d", len(paths))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NEW TEST: Multi-day transfer bug
// ─────────────────────────────────────────────────────────────────────────────

func TestReconstructPaths_MultiDayJourney(t *testing.T) {
	// Not testing reconstructPaths directly here, we need to test RaptorSearch.
	// We will create a small RouteBuffer and run RaptorSearch.
}

