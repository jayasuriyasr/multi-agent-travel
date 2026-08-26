package raptor

import (
	"context"
	"fmt"
	"testing"

	"axentra/internal/model"
	"axentra/internal/state"
)

// buildNetwork assembles the multi-scenario fixture network used below.
//
// Every timestamp is a real unix time on the trip's own operating date. The
// previous fixture used bare seconds-since-midnight while labelling trips with
// a 2026 date, which meant the overnight and multi-day scenarios never touched
// the calendar-window logic they were supposed to be testing.
func buildNetwork() *graph {
	g := newGraph()

	// 1. Direct vs transfer: the connection arrives earlier but costs a change.
	g.addRoute("R_DIRECT", []string{"A", "C"}, []model.TripStopTimes{
		simpleTST("T_DIRECT", testDate, []string{"A", "C"}, []int64{at(9, 0), at(13, 0)}),
	}, true)
	g.addRoute("R_A_B", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T_A_B", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
	}, true)
	g.addRoute("R_B_C", []string{"B", "C"}, []model.TripStopTimes{
		simpleTST("T_B_C", testDate, []string{"B", "C"}, []int64{at(10, 30), at(11, 30)}),
	}, true)

	// 2. Footpath transfer: walking from E to E_WALK beats staying aboard.
	g.addRoute("R_D_E", []string{"D", "E"}, []model.TripStopTimes{
		simpleTST("T_D_E", testDate, []string{"D", "E"}, []int64{at(9, 0), at(9, 30)}),
	}, true)
	g.addRoute("R_EW_F", []string{"E_WALK", "F"}, []model.TripStopTimes{
		simpleTST("T_EW_F", testDate, []string{"E_WALK", "F"}, []int64{at(10, 0), at(10, 30)}),
	}, true)
	g.addRoute("R_D_F", []string{"D", "F"}, []model.TripStopTimes{
		simpleTST("T_D_F", testDate, []string{"D", "F"}, []int64{at(9, 0), at(12, 0)}),
	}, true)
	g.walk("E", "E_WALK", 600)

	// 3. Overnight: departs 23:00, arrives 02:00 on the NEXT calendar day.
	g.addRoute("R_OVN", []string{"G", "H"}, []model.TripStopTimes{
		simpleTST("T_OVN", testDate, []string{"G", "H"}, []int64{at(23, 0), atDay(1, 2, 0)}),
	}, true)

	// 4. Seat availability: the fast service is full, the slow one is not.
	g.addRoute("R_I_J", []string{"I", "J"}, []model.TripStopTimes{
		simpleTST("T_IJ_FAST", testDate, []string{"I", "J"}, []int64{at(9, 0), at(10, 0)}),
		simpleTST("T_IJ_SLOW", testDate, []string{"I", "J"}, []int64{at(9, 30), at(11, 0)}),
	}, true)

	// 5. Missed connection: the onward service leaves one minute too early.
	g.addRoute("R_J2_K", []string{"J2", "K"}, []model.TripStopTimes{
		simpleTST("T_J2_K", testDate, []string{"J2", "K"}, []int64{at(11, 0), at(12, 0)}),
	}, true)
	g.addRoute("R_K_L", []string{"K", "L"}, []model.TripStopTimes{
		simpleTST("T_KL_MISSED", testDate, []string{"K", "L"}, []int64{at(11, 59), at(13, 0)}),
		simpleTST("T_KL_CATCH", testDate, []string{"K", "L"}, []int64{at(12, 30), at(13, 30)}),
	}, true)

	// 6. Cycle: the route returns to where it started.
	g.addRoute("R_CYCLE", []string{"M", "N", "O", "M"}, []model.TripStopTimes{
		simpleTST("T_CYCLE", testDate, []string{"M", "N", "O", "M"},
			[]int64{at(9, 0), at(9, 30), at(10, 0), at(10, 30)}),
	}, true)

	// 7. Chain of five separate services: Q→R→S→T→U→V needs five vehicles.
	for i, pair := range [][2]string{{"Q", "R"}, {"R", "S"}, {"S", "T"}, {"T", "U"}, {"U", "V"}} {
		g.addRoute("R_"+pair[0]+"_"+pair[1], []string{pair[0], pair[1]}, []model.TripStopTimes{
			simpleTST("T_"+pair[0]+"_"+pair[1], testDate, []string{pair[0], pair[1]},
				[]int64{at(1+i, 0), at(2+i, 0)}),
		}, true)
	}

	// 8. Stale data: zero seats but flagged stale, so boarding stays optimistic.
	g.addRoute("R_V2_W", []string{"V2", "W"}, []model.TripStopTimes{
		simpleTST("T_VW_STALE", testDate, []string{"V2", "W"}, []int64{at(9, 0), at(10, 0)}),
		simpleTST("T_VW_FRESH", testDate, []string{"V2", "W"}, []int64{at(9, 30), at(11, 0)}),
	}, true)

	// 9. Earlier departure does not imply earlier arrival — separate routes,
	//    because RAPTOR assumes FIFO within a single route.
	g.addRoute("R_X_Y_SLOW", []string{"X", "Y"}, []model.TripStopTimes{
		simpleTST("T_XY_SLOW", testDate, []string{"X", "Y"}, []int64{at(8, 0), at(12, 0)}),
	}, true)
	g.addRoute("R_X_Y_FAST", []string{"X", "Y"}, []model.TripStopTimes{
		simpleTST("T_XY_FAST", testDate, []string{"X", "Y"}, []int64{at(10, 0), at(11, 0)}),
	}, true)

	// 10. Multi-day: leg two runs on the following calendar day.
	g.addRoute("R_P1", []string{"P", "P2"}, []model.TripStopTimes{
		simpleTST("T_P1", testDate, []string{"P", "P2"}, []int64{at(20, 0), atDay(1, 4, 0)}),
	}, true)
	g.addRoute("R_P2", []string{"P2", "P3"}, []model.TripStopTimes{
		simpleTST("T_P2", dayString(1), []string{"P2", "P3"}, []int64{atDay(1, 6, 0), atDay(1, 9, 0)}),
	}, true)

	return g
}

func networkSignals() state.SignalBuffer {
	return state.SignalBuffer{
		model.TripKey{TripID: "T_IJ_FAST", Date: testDate}:  {ByClass: map[string]int{"lower": 0}},
		model.TripKey{TripID: "T_IJ_SLOW", Date: testDate}:  {ByClass: map[string]int{"lower": 10}},
		model.TripKey{TripID: "T_VW_STALE", Date: testDate}: {ByClass: map[string]int{"lower": 0}, Stale: true},
		model.TripKey{TripID: "T_VW_FRESH", Date: testDate}: {ByClass: map[string]int{"lower": 10}},
	}
}

func publishNetwork() { buildNetwork().publish(networkSignals()) }

func TestNetwork_Scenarios(t *testing.T) {
	publishNetwork()

	t.Run("direct and connecting services form a Pareto frontier", func(t *testing.T) {
		got := search(t, "A", "C", at(8, 0), nil)
		if len(got) != 2 {
			t.Fatalf("want 2 Pareto options, got %d: %v", len(got), summaries(got))
		}
		// Fastest first: the connection arrives 11:30 with one change; the
		// direct arrives 13:00 with none. Neither dominates the other.
		if got[0].ArrivalUnix != at(11, 30) || got[0].Transfers != 1 {
			t.Errorf("first option = arrive %d transfers %d, want 11:30 / 1", got[0].ArrivalUnix, got[0].Transfers)
		}
		if got[1].ArrivalUnix != at(13, 0) || got[1].Transfers != 0 {
			t.Errorf("second option = arrive %d transfers %d, want 13:00 / 0", got[1].ArrivalUnix, got[1].Transfers)
		}
	})

	t.Run("walking to a nearby station beats staying aboard", func(t *testing.T) {
		got := search(t, "D", "F", at(8, 0), nil)
		if len(got) == 0 {
			t.Fatal("expected at least one option")
		}
		best := got[0]
		if best.ArrivalUnix != at(10, 30) {
			t.Fatalf("want arrival 10:30 via the walk, got %d (%s)", best.ArrivalUnix, legSummary(best))
		}
		if best.WalkSeconds != 600 {
			t.Errorf("want 600s of walking, got %d", best.WalkSeconds)
		}
		var sawWalk bool
		for _, l := range best.Legs {
			if l.Kind == model.LegWalk {
				sawWalk = true
			}
		}
		if !sawWalk {
			t.Errorf("expected a walk leg in %s", legSummary(best))
		}
	})

	t.Run("overnight service crosses midnight", func(t *testing.T) {
		got := search(t, "G", "H", at(22, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d", len(got))
		}
		if got[0].ArrivalUnix != atDay(1, 2, 0) {
			t.Errorf("want arrival 02:00 next day (%d), got %d", atDay(1, 2, 0), got[0].ArrivalUnix)
		}
		if got[0].TotalTimeSeconds != 3*3600 {
			t.Errorf("want a 3h journey, got %ds", got[0].TotalTimeSeconds)
		}
	})

	t.Run("multi-day journey uses a trip on the following date", func(t *testing.T) {
		got := search(t, "P", "P3", at(19, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d: %v", len(got), summaries(got))
		}
		if got[0].ArrivalUnix != atDay(1, 9, 0) {
			t.Errorf("want arrival 09:00 next day, got %d", got[0].ArrivalUnix)
		}
		if got[0].Legs[1].Date != dayString(1) {
			t.Errorf("second leg should run on %s, got %s", dayString(1), got[0].Legs[1].Date)
		}
	})

	t.Run("a full train is skipped for a later one with seats", func(t *testing.T) {
		got := search(t, "I", "J", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d", len(got))
		}
		if id := got[0].Legs[0].TripID; id != "T_IJ_SLOW" {
			t.Errorf("want T_IJ_SLOW (T_IJ_FAST is full), got %s", id)
		}
	})

	t.Run("stale zero-seat data still boards optimistically", func(t *testing.T) {
		got := search(t, "V2", "W", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d", len(got))
		}
		if id := got[0].Legs[0].TripID; id != "T_VW_STALE" {
			t.Errorf("want T_VW_STALE (stale data must not block), got %s", id)
		}
	})

	t.Run("a connection missed by one minute falls through to the next", func(t *testing.T) {
		got := search(t, "J2", "L", at(10, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d", len(got))
		}
		if id := got[0].Legs[1].TripID; id != "T_KL_CATCH" {
			t.Errorf("want T_KL_CATCH, got %s", id)
		}
		if got[0].ArrivalUnix != at(13, 30) {
			t.Errorf("want arrival 13:30, got %d", got[0].ArrivalUnix)
		}
	})

	t.Run("a looping route does not loop the search", func(t *testing.T) {
		got := search(t, "M", "O", at(8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("want 1 option, got %d", len(got))
		}
		if got[0].ArrivalUnix != at(10, 0) {
			t.Errorf("want arrival 10:00, got %d", got[0].ArrivalUnix)
		}
	})

	t.Run("earlier departure does not win when a later service arrives first", func(t *testing.T) {
		got := search(t, "X", "Y", at(7, 0), nil)
		if len(got) == 0 {
			t.Fatal("expected an option")
		}
		if id := got[0].Legs[0].TripID; id != "T_XY_FAST" {
			t.Errorf("want T_XY_FAST (arrives 11:00), got %s", id)
		}
	})

	t.Run("a journey needing more vehicles than max_rounds is not returned", func(t *testing.T) {
		// Q→V needs five vehicles.
		if got := search(t, "Q", "V", at(0, 0), func(p *model.SearchParams) { p.MaxRounds = 4 }); len(got) != 0 {
			t.Fatalf("want no options at max_rounds=4, got %d: %v", len(got), summaries(got))
		}
		got := search(t, "Q", "V", at(0, 0), func(p *model.SearchParams) { p.MaxRounds = 5 })
		if len(got) != 1 {
			t.Fatalf("want 1 option at max_rounds=5, got %d", len(got))
		}
		if n := got[0].TransitLegs(); n != 5 {
			t.Errorf("want 5 transit legs, got %d", n)
		}
		if got[0].Transfers != 4 {
			t.Errorf("want 4 transfers, got %d", got[0].Transfers)
		}
	})

	t.Run("unreachable destination yields nothing", func(t *testing.T) {
		if got := search(t, "A", "V", at(8, 0), nil); len(got) != 0 {
			t.Fatalf("want no options, got %d", len(got))
		}
	})
}

// Regression for the defect that made this engine unusable: round k boarded
// off the global best-arrival map, which the same round was still mutating.
// Whether two legs collapsed into one round then depended on Go's randomised
// map iteration order, so the same query returned different answers run to run
// and could exceed max_rounds.
//
// The fixture is the adversarial shape: every route also calls at the origin,
// so all of them land in the round-1 queue together.
func chainGraph() *graph {
	g := newGraph()
	hop := func(id string, stops []string, times []int64) []model.TripStopTimes {
		return []model.TripStopTimes{simpleTST(id, testDate, stops, times)}
	}
	g.addRoute("R_AB", []string{"A", "B"}, hop("T_AB", []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}), true)
	g.addRoute("R_BC", []string{"A", "B", "C"}, hop("T_BC", []string{"A", "B", "C"}, []int64{at(1, 0), at(10, 30), at(11, 0)}), true)
	g.addRoute("R_CD", []string{"A", "C", "D"}, hop("T_CD", []string{"A", "C", "D"}, []int64{at(1, 0), at(11, 30), at(12, 0)}), true)
	g.addRoute("R_DE", []string{"A", "D", "E"}, hop("T_DE", []string{"A", "D", "E"}, []int64{at(1, 0), at(12, 30), at(13, 0)}), true)
	g.addRoute("R_EF", []string{"A", "E", "F"}, hop("T_EF", []string{"A", "E", "F"}, []int64{at(1, 0), at(13, 30), at(14, 0)}), true)
	return g
}

func TestSearch_IsDeterministic(t *testing.T) {
	chainGraph().publish(nil)

	const runs = 500
	first := ""
	for i := 0; i < runs; i++ {
		got := search(t, "A", "F", at(9, 0), func(p *model.SearchParams) { p.MaxRounds = 5 })
		key := fmt.Sprint(summaries(got))
		if i == 0 {
			first = key
			continue
		}
		if key != first {
			t.Fatalf("run %d returned a different answer for identical input:\n  first: %s\n  now:   %s", i, first, key)
		}
	}
}

func TestSearch_RoundBoundIsEnforced(t *testing.T) {
	chainGraph().publish(nil)

	// A→F needs five vehicles. At max_rounds=4 it must not appear at all, no
	// matter how the routes happen to be ordered internally.
	for i := 0; i < 200; i++ {
		got := search(t, "A", "F", at(9, 0), func(p *model.SearchParams) { p.MaxRounds = 4 })
		if len(got) != 0 {
			t.Fatalf("run %d: a 5-vehicle journey escaped a 4-round limit: %v", i, summaries(got))
		}
	}

	got := search(t, "A", "F", at(9, 0), func(p *model.SearchParams) { p.MaxRounds = 5 })
	if len(got) != 1 {
		t.Fatalf("want 1 option at max_rounds=5, got %d", len(got))
	}
	if n := got[0].TransitLegs(); n != 5 || got[0].Transfers != 4 {
		t.Errorf("want 5 legs / 4 transfers, got %d legs / %d transfers", n, got[0].Transfers)
	}
}

// Every returned journey must be internally consistent: legs join end to end,
// times move forwards, and it starts and ends where the caller asked.
func TestSearch_ResultsAreWellFormed(t *testing.T) {
	publishNetwork()

	pairs := [][2]string{{"A", "C"}, {"D", "F"}, {"G", "H"}, {"J2", "L"}, {"P", "P3"}, {"X", "Y"}}
	for _, pr := range pairs {
		params := model.SearchParams{
			Origin: pr[0], Destination: pr[1], Date: testDate,
			DepTime: at(0, 0), SeatClass: "lower", Passengers: 1, MaxRounds: 6,
		}
		for _, p := range RaptorSearch(context.Background(), params, 100) {
			if err := ValidatePath(p, params); err != nil {
				t.Errorf("%s→%s produced a malformed path (%s): %v", pr[0], pr[1], legSummary(p), err)
			}
			if p.TotalTimeSeconds != p.ArrivalUnix-p.DepartureUnix {
				t.Errorf("%s→%s: total time does not match departure/arrival", pr[0], pr[1])
			}
			if p.WaitSeconds < 0 {
				t.Errorf("%s→%s: negative wait time %d", pr[0], pr[1], p.WaitSeconds)
			}
		}
	}
}

func TestSearch_ParetoFrontierIsStrictlyOrdered(t *testing.T) {
	publishNetwork()

	got := search(t, "A", "C", at(8, 0), nil)
	for i := 1; i < len(got); i++ {
		if got[i].ArrivalUnix <= got[i-1].ArrivalUnix {
			t.Errorf("arrivals must strictly increase down the frontier: %v", summaries(got))
		}
		if got[i].Transfers >= got[i-1].Transfers {
			t.Errorf("transfers must strictly decrease down the frontier: %v", summaries(got))
		}
		if got[i-1].Dominates(got[i]) || got[i].Dominates(got[i-1]) {
			t.Errorf("frontier contains a dominated option: %v", summaries(got))
		}
	}
}

// A minimum transfer time must actually prevent connections that are too tight.
func TestSearch_MinTransferTimeIsHonoured(t *testing.T) {
	g := newGraph()
	g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
	}, true)
	g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
		// Five minutes to change platforms.
		simpleTST("T2", testDate, []string{"B", "C"}, []int64{at(10, 5), at(11, 0)}),
		simpleTST("T3", testDate, []string{"B", "C"}, []int64{at(10, 30), at(11, 30)}),
	}, true)
	g.publish(nil)

	tight := search(t, "A", "C", at(8, 0), nil)
	if len(tight) == 0 || tight[0].Legs[1].TripID != "T2" {
		t.Fatalf("with no transfer buffer the 5-minute connection should be used, got %v", summaries(tight))
	}

	relaxed := search(t, "A", "C", at(8, 0), func(p *model.SearchParams) { p.MinTransferSeconds = 600 })
	if len(relaxed) == 0 {
		t.Fatal("expected an option with a 10-minute transfer buffer")
	}
	if id := relaxed[0].Legs[1].TripID; id != "T3" {
		t.Errorf("a 10-minute buffer should rule out the 5-minute connection, got %s", id)
	}
}

// Walking must chain across a transitively closed footpath graph.
func TestSearch_MultiHopWalkUsesClosure(t *testing.T) {
	g := newGraph()
	g.addRoute("R_IN", []string{"START", "W1"}, []model.TripStopTimes{
		simpleTST("T_IN", testDate, []string{"START", "W1"}, []int64{at(9, 0), at(9, 30)}),
	}, true)
	g.addRoute("R_OUT", []string{"W3", "END"}, []model.TripStopTimes{
		simpleTST("T_OUT", testDate, []string{"W3", "END"}, []int64{at(10, 0), at(10, 30)}),
	}, true)
	// The loader publishes the closure; W1→W3 exists as a single 600s edge.
	g.walk("W1", "W2", 300).walk("W1", "W3", 600).walk("W2", "W3", 300)
	g.publish(nil)

	got := search(t, "START", "END", at(8, 0), nil)
	if len(got) != 1 {
		t.Fatalf("want 1 option, got %d: %v", len(got), summaries(got))
	}
	if got[0].ArrivalUnix != at(10, 30) {
		t.Errorf("want arrival 10:30, got %d (%s)", got[0].ArrivalUnix, legSummary(got[0]))
	}
}

func summaries(paths []model.Path) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, fmt.Sprintf("arr=%d transfers=%d legs=%s", p.ArrivalUnix, p.Transfers, legSummary(p)))
	}
	return out
}
