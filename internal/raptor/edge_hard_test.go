package raptor

import (
	"math"
	"testing"

	"axentra/internal/model"
)

// The second pass of edge cases: multi-day journeys, calendar handling, extreme
// values, and network shapes that break the usual "scan each route once"
// assumption.

// A journey whose legs fall on different operating dates. The date window has
// to admit the later leg, and the arithmetic has to stay monotonic across the
// boundary.
func TestHard_JourneySpansCalendarDays(t *testing.T) {
	g := newGraph()
	// Arrives B at 23:30 today.
	g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(22, 0), at(23, 30)}),
	}, true)
	// Leaves B at 06:00 TOMORROW, on tomorrow's operating date.
	g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
		simpleTST("T2", dayString(1), []string{"B", "C"}, []int64{atDay(1, 6, 0), atDay(1, 9, 0)}),
	}, true)
	g.publish(nil)

	got := query("A", "C", at(21, 0), nil)
	if len(got) != 1 {
		t.Fatalf("an overnight connection should be found, got %d journeys", len(got))
	}
	p := got[0]
	if bad := checkJourney(p, "A", "C", at(21, 0), model.DefaultMaxRounds, 0); bad != "" {
		t.Fatal(bad)
	}
	if p.ArrivalUnix != atDay(1, 9, 0) {
		t.Errorf("arrival %d, want %d", p.ArrivalUnix, atDay(1, 9, 0))
	}
	if p.Legs[0].Date != testDate || p.Legs[1].Date != dayString(1) {
		t.Errorf("leg dates %s then %s, want %s then %s", p.Legs[0].Date, p.Legs[1].Date, testDate, dayString(1))
	}
	if p.TotalTimeSeconds != 11*3600 {
		t.Errorf("total time %ds, want %ds", p.TotalTimeSeconds, 11*3600)
	}
}

// The same trip ID running on several dates — exactly what the seeder produces.
// The engine must pick the right day's service, not blend them.
func TestHard_SameTripIDOnManyDates(t *testing.T) {
	var trips []model.TripStopTimes
	for day := 0; day <= 2; day++ {
		trips = append(trips, simpleTST("DAILY", dayString(day), []string{"A", "B"},
			[]int64{atDay(day, 9, 0), atDay(day, 10, 0)}))
	}
	newGraph().addRoute("R", []string{"A", "B"}, trips, true).publish(nil)

	for day := 0; day <= 2; day++ {
		got := query("A", "B", atDay(day, 8, 0), nil)
		if len(got) != 1 {
			t.Fatalf("day %d: want 1 journey, got %d", day, len(got))
		}
		if got[0].Legs[0].Date != dayString(day) {
			t.Errorf("day %d: leg carries date %s", day, got[0].Legs[0].Date)
		}
		if got[0].ArrivalUnix != atDay(day, 10, 0) {
			t.Errorf("day %d: arrival %d, want %d", day, got[0].ArrivalUnix, atDay(day, 10, 0))
		}
		if bad := checkJourney(got[0], "A", "B", atDay(day, 8, 0), model.DefaultMaxRounds, 0); bad != "" {
			t.Errorf("day %d: %s", day, bad)
		}
	}
}

// Riding a route, walking away, and coming back to the same route later. The
// route queue holds one boarding position per route per round, so this only
// works if the second boarding happens in a later round at the right position.
func TestHard_SameRouteBoardedTwice(t *testing.T) {
	// Route R calls A, B, C, D. There is no through service: the only trip
	// covering A→B leaves early, and the only one covering C→D leaves later.
	// A walk links B to C.
	g := newGraph()
	g.addRoute("R", []string{"A", "B", "C", "D"}, []model.TripStopTimes{
		// Early trip: useful for A→B only (it passes C and D too early to matter).
		simpleTST("EARLY", testDate, []string{"A", "B", "C", "D"},
			[]int64{at(8, 0), at(8, 30), at(8, 40), at(8, 50)}),
		// Late trip: the one that actually carries C→D at a useful hour.
		simpleTST("LATE", testDate, []string{"A", "B", "C", "D"},
			[]int64{at(11, 0), at(11, 30), at(12, 0), at(12, 30)}),
	}, true)
	g.walk("B", "C", 300)
	g.walk("C", "B", 300)
	g.publish(nil)

	got := query("A", "D", at(7, 0), nil)
	if len(got) == 0 {
		t.Fatal("A → D should be reachable")
	}
	for _, p := range got {
		if bad := checkJourney(p, "A", "D", at(7, 0), model.DefaultMaxRounds, 0); bad != "" {
			t.Errorf("%s\n  legs: %s", bad, legSummary(p))
		}
	}
	// The through ride on EARLY arrives 08:50 and beats everything else.
	if got[0].ArrivalUnix != at(8, 50) {
		t.Errorf("best arrival %d, want %d (%s)", got[0].ArrivalUnix, at(8, 50), legSummary(got[0]))
	}
}

// Boarding the same route in a later round after arriving on foot.
func TestHard_ReboardAfterWalking(t *testing.T) {
	// The only way from A to Z: ride R1 to M, walk to N, ride R2 to Z. R2 also
	// calls at a stop before N that the passenger can never reach, so the route
	// must be entered at N specifically.
	g := newGraph()
	g.addRoute("R1", []string{"A", "M"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "M"}, []int64{at(8, 0), at(9, 0)})}, true)
	g.addRoute("R2", []string{"UNREACHABLE", "N", "Z"}, []model.TripStopTimes{
		simpleTST("T2", testDate, []string{"UNREACHABLE", "N", "Z"},
			[]int64{at(8, 0), at(9, 30), at(10, 30)})}, true)
	g.walk("M", "N", 600)
	g.walk("N", "M", 600)
	g.publish(nil)

	got := query("A", "Z", at(7, 0), nil)
	if len(got) != 1 {
		t.Fatalf("want 1 journey, got %d", len(got))
	}
	if bad := checkJourney(got[0], "A", "Z", at(7, 0), model.DefaultMaxRounds, 0); bad != "" {
		t.Fatal(bad)
	}
	if want := "T1:A>M|WALK:M>N|T2:N>Z"; legSummary(got[0]) != want {
		t.Errorf("legs %s, want %s", legSummary(got[0]), want)
	}
}

// The round limit is clamped, and asking for an absurd number must not blow up
// memory or produce a journey longer than the clamp allows.
func TestHard_RoundLimitClamp(t *testing.T) {
	g := newGraph()
	for i := 0; i < 30; i++ {
		from, to := string(rune('a'+i)), string(rune('a'+i+1))
		g.addRoute("R"+from, []string{from, to}, []model.TripStopTimes{
			simpleTST("T"+from, testDate, []string{from, to},
				[]int64{at(6, 0) + int64(i)*600, at(6, 0) + int64(i)*600 + 300}),
		}, true)
	}
	g.publish(nil)

	for _, rounds := range []int{model.MaxAllowedRounds, model.MaxAllowedRounds + 1, 1000, math.MaxInt32} {
		got := query("a", string(rune('a'+model.MaxAllowedRounds)), at(5, 0),
			func(p *model.SearchParams) { p.MaxRounds = rounds })
		if len(got) != 1 {
			t.Errorf("max_rounds=%d: a journey needing exactly %d vehicles should be found, got %d",
				rounds, model.MaxAllowedRounds, len(got))
			continue
		}
		transit := 0
		for _, l := range got[0].Legs {
			if l.Kind == model.LegTransit {
				transit++
			}
		}
		if transit > model.MaxAllowedRounds {
			t.Errorf("max_rounds=%d: journey uses %d vehicles, over the clamp of %d",
				rounds, transit, model.MaxAllowedRounds)
		}
	}

	// One more vehicle than the clamp allows must find nothing.
	got := query("a", string(rune('a'+model.MaxAllowedRounds+1)), at(5, 0),
		func(p *model.SearchParams) { p.MaxRounds = 1000 })
	if len(got) != 0 {
		t.Errorf("a journey needing %d vehicles is past the clamp, got %s",
			model.MaxAllowedRounds+1, legSummary(got[0]))
	}
}

// Far-future timestamps, and a transfer buffer big enough to overflow careless
// arithmetic.
func TestHard_ExtremeValues(t *testing.T) {
	t.Run("year 2400 timestamps", func(t *testing.T) {
		far := int64(13569465600) // 2400-01-01
		newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{far, far + 3600}),
		}, true).publish(nil)
		got := query("A", "B", far-3600, nil)
		if len(got) != 1 {
			t.Fatalf("want 1 journey, got %d", len(got))
		}
		if got[0].ArrivalUnix != far+3600 {
			t.Errorf("arrival %d, want %d", got[0].ArrivalUnix, far+3600)
		}
	})

	t.Run("an enormous transfer buffer does not overflow", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
			simpleTST("T2", testDate, []string{"B", "C"}, []int64{at(11, 0), at(12, 0)})}, true)
		g.publish(nil)
		for _, buf := range []int{math.MaxInt32, math.MaxInt32 - 1, 1 << 30} {
			got := query("A", "C", at(8, 0), func(p *model.SearchParams) { p.MinTransferSeconds = buf })
			if len(got) != 0 {
				t.Errorf("buffer=%d makes the connection impossible, got %s", buf, legSummary(got[0]))
			}
		}
	})

	t.Run("an enormous footpath does not overflow", func(t *testing.T) {
		g := newGraph()
		g.addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)})}, true)
		g.walk("A", "FAR", math.MaxInt32)
		g.publish(nil)
		got := query("A", "FAR", at(8, 0), nil)
		for _, p := range got {
			if p.ArrivalUnix < p.DepartureUnix {
				t.Errorf("arrival %d is before departure %d — the walk arithmetic wrapped",
					p.ArrivalUnix, p.DepartureUnix)
			}
		}
	})
}

// Two trips with byte-identical times: the result must not contain the journey
// twice.
func TestHard_DuplicateTripsAreDeduplicated(t *testing.T) {
	newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("TWIN_1", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		simpleTST("TWIN_2", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
	}, true).publish(nil)

	got := query("A", "B", at(8, 0), nil)
	if len(got) != 1 {
		t.Errorf("identical services should collapse to one journey, got %d", len(got))
		for _, p := range got {
			t.Logf("  %s arriving %d", legSummary(p), p.ArrivalUnix)
		}
	}
}

// A route that runs the wrong way for this query must not be usable.
func TestHard_WrongDirection(t *testing.T) {
	newGraph().addRoute("R", []string{"A", "B", "C"}, []model.TripStopTimes{
		simpleTST("T", testDate, []string{"A", "B", "C"}, []int64{at(9, 0), at(10, 0), at(11, 0)}),
	}, true).publish(nil)

	if got := query("C", "A", at(8, 0), nil); len(got) != 0 {
		t.Errorf("the route only runs A→C, got %s", legSummary(got[0]))
	}
	if got := query("B", "A", at(8, 0), nil); len(got) != 0 {
		t.Errorf("the route only runs forward, got %s", legSummary(got[0]))
	}
	if got := query("B", "C", at(8, 0), nil); len(got) != 1 {
		t.Errorf("B→C is served, got %d journeys", len(got))
	}
}
