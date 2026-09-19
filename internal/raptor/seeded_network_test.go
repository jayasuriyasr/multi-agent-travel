package raptor

import (
	"fmt"
	"testing"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
)

// The demo network from internal/schedule/seeder.go, rebuilt in memory so the
// journeys it is supposed to produce can be pinned without a database.
//
// It is worth having as a fixture because it is the network every reviewer,
// screenshot and piece of documentation refers to, and because it is the only
// one that exercises all five features at once: a non-FIFO route, footpaths
// that beat staying on the train, an overnight service that crosses a calendar
// boundary, target pruning, and a three-entry Pareto frontier.
func buildSeededNetwork() {
	const dwell int64 = 300

	type routeDef struct {
		id         string
		stops      []string
		departures []float64
		hopSeconds int64
		speedups   []float64
	}
	routes := []routeDef{
		{"ROUTE-EXP-1", []string{"STA-001", "STA-004"},
			[]float64{8, 10, 12, 14, 16}, 3 * 3600, []float64{1, 1, 1, 1, 1}},
		{"ROUTE-LOC-1", []string{"STA-001", "STA-002", "STA-003", "STA-004"},
			[]float64{7, 9, 11, 13, 15}, 3600, []float64{1, 1, 1, 1, 1}},
		{"ROUTE-NOR-1", []string{"STA-005", "STA-006", "STA-007", "STA-001"},
			[]float64{6, 8, 10, 12, 14}, 3600, []float64{1, 1, 1, 1, 1}},
		// Deliberately non-FIFO: the 09:00 service runs the same track at 0.4×
		// the running time and passes the 08:00 local before Trenton.
		{"ROUTE-MIX-1", []string{"STA-008", "STA-010", "STA-002"},
			[]float64{8, 9, 11, 13}, 5400, []float64{1, 0.4, 1, 0.5}},
		{"ROUTE-OVN-1", []string{"STA-005", "STA-001", "STA-004"},
			[]float64{22, 23.5}, 4 * 3600, []float64{1, 1}},
	}

	g := newGraph()
	for _, r := range routes {
		var trips []model.TripStopTimes
		for day := 0; day < 3; day++ {
			for ti, depHour := range r.departures {
				speed := 1.0
				if ti < len(r.speedups) && r.speedups[ti] > 0 {
					speed = r.speedups[ti]
				}
				hop := int64(float64(r.hopSeconds) * speed)
				start := at(0, 0) + int64(day)*86400 + int64(depHour*3600)

				arrivals := make([]int64, len(r.stops))
				departures := make([]int64, len(r.stops))
				for seq := range r.stops {
					// Same arithmetic as the seeder: dwell at every stop but the
					// first, with later stops shifted by the accumulated dwell.
					arr := start + int64(seq)*hop
					dep := arr
					if seq > 0 {
						dep = arr + dwell
					}
					arrivals[seq] = arr + int64(seq)*dwell
					departures[seq] = dep + int64(seq)*dwell
				}
				trips = append(trips, makeTST(
					model.TripKey{
						TripID: fmt.Sprintf("%s_%s_T%02d", r.id, dayString(day), ti+1),
						Date:   dayString(day),
					}, r.stops, arrivals, departures))
			}
		}
		g.addRoute(r.id, r.stops, trips, trueFIFO(trips))
	}

	// Three disjoint walk pairs, so the graph is already its own closure.
	for _, fp := range []struct {
		from, to string
		seconds  int
	}{
		{"STA-009", "STA-001", 600}, {"STA-001", "STA-009", 600}, // Newark ↔ New York
		{"STA-010", "STA-002", 720}, {"STA-002", "STA-010", 720}, // Trenton ↔ Philadelphia
		{"STA-008", "STA-007", 480}, {"STA-007", "STA-008", 480}, // Stamford ↔ New Haven
	} {
		g.walk(fp.from, fp.to, fp.seconds)
	}
	g.publish(nil)
}

// hhmm renders a timestamp relative to the fixture's first day, as "12:15" or
// "06:10+1" for the next calendar day.
func hhmm(unix int64) string {
	delta := unix - at(0, 0)
	day, rem := delta/86400, delta%86400
	out := time.Unix(0, 0).UTC().Add(time.Duration(rem) * time.Second).Format("15:04")
	if day > 0 {
		out += fmt.Sprintf("+%d", day)
	}
	return out
}

// TestSeededNetwork_BostonToDC pins the frontier the demo query returns.
//
// Each entry is doing a different job:
//
//	12:15, 2 transfers — rides the Northern Local to New Haven, walks 8 minutes
//	  to Stamford, catches the 09:00 non-FIFO express (the later departure that
//	  arrives first), then walks 12 minutes Trenton → Philadelphia because that
//	  beats staying on the train, and finishes on the Regional.
//	13:05, 1 transfer  — the obvious ride: Northern Local to New York, express to DC.
//	06:10+1, 0 transfers — the overnight sleeper, whose stop times cross midnight
//	  and so depend on the calendar window being right.
func TestSeededNetwork_BostonToDC(t *testing.T) {
	buildSeededNetwork()

	want := []struct {
		arrival   string
		transfers int
		walk      int64
		legs      string
	}{
		{"12:15", 2, 1200,
			"ROUTE-NOR-1_" + dayString(0) + "_T01:STA-005>STA-007|WALK:STA-007>STA-008|" +
				"ROUTE-MIX-1_" + dayString(0) + "_T02:STA-008>STA-010|WALK:STA-010>STA-002|" +
				"ROUTE-LOC-1_" + dayString(0) + "_T02:STA-002>STA-004"},
		{"13:05", 1, 0,
			"ROUTE-NOR-1_" + dayString(0) + "_T01:STA-005>STA-001|" +
				"ROUTE-EXP-1_" + dayString(0) + "_T02:STA-001>STA-004"},
		{"06:10+1", 0, 0,
			"ROUTE-OVN-1_" + dayString(0) + "_T01:STA-005>STA-004"},
	}

	got := search(t, "STA-005", "STA-004", at(6, 0), nil)
	if len(got) != len(want) {
		for _, p := range got {
			t.Logf("  %s  transfers=%d  %s", hhmm(p.ArrivalUnix), p.Transfers, legSummary(p))
		}
		t.Fatalf("frontier has %d journeys, want %d", len(got), len(want))
	}
	for i, w := range want {
		p := got[i]
		if hhmm(p.ArrivalUnix) != w.arrival || p.Transfers != w.transfers || p.WalkSeconds != w.walk {
			t.Errorf("journey %d: arrives %s with %d transfers and %ds walking; want %s, %d, %ds",
				i, hhmm(p.ArrivalUnix), p.Transfers, p.WalkSeconds, w.arrival, w.transfers, w.walk)
		}
		if legSummary(p) != w.legs {
			t.Errorf("journey %d legs:\n got  %s\n want %s", i, legSummary(p), w.legs)
		}
	}
}

// The Shore Line is the reason the loader measures FIFO instead of assuming it.
func TestSeededNetwork_ShoreLineIsNotFIFO(t *testing.T) {
	buildSeededNetwork()
	routes := schedule.LiveRoutes()
	for i, r := range routes.Routes {
		if r.RouteID != "ROUTE-MIX-1" {
			continue
		}
		if routes.IsFIFO(i) {
			t.Error("ROUTE-MIX-1 should be measured non-FIFO: its 09:00 service overtakes the 08:00 one before Trenton")
		}
		return
	}
	t.Fatal("ROUTE-MIX-1 not found in the published buffer")
}
