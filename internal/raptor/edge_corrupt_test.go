package raptor

import (
	"testing"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// A partial or interrupted ingest can publish a buffer whose indexes disagree
// with each other. The search must survive all of it: a panic here takes down
// the process for every request, not just the malformed one.
func TestCorrupt_InconsistentBuffer(t *testing.T) {
	base := func() *schedule.RouteBuffer {
		b := &schedule.RouteBuffer{
			TripIndex:    map[model.TripKey]model.TripLocation{},
			StopToRoutes: map[string][]schedule.RouteStop{},
			Footpaths:    map[string][]model.Footpath{},
		}
		key := model.TripKey{TripID: "T", Date: testDate}
		b.Routes = append(b.Routes, model.RouteEntry{RouteID: "R", StopIDs: []string{"A", "B"}})
		b.StopTimes = append(b.StopTimes, []model.TripStopTimes{
			simpleTST("T", testDate, []string{"A", "B"}, []int64{at(9, 0), at(10, 0)}),
		})
		b.RouteFIFO = append(b.RouteFIFO, true)
		b.TripIndex[key] = model.TripLocation{RouteIdx: 0, TripIdx: 0}
		b.StopToRoutes["A"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: 0}}
		b.StopToRoutes["B"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: 1}}
		return b
	}

	cases := []struct {
		name    string
		corrupt func(*schedule.RouteBuffer)
	}{
		{"stop points at a route index that does not exist", func(b *schedule.RouteBuffer) {
			b.StopToRoutes["A"] = append(b.StopToRoutes["A"], schedule.RouteStop{RouteIdx: 99, StopPos: 0})
		}},
		{"stop points at a negative route index", func(b *schedule.RouteBuffer) {
			b.StopToRoutes["A"] = append(b.StopToRoutes["A"], schedule.RouteStop{RouteIdx: -1, StopPos: 0})
		}},
		{"stop position beyond the end of the route", func(b *schedule.RouteBuffer) {
			b.StopToRoutes["A"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: 50}}
		}},
		{"negative stop position", func(b *schedule.RouteBuffer) {
			b.StopToRoutes["A"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: -3}}
		}},
		{"more routes than stop-time entries", func(b *schedule.RouteBuffer) {
			b.Routes = append(b.Routes, model.RouteEntry{RouteID: "GHOST", StopIDs: []string{"A", "C"}})
			b.StopToRoutes["A"] = append(b.StopToRoutes["A"], schedule.RouteStop{RouteIdx: 1, StopPos: 0})
			b.StopToRoutes["C"] = []schedule.RouteStop{{RouteIdx: 1, StopPos: 1}}
		}},
		{"RouteFIFO shorter than Routes", func(b *schedule.RouteBuffer) {
			b.RouteFIFO = nil
		}},
		{"route with no stops", func(b *schedule.RouteBuffer) {
			b.Routes = append(b.Routes, model.RouteEntry{RouteID: "EMPTY", StopIDs: nil})
			b.StopTimes = append(b.StopTimes, []model.TripStopTimes{
				simpleTST("TE", testDate, nil, nil)})
			b.RouteFIFO = append(b.RouteFIFO, true)
			b.StopToRoutes["A"] = append(b.StopToRoutes["A"], schedule.RouteStop{RouteIdx: 1, StopPos: 0})
		}},
		{"trip with no stop times", func(b *schedule.RouteBuffer) {
			b.StopTimes[0] = append(b.StopTimes[0], model.TripStopTimes{
				Key: model.TripKey{TripID: "HOLLOW", Date: testDate}})
		}},
		{"trip index points at the wrong route", func(b *schedule.RouteBuffer) {
			b.TripIndex[model.TripKey{TripID: "T", Date: testDate}] = model.TripLocation{RouteIdx: 77, TripIdx: 5}
		}},
		{"nil footpath map", func(b *schedule.RouteBuffer) {
			b.Footpaths = nil
		}},
		{"footpath to a station that appears nowhere", func(b *schedule.RouteBuffer) {
			b.Footpaths["A"] = []model.Footpath{{NeighbourStop: "VOID", WalkSeconds: 100}}
		}},
		{"self-referencing footpath", func(b *schedule.RouteBuffer) {
			b.Footpaths["A"] = []model.Footpath{{NeighbourStop: "A", WalkSeconds: 60}}
		}},
		{"negative walk time", func(b *schedule.RouteBuffer) {
			b.Footpaths["A"] = []model.Footpath{{NeighbourStop: "B", WalkSeconds: -3600}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := base()
			tc.corrupt(b)
			schedule.SwapRoutes(b)
			state.SwapSignal(make(state.SignalBuffer))

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("search panicked on a malformed buffer: %v", r)
				}
			}()

			for _, pair := range [][2]string{{"A", "B"}, {"A", "C"}, {"B", "A"}, {"A", "VOID"}, {"A", "A"}} {
				for _, p := range query(pair[0], pair[1], at(8, 0), nil) {
					// Whatever comes back must still be a coherent journey.
					if len(p.Legs) == 0 {
						t.Errorf("%s→%s: returned a journey with no legs", pair[0], pair[1])
					}
					if p.ArrivalUnix < p.DepartureUnix {
						t.Errorf("%s→%s: arrives %d before departing %d", pair[0], pair[1], p.ArrivalUnix, p.DepartureUnix)
					}
					if err := ValidatePath(p, model.SearchParams{
						Origin: pair[0], Destination: pair[1], DepTime: at(8, 0)}); err != nil {
						t.Errorf("%s→%s: returned a journey that does not validate: %v (%s)",
							pair[0], pair[1], err, legSummary(p))
					}
				}
			}
		})
	}
}

// A nil buffer pointer must be handled, not dereferenced.
func TestCorrupt_NilBuffer(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("search panicked on a nil route buffer: %v", r)
		}
	}()
	schedule.SwapRoutes(nil)
	state.SwapSignal(make(state.SignalBuffer))
	if got := query("A", "B", at(8, 0), nil); got != nil {
		t.Errorf("a nil buffer should yield no journeys, got %d", len(got))
	}
}
