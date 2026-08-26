package schedule

import (
	"testing"

	"axentra/internal/model"
)

// sampleBuffer builds a one-route buffer whose fields the tests then perturb.
func sampleBuffer(mutate func(*RouteBuffer)) *RouteBuffer {
	b := newRouteBuffer()
	key := model.TripKey{TripID: "T1", Date: "2026-08-23"}
	b.Routes = append(b.Routes, model.RouteEntry{
		RouteID: "R1", StopIDs: []string{"A", "B"}, TripKeys: []model.TripKey{key},
	})
	b.StopTimes = append(b.StopTimes, []model.TripStopTimes{{
		Key:        key,
		StationIDs: []string{"A", "B"},
		Arrivals:   []int64{100, 190},
		Departures: []int64{100, 200},
	}})
	b.RouteFIFO = append(b.RouteFIFO, true)
	b.TripIndex[key] = model.TripLocation{}
	b.StopToRoutes["A"] = []RouteStop{{RouteIdx: 0, StopPos: 0}}
	b.StopToRoutes["B"] = []RouteStop{{RouteIdx: 0, StopPos: 1}}
	if mutate != nil {
		mutate(b)
	}
	return b
}

// Regression: the previous hash covered only trip keys and departure times, so
// a reload that corrected arrival times or changed the walk graph was written
// off as a no-op and never reached memory.
func TestManifestHash_DetectsEveryObservableChange(t *testing.T) {
	base := manifestHash(sampleBuffer(nil))

	cases := []struct {
		name   string
		mutate func(*RouteBuffer)
	}{
		{"arrival time changed", func(b *RouteBuffer) { b.StopTimes[0][0].Arrivals[1] = 195 }},
		{"departure time changed", func(b *RouteBuffer) { b.StopTimes[0][0].Departures[1] = 205 }},
		{"stop sequence changed", func(b *RouteBuffer) { b.Routes[0].StopIDs[1] = "C" }},
		{"route id changed", func(b *RouteBuffer) { b.Routes[0].RouteID = "R2" }},
		{"trip id changed", func(b *RouteBuffer) { b.StopTimes[0][0].Key.TripID = "T2" }},
		{"operating date changed", func(b *RouteBuffer) { b.StopTimes[0][0].Key.Date = "2026-08-24" }},
		{"footpath added", func(b *RouteBuffer) {
			b.Footpaths["A"] = []model.Footpath{{NeighbourStop: "B", WalkSeconds: 60}}
		}},
		{"footpath duration changed", func(b *RouteBuffer) {
			b.Footpaths["A"] = []model.Footpath{{NeighbourStop: "B", WalkSeconds: 120}}
		}},
		{"trip added", func(b *RouteBuffer) {
			b.StopTimes[0] = append(b.StopTimes[0], model.TripStopTimes{
				Key:      model.TripKey{TripID: "T9", Date: "2026-08-23"},
				Arrivals: []int64{300, 400}, Departures: []int64{300, 400},
			})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := manifestHash(sampleBuffer(tc.mutate)); got == base {
				t.Fatalf("hash did not change — this reload would be silently discarded")
			}
		})
	}
}

func TestManifestHash_StableForIdenticalContent(t *testing.T) {
	a := manifestHash(sampleBuffer(nil))
	for i := 0; i < 20; i++ {
		if got := manifestHash(sampleBuffer(nil)); got != a {
			t.Fatalf("hash is not stable across runs: %s vs %s", a, got)
		}
	}
}

func TestManifestChanged_Bookkeeping(t *testing.T) {
	ResetManifest()
	if !manifestChanged("aaa") {
		t.Fatal("first hash should count as changed")
	}
	if manifestChanged("aaa") {
		t.Fatal("repeating the same hash should not count as changed")
	}
	if !manifestChanged("bbb") {
		t.Fatal("a new hash should count as changed")
	}
	ResetManifest()
	if !manifestChanged("bbb") {
		t.Fatal("after reset, any hash should count as changed")
	}
}

func TestComputeFIFO(t *testing.T) {
	tst := func(deps ...int64) model.TripStopTimes {
		return model.TripStopTimes{Departures: deps, Arrivals: deps}
	}

	cases := []struct {
		name  string
		trips []model.TripStopTimes
		want  bool
	}{
		{"single trip is trivially FIFO", []model.TripStopTimes{tst(100, 200)}, true},
		{"parallel trips are FIFO", []model.TripStopTimes{tst(100, 200), tst(300, 400)}, true},
		{"equal times at a stop are FIFO", []model.TripStopTimes{tst(100, 200), tst(300, 200)}, true},
		{"an express overtaking a local is not FIFO", []model.TripStopTimes{tst(100, 500), tst(200, 300)}, false},
		{"mismatched stop counts compare only the overlap",
			[]model.TripStopTimes{tst(100, 200), tst(300)}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := computeFIFO(tc.trips); got != tc.want {
				t.Fatalf("computeFIFO = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCloseFootpaths(t *testing.T) {
	direct := map[string][]model.Footpath{
		"A": {{NeighbourStop: "B", WalkSeconds: 300}},
		"B": {{NeighbourStop: "C", WalkSeconds: 300}},
		"C": {{NeighbourStop: "D", WalkSeconds: 300}},
	}
	closed := closeFootpaths(direct, DefaultLoadOptions())

	reach := func(from, to string) (int, bool) {
		for _, e := range closed[from] {
			if e.NeighbourStop == to {
				return e.WalkSeconds, true
			}
		}
		return 0, false
	}

	// Without closure a single relaxation pass can never find A→C or A→D.
	if secs, ok := reach("A", "C"); !ok || secs != 600 {
		t.Errorf("A→C should close to 600s, got %d (found=%v)", secs, ok)
	}
	if secs, ok := reach("A", "D"); !ok || secs != 900 {
		t.Errorf("A→D should close to 900s, got %d (found=%v)", secs, ok)
	}
	if _, ok := reach("A", "A"); ok {
		t.Error("closure must not produce a self-edge")
	}
}

func TestCloseFootpaths_PrefersShortestAndRespectsCap(t *testing.T) {
	direct := map[string][]model.Footpath{
		// Two ways from A to C: 900s direct, or 200s via B.
		"A": {{NeighbourStop: "B", WalkSeconds: 100}, {NeighbourStop: "C", WalkSeconds: 900}},
		"B": {{NeighbourStop: "C", WalkSeconds: 100}},
		// Beyond the cap, so it must not appear at all.
		"C": {{NeighbourStop: "FAR", WalkSeconds: DefaultMaxWalkSeconds + 1}},
	}
	closed := closeFootpaths(direct, DefaultLoadOptions())

	for _, e := range closed["A"] {
		if e.NeighbourStop == "C" && e.WalkSeconds != 200 {
			t.Errorf("A→C should take the 200s route, got %d", e.WalkSeconds)
		}
		if e.NeighbourStop == "FAR" {
			t.Errorf("walks longer than %ds must be dropped", DefaultMaxWalkSeconds)
		}
	}
	for _, e := range closed["C"] {
		if e.NeighbourStop == "FAR" {
			t.Errorf("walks longer than %ds must be dropped", DefaultMaxWalkSeconds)
		}
	}
}

func TestCloseFootpaths_EmptyInput(t *testing.T) {
	if got := closeFootpaths(nil, DefaultLoadOptions()); got == nil || len(got) != 0 {
		t.Fatalf("empty input should give an empty, non-nil map, got %v", got)
	}
}

func TestRouteBuffer_IsFIFODefaultsToSafe(t *testing.T) {
	b := newRouteBuffer()
	if b.IsFIFO(0) || b.IsFIFO(-1) || b.IsFIFO(99) {
		t.Fatal("an unknown route must be treated as non-FIFO")
	}
}

func TestRouteBuffer_TripDeparture(t *testing.T) {
	b := sampleBuffer(nil)
	key := model.TripKey{TripID: "T1", Date: "2026-08-23"}
	if got := b.TripDeparture(key); got != 100 {
		t.Errorf("TripDeparture = %d, want 100", got)
	}
	if got := b.TripDeparture(model.TripKey{TripID: "nope", Date: "2026-08-23"}); got != 0 {
		t.Errorf("unknown trip should give 0, got %d", got)
	}
}

func TestLiveRoutes_NeverNil(t *testing.T) {
	// The package initialiser must publish a fully allocated buffer so a search
	// that lands before the first load reads empty maps rather than panicking.
	b := LiveRoutes()
	if b == nil || b.TripIndex == nil || b.StopToRoutes == nil || b.Footpaths == nil {
		t.Fatal("initial buffer must have every map allocated")
	}
}

// The closure must not hand a station an unbounded neighbour list: those edges
// are re-walked on every footpath relaxation of every round of every search.
func TestCloseFootpaths_CapsEdgesPerStation(t *testing.T) {
	direct := map[string][]model.Footpath{}
	for i := 0; i < 40; i++ {
		s := "S" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		direct[s] = []model.Footpath{{NeighbourStop: "HUB", WalkSeconds: 60}}
		direct["HUB"] = append(direct["HUB"], model.Footpath{NeighbourStop: s, WalkSeconds: 60})
	}

	closed := closeFootpaths(direct, LoadOptions{MaxWalkSeconds: 1800, MaxFootpathsPerStation: 10})
	for station, edges := range closed {
		if len(edges) > 10 {
			t.Fatalf("station %s kept %d edges, cap is 10", station, len(edges))
		}
		// Truncation keeps the nearest neighbours, so what survives is what a
		// passenger would actually use.
		for i := 1; i < len(edges); i++ {
			if edges[i].WalkSeconds < edges[i-1].WalkSeconds {
				t.Fatalf("station %s edges are not nearest-first: %+v", station, edges)
			}
		}
	}
}

// A zero LoadOptions must behave, so a caller that forgets to configure the
// loader gets the documented defaults rather than an empty walk graph.
func TestLoadOptions_ZeroValueUsesDefaults(t *testing.T) {
	got := LoadOptions{}.withDefaults()
	if got.MaxWalkSeconds != DefaultMaxWalkSeconds {
		t.Errorf("MaxWalkSeconds = %d, want %d", got.MaxWalkSeconds, DefaultMaxWalkSeconds)
	}
	if got.MaxFootpathsPerStation != DefaultMaxFootpathsPerStation {
		t.Errorf("MaxFootpathsPerStation = %d, want %d", got.MaxFootpathsPerStation, DefaultMaxFootpathsPerStation)
	}
	direct := map[string][]model.Footpath{"A": {{NeighbourStop: "B", WalkSeconds: 300}}}
	if len(closeFootpaths(direct, LoadOptions{})) == 0 {
		t.Fatal("a zero LoadOptions must still produce a closure")
	}
}

// The walk cap must be honoured exactly: a two-hop walk totalling more than the
// cap is not a walk anyone will make.
func TestCloseFootpaths_RespectsWalkCap(t *testing.T) {
	direct := map[string][]model.Footpath{
		"A": {{NeighbourStop: "B", WalkSeconds: 400}},
		"B": {{NeighbourStop: "C", WalkSeconds: 400}},
	}
	closed := closeFootpaths(direct, LoadOptions{MaxWalkSeconds: 700, MaxFootpathsPerStation: 64})
	for _, e := range closed["A"] {
		if e.NeighbourStop == "C" {
			t.Fatal("A→C is 800s and must not survive a 700s cap")
		}
	}
	if len(closed["A"]) != 1 || closed["A"][0].NeighbourStop != "B" {
		t.Fatalf("A should still reach B: %+v", closed["A"])
	}
}
