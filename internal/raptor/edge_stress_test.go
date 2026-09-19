package raptor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// Adversarial network shapes and the concurrency the double buffer exists for.

// A chain long enough to exhaust the round budget, so the limit is what stops
// the search rather than the network running out.
func TestStress_DeepTransferChain(t *testing.T) {
	const links = 20
	g := newGraph()
	stops := make([]string, links+1)
	for i := range stops {
		stops[i] = fmt.Sprintf("S%02d", i)
	}
	for i := 0; i < links; i++ {
		from, to := stops[i], stops[i+1]
		g.addRoute(fmt.Sprintf("R%02d", i), []string{from, to}, []model.TripStopTimes{
			simpleTST(fmt.Sprintf("T%02d", i), testDate, []string{from, to},
				[]int64{at(6, 0) + int64(i)*1800, at(6, 0) + int64(i)*1800 + 900}),
		}, true)
	}
	g.publish(nil)

	for rounds := 1; rounds <= 8; rounds++ {
		got := query(stops[0], stops[links], at(5, 0), func(p *model.SearchParams) { p.MaxRounds = rounds })
		if len(got) != 0 {
			t.Errorf("max_rounds=%d: %d links need %d vehicles, should find nothing, got %s",
				rounds, links, links, legSummary(got[0]))
		}
		// A destination that IS within reach must be found exactly.
		reachable := query(stops[0], stops[rounds], at(5, 0), func(p *model.SearchParams) { p.MaxRounds = rounds })
		if len(reachable) == 0 {
			t.Errorf("max_rounds=%d: S00 → %s needs exactly %d vehicles and should be found", rounds, stops[rounds], rounds)
			continue
		}
		if bad := checkJourney(reachable[0], stops[0], stops[rounds], at(5, 0), rounds, 0); bad != "" {
			t.Errorf("max_rounds=%d: %s", rounds, bad)
		}
	}
}

// A hub every route passes through: the route queue, marked set and target
// pruning all get their worst case here.
func TestStress_HubTopology(t *testing.T) {
	const spokes = 40
	g := newGraph()
	for i := 0; i < spokes; i++ {
		in, out := fmt.Sprintf("IN%02d", i), fmt.Sprintf("OUT%02d", i)
		g.addRoute(fmt.Sprintf("RIN%02d", i), []string{in, "HUB"}, []model.TripStopTimes{
			simpleTST(fmt.Sprintf("TIN%02d", i), testDate, []string{in, "HUB"},
				[]int64{at(8, 0), at(9, 0)}),
		}, true)
		g.addRoute(fmt.Sprintf("ROUT%02d", i), []string{"HUB", out}, []model.TripStopTimes{
			simpleTST(fmt.Sprintf("TOUT%02d", i), testDate, []string{"HUB", out},
				[]int64{at(9, 30), at(10, 0) + int64(i)*60}),
		}, true)
	}
	g.publish(nil)

	got := query("IN00", "OUT00", at(7, 0), nil)
	if len(got) != 1 {
		t.Fatalf("IN00 → OUT00 should be one journey through the hub, got %d", len(got))
	}
	if bad := checkJourney(got[0], "IN00", "OUT00", at(7, 0), model.DefaultMaxRounds, 0); bad != "" {
		t.Error(bad)
	}
	if got[0].ArrivalUnix != at(10, 0) {
		t.Errorf("arrival %d, want %d", got[0].ArrivalUnix, at(10, 0))
	}
}

// One route with a great many stops and a great many trips.
func TestStress_LongRouteManyTrips(t *testing.T) {
	const stops, trips = 300, 200
	ids := make([]string, stops)
	for i := range ids {
		ids[i] = fmt.Sprintf("P%03d", i)
	}
	tsts := make([]model.TripStopTimes, 0, trips)
	for tIdx := 0; tIdx < trips; tIdx++ {
		times := make([]int64, stops)
		base := at(5, 0) + int64(tIdx)*300
		for i := range times {
			times[i] = base + int64(i)*120
		}
		tsts = append(tsts, simpleTST(fmt.Sprintf("T%03d", tIdx), testDate, ids, times))
	}
	newGraph().addRoute("LONG", ids, tsts, true).publish(nil)

	got := query(ids[0], ids[stops-1], at(6, 0), nil)
	if len(got) != 1 {
		t.Fatalf("want 1 journey, got %d", len(got))
	}
	if bad := checkJourney(got[0], ids[0], ids[stops-1], at(6, 0), model.DefaultMaxRounds, 0); bad != "" {
		t.Error(bad)
	}
	// The first trip departing at or after 06:00 is T012 (05:00 + 12×300).
	if want := at(5, 0) + 12*300; got[0].DepartureUnix != want {
		t.Errorf("departs %d, want %d — the binary search picked the wrong trip", got[0].DepartureUnix, want)
	}

	// Boarding mid-route must work identically.
	got = query(ids[150], ids[stops-1], at(9, 0), nil)
	if len(got) != 1 {
		t.Fatalf("mid-route boarding: want 1 journey, got %d", len(got))
	}
	if bad := checkJourney(got[0], ids[150], ids[stops-1], at(9, 0), model.DefaultMaxRounds, 0); bad != "" {
		t.Error(bad)
	}
}

// Non-FIFO, footpaths and a transfer buffer all at once.
func TestStress_OvertakingWithWalksAndBuffer(t *testing.T) {
	g := newGraph()
	// An overtaking route: the later departure runs express.
	g.addRoute("MIX", []string{"A", "B", "C"}, []model.TripStopTimes{
		simpleTST("LOCAL", testDate, []string{"A", "B", "C"}, []int64{at(8, 0), at(10, 0), at(12, 0)}),
		simpleTST("EXPRESS", testDate, []string{"A", "B", "C"}, []int64{at(9, 0), at(9, 30), at(10, 0)}),
	}, false)
	// A walk out of C, and an onward service from the far side.
	g.walk("C", "D", 600)
	g.walk("D", "C", 600)
	g.addRoute("ONWARD", []string{"D", "E"}, []model.TripStopTimes{
		simpleTST("ON1", testDate, []string{"D", "E"}, []int64{at(10, 30), at(11, 0)}),
	}, true)
	g.publish(nil)

	got := query("A", "E", at(7, 0), func(p *model.SearchParams) { p.MinTransferSeconds = 300 })
	if len(got) == 0 {
		t.Fatal("A → E should be reachable: express to C, walk to D, onward to E")
	}
	if bad := checkJourney(got[0], "A", "E", at(7, 0), model.DefaultMaxRounds, 300); bad != "" {
		t.Fatal(bad)
	}
	if got[0].ArrivalUnix != at(11, 0) {
		t.Errorf("arrival %d, want %d (%s)", got[0].ArrivalUnix, at(11, 0), legSummary(got[0]))
	}
	if want := "EXPRESS:A>C|WALK:C>D|ON1:D>E"; legSummary(got[0]) != want {
		t.Errorf("legs %s, want %s", legSummary(got[0]), want)
	}
}

// A cancelled context must stop the search rather than run to completion.
func TestStress_ContextCancellation(t *testing.T) {
	g := newGraph()
	for i := 0; i < 40; i++ {
		from, to := fmt.Sprintf("C%02d", i), fmt.Sprintf("C%02d", i+1)
		g.addRoute(fmt.Sprintf("R%02d", i), []string{from, to}, []model.TripStopTimes{
			simpleTST(fmt.Sprintf("T%02d", i), testDate, []string{from, to},
				[]int64{at(6, 0) + int64(i)*600, at(6, 0) + int64(i)*600 + 300}),
		}, true)
	}
	g.publish(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := RaptorSearch(ctx, model.SearchParams{
		Origin: "C00", Destination: "C40", Date: testDate, DepTime: at(5, 0),
		SeatClass: "lower", Passengers: 1, MaxRounds: 6}, 100)
	if got != nil {
		t.Errorf("a cancelled context should abort the search, got %d journeys", len(got))
	}

	// A deadline that expires part-way through must also be honoured.
	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if got := RaptorSearch(ctx, model.SearchParams{
		Origin: "C00", Destination: "C40", Date: testDate, DepTime: at(5, 0),
		SeatClass: "lower", Passengers: 1, MaxRounds: 6}, 100); got != nil {
		t.Errorf("an expired deadline should abort the search, got %d journeys", len(got))
	}
}

// Searches running while the schedule and seat buffers are being republished.
// The snapshot rule is what makes this safe; run under -race.
func TestStress_SearchesDuringLiveSwaps(t *testing.T) {
	makeGraph := func(offset int64) *schedule.RouteBuffer {
		g := newGraph()
		g.addRoute("R1", []string{"A", "B"}, []model.TripStopTimes{
			simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(9, 0) + offset, at(10, 0) + offset})}, true)
		g.addRoute("R2", []string{"B", "C"}, []model.TripStopTimes{
			simpleTST("T2", testDate, []string{"B", "C"}, []int64{at(10, 30) + offset, at(11, 0) + offset})}, true)
		g.walk("B", "X", 300)
		g.walk("X", "B", 300)
		return g.buf
	}
	schedule.SwapRoutes(makeGraph(0))
	state.SwapSignal(make(state.SignalBuffer))

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			schedule.SwapRoutes(makeGraph(i % 7 * 60))
			buf := make(state.SignalBuffer)
			buf[model.TripKey{TripID: "T1", Date: testDate}] = model.SeatSignal{
				Total: 10, ByClass: map[string]int{"lower": 10}}
			state.SwapSignal(buf)
		}
	}()

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				for _, p := range query("A", "C", at(8, 0), nil) {
					if len(p.Legs) == 0 {
						t.Error("a returned journey has no legs")
						return
					}
					if p.Legs[0].BoardStation != "A" || p.Legs[len(p.Legs)-1].AlightStation != "C" {
						t.Errorf("journey does not run A → C: %s", legSummary(p))
						return
					}
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Nothing departs after the requested time.
func TestStress_EverythingAlreadyDeparted(t *testing.T) {
	newGraph().addRoute("R", []string{"A", "B"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"A", "B"}, []int64{at(6, 0), at(7, 0)}),
		simpleTST("T2", testDate, []string{"A", "B"}, []int64{at(7, 0), at(8, 0)}),
	}, true).publish(nil)

	if got := query("A", "B", at(23, 0), nil); len(got) != 0 {
		t.Errorf("everything has gone for the day, got %s", legSummary(got[0]))
	}
}
