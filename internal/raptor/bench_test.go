package raptor

import (
	"context"
	"fmt"
	"testing"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// syntheticNetwork builds a grid-shaped timetable: `lines` routes of `stops`
// stations each, `tripsPerLine` services a day, with every line crossing a
// shared interchange so transfers are genuinely available.
//
// This exists so the README can quote measured numbers instead of an
// unsubstantiated latency claim. Run: go test -bench=. -benchmem ./internal/raptor
func syntheticNetwork(lines, stops, tripsPerLine int) *schedule.RouteBuffer {
	buf := &schedule.RouteBuffer{
		TripIndex:    map[model.TripKey]model.TripLocation{},
		StopToRoutes: map[string][]schedule.RouteStop{},
		Footpaths:    map[string][]model.Footpath{},
	}

	for l := 0; l < lines; l++ {
		stopIDs := make([]string, stops)
		for s := 0; s < stops; s++ {
			if s == stops/2 {
				stopIDs[s] = "HUB" // shared interchange
			} else {
				stopIDs[s] = fmt.Sprintf("L%d_S%d", l, s)
			}
		}

		trips := make([]model.TripStopTimes, tripsPerLine)
		for tIdx := 0; tIdx < tripsPerLine; tIdx++ {
			start := at(5, 0) + int64(tIdx)*900 // one service every 15 minutes
			arrivals := make([]int64, stops)
			departures := make([]int64, stops)
			ids := make([]string, stops)
			for s := 0; s < stops; s++ {
				arrivals[s] = start + int64(s)*600
				departures[s] = arrivals[s] + 60
				ids[s] = stopIDs[s]
			}
			key := model.TripKey{TripID: fmt.Sprintf("L%d_T%d", l, tIdx), Date: testDate}
			trips[tIdx] = model.TripStopTimes{Key: key, Arrivals: arrivals, Departures: departures, StationIDs: ids}
			buf.TripIndex[key] = model.TripLocation{RouteIdx: l, TripIdx: tIdx}
		}

		buf.Routes = append(buf.Routes, model.RouteEntry{RouteID: fmt.Sprintf("L%d", l), StopIDs: stopIDs})
		buf.StopTimes = append(buf.StopTimes, trips)
		buf.RouteFIFO = append(buf.RouteFIFO, true)
		for pos, sid := range stopIDs {
			buf.StopToRoutes[sid] = append(buf.StopToRoutes[sid], schedule.RouteStop{RouteIdx: l, StopPos: pos})
		}
	}
	return buf
}

func benchSearch(b *testing.B, lines, stops, tripsPerLine int) {
	buf := syntheticNetwork(lines, stops, tripsPerLine)
	schedule.SwapRoutes(buf)
	state.SwapSignal(make(state.SignalBuffer))

	params := model.SearchParams{
		Origin:      "L0_S0",
		Destination: fmt.Sprintf("L%d_S%d", lines-1, stops-1),
		Date:        testDate,
		DepTime:     at(6, 0),
		SeatClass:   "lower",
		Passengers:  1,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RaptorSearch(ctx, params, 10)
	}
}

func BenchmarkSearch_Small(b *testing.B)  { benchSearch(b, 5, 10, 20) }
func BenchmarkSearch_Medium(b *testing.B) { benchSearch(b, 50, 20, 60) }
func BenchmarkSearch_Large(b *testing.B)  { benchSearch(b, 200, 30, 120) }

// BenchmarkSearch_Parallel measures throughput with many concurrent searches,
// which is the case the lock-free double buffer exists for.
func BenchmarkSearch_Parallel(b *testing.B) {
	schedule.SwapRoutes(syntheticNetwork(50, 20, 60))
	state.SwapSignal(make(state.SignalBuffer))

	params := model.SearchParams{
		Origin: "L0_S0", Destination: "L49_S19", Date: testDate,
		DepTime: at(6, 0), SeatClass: "lower", Passengers: 1,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			RaptorSearch(ctx, params, 10)
		}
	})
}
