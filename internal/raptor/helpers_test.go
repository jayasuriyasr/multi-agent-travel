package raptor

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

// TestMain silences logging so test output shows assertions, not search traces.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// testDate is the operating date every fixture uses.
const testDate = "2026-08-23"

// at returns the unix timestamp for hh:mm on testDate (UTC).
//
// Fixtures use real timestamps on the real operating date. Using bare
// seconds-since-midnight while labelling trips with a 2026 date meant the
// calendar-window logic was never actually exercised by the very tests that
// claimed to cover overnight and multi-day journeys.
func at(hour, min int) int64 {
	d, err := time.Parse(model.DateLayout, testDate)
	if err != nil {
		panic(err)
	}
	return d.Add(time.Duration(hour)*time.Hour + time.Duration(min)*time.Minute).Unix()
}

// atDay returns the unix timestamp for hh:mm, dayOffset days after testDate.
func atDay(dayOffset, hour, min int) int64 {
	return at(hour, min) + int64(dayOffset)*86400
}

// dayString returns the YYYY-MM-DD string dayOffset days after testDate.
func dayString(dayOffset int) string {
	d, _ := time.Parse(model.DateLayout, testDate)
	return d.AddDate(0, 0, dayOffset).Format(model.DateLayout)
}

// makeTST builds a TripStopTimes with parallel arrival and departure slices.
func makeTST(key model.TripKey, stations []string, arrivals, departures []int64) model.TripStopTimes {
	return model.TripStopTimes{
		Key:        key,
		StationIDs: stations,
		Arrivals:   arrivals,
		Departures: departures,
	}
}

// simpleTST builds a trip whose arrival and departure at each stop are equal.
func simpleTST(tripID, date string, stations []string, times []int64) model.TripStopTimes {
	return makeTST(model.TripKey{TripID: tripID, Date: date}, stations, times, times)
}

// graph incrementally assembles a RouteBuffer for tests.
type graph struct{ buf *schedule.RouteBuffer }

func newGraph() *graph {
	return &graph{buf: &schedule.RouteBuffer{
		TripIndex:    map[model.TripKey]model.TripLocation{},
		StopToRoutes: map[string][]schedule.RouteStop{},
		Footpaths:    map[string][]model.Footpath{},
	}}
}

// addRoute registers a route. fifo mirrors what the loader would measure.
func (g *graph) addRoute(routeID string, stops []string, trips []model.TripStopTimes, fifo bool) *graph {
	idx := len(g.buf.Routes)
	keys := make([]model.TripKey, len(trips))
	for ti, t := range trips {
		keys[ti] = t.Key
		g.buf.TripIndex[t.Key] = model.TripLocation{RouteIdx: idx, TripIdx: ti}
	}
	g.buf.Routes = append(g.buf.Routes, model.RouteEntry{RouteID: routeID, StopIDs: stops, TripKeys: keys})
	g.buf.StopTimes = append(g.buf.StopTimes, trips)
	g.buf.RouteFIFO = append(g.buf.RouteFIFO, fifo)
	for pos, s := range stops {
		g.buf.StopToRoutes[s] = append(g.buf.StopToRoutes[s], schedule.RouteStop{RouteIdx: idx, StopPos: pos})
	}
	return g
}

// walk adds a one-way footpath. Test graphs are published as-is, so callers
// must supply an already-closed walk graph exactly as the loader would.
func (g *graph) walk(from, to string, seconds int) *graph {
	g.buf.Footpaths[from] = append(g.buf.Footpaths[from], model.Footpath{NeighbourStop: to, WalkSeconds: seconds})
	return g
}

// publish installs the graph as the live schedule and seat buffer.
func (g *graph) publish(signals state.SignalBuffer) {
	if signals == nil {
		signals = make(state.SignalBuffer)
	}
	schedule.SwapRoutes(g.buf)
	state.SwapSignal(signals)
}

// search runs a query against the published graph with sensible defaults.
func search(t *testing.T, origin, dest string, dep int64, tweak func(*model.SearchParams)) []model.Path {
	t.Helper()
	p := model.SearchParams{
		Origin:      origin,
		Destination: dest,
		Date:        testDate,
		DepTime:     dep,
		SeatClass:   "lower",
		Passengers:  1,
	}
	if tweak != nil {
		tweak(&p)
	}
	return RaptorSearch(context.Background(), p, 100)
}

// legSummary renders a path as "TRIP:A>B|TRIP2:B>C" for readable assertions.
func legSummary(p model.Path) string {
	out := ""
	for i, l := range p.Legs {
		if i > 0 {
			out += "|"
		}
		id := l.TripID
		if l.Kind == model.LegWalk {
			id = "WALK"
		}
		out += id + ":" + l.BoardStation + ">" + l.AlightStation
	}
	return out
}
