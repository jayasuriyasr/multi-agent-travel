package raptor

import (
	"strings"
	"testing"

	"axentra/internal/model"
)

// The footpath graph is closed at load time and capped at MaxWalkSeconds, so a
// journey may contain at most one walk between vehicles. These tests hold that
// property at the engine level.

// Regression: relaxFootpaths used to read each source's label from the live
// map. A station that was both a source (transit improved it) and the target of
// a walk from an earlier source in the same pass would then set off again from
// the walk value, chaining two walk legs and rebuilding an edge the closure had
// deliberately dropped for being too long.
func TestFootpaths_NeverChainTwoWalkLegs(t *testing.T) {
	// Models a capped closure: B→C (900s) and C→D (1200s) survive, but B→D
	// (2100s) was dropped for exceeding MaxWalkSeconds, so D is not reachable
	// on foot from B in one hop and must not become reachable in two.
	g := newGraph()
	g.addRoute("R1", []string{"AAA", "BBB", "CCC"}, []model.TripStopTimes{
		simpleTST("T1", testDate, []string{"AAA", "BBB", "CCC"},
			[]int64{at(8, 0), at(9, 0), at(9, 30)}),
	}, true)
	g.walk("BBB", "CCC", 900)
	g.walk("CCC", "DDD", 1200)
	g.publish(nil)

	for _, p := range search(t, "AAA", "DDD", at(7, 0), nil) {
		legs := legSummary(p)
		if n := strings.Count(legs, "WALK"); n > 1 {
			t.Errorf("journey chains %d walk legs in one round: %s (walking %ds)", n, legs, p.WalkSeconds)
		}
	}
}

// The same graph must answer identically however the station IDs happen to
// sort. relaxFootpaths iterates its sources in sorted order, so reading live
// labels made the result depend on collation — rename a station and the journey
// changed.
func TestFootpaths_AnswerDoesNotDependOnStationIDOrder(t *testing.T) {
	// A train reaches the first stop at 09:00 and the second at 09:30, and a
	// 900s walk between them beats the train. Whether the second stop then
	// walks onward from 09:15 or 09:30 must not depend on its name.
	run := func(first, second, beyond string) (string, int64) {
		stops := []string{"ORG", first, second}
		g := newGraph()
		g.addRoute("R1", stops, []model.TripStopTimes{
			simpleTST("T1", testDate, stops, []int64{at(8, 0), at(9, 0), at(9, 30)}),
		}, true)
		g.walk(first, second, 900)
		g.walk(second, beyond, 900)
		g.publish(nil)
		paths := search(t, "ORG", beyond, at(7, 0), nil)
		if len(paths) == 0 {
			return "(no journey)", 0
		}
		return legSummary(paths[0]), paths[0].ArrivalUnix
	}

	// Second stop sorts AFTER the first, so the walk into it lands before it is
	// itself relaxed.
	sortsAfter, arrAfter := run("AAA", "MMM", "ZZZ")
	// Same topology, but the second stop sorts BEFORE the first.
	sortsBefore, arrBefore := run("MMM", "AAA", "AAB")

	t.Logf("second stop sorts after  the first: %s (arrives %d)", sortsAfter, arrAfter)
	t.Logf("second stop sorts before the first: %s (arrives %d)", sortsBefore, arrBefore)

	walksAfter := strings.Count(sortsAfter, "WALK")
	walksBefore := strings.Count(sortsBefore, "WALK")
	if walksAfter != walksBefore {
		t.Errorf("identical topology produced %d walk legs one way and %d the other — the answer depends on station-ID collation:\n  %s\n  %s",
			walksAfter, walksBefore, sortsAfter, sortsBefore)
	}
}
