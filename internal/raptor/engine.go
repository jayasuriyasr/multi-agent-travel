// Package raptor implements the seat-aware RAPTOR search algorithm.
//
// RAPTOR (Round-bAsed Public Transit Optimized Router) computes the Pareto
// frontier over two objectives — arrival time and number of transfers — by
// running one "round" per vehicle leg. Round k holds the earliest arrival at
// every station reachable using at most k vehicles, so the answer set is one
// journey per useful transfer count.
//
// The single most important invariant in this file:
//
//	A trip may only be boarded in round k using the label from round k-1.
//
// Boarding off a label that the current round is still mutating collapses the
// rounds into each other: journeys silently exceed the transfer limit, the
// reported transfer count stops matching the legs, and — because the route
// queue is a Go map — the answer starts depending on randomised map iteration
// order. tau[k-1] is the previous round's frozen snapshot; it is the only
// thing Step B is ever allowed to read.
package raptor

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"time"

	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"
)

const infinity = int64(math.MaxInt64)

// canBoard checks seat availability using the in-memory snapshot.
//
// TWO-LAYER DESIGN — do not make this pessimistic without understanding both layers:
//
//	Layer 1 (this function): OPTIMISTIC.
//	  - Missing key (no seat data yet) → allow boarding.
//	    Reason: ColdStart may not have populated all trips (first boot, partial
//	    Redis failure). Blocking here causes 0-result searches while warming up.
//	  - Stale signal → allow boarding.
//	    Reason: ValidateAndTruncate (validator.go) is the authoritative gate;
//	    this is only a fast pre-filter in RAM.
//
//	Layer 2 (ValidateAndTruncate in validator.go): PESSIMISTIC.
//	  - Missing key in Redis → REJECT path.
//	  - Insufficient seats in Redis → REJECT path.
//	  - Runs after the search and does a fresh MGET before returning results.
//
// The asymmetry is intentional. Do not "fix" layer 1 to be strict without
// understanding that layer 2 is the real enforcement point.
func canBoard(buf *state.SignalBuffer, key model.TripKey, class string, count int) bool {
	sig, ok := (*buf)[key]
	if !ok {
		return true // optimistic: no seat data yet (warming up)
	}
	if sig.Stale {
		return true // stale: validator will re-check via Redis MGET
	}
	return sig.ByClass[class] >= count
}

// journalEntry records how a station was reached, so a path can be rebuilt by
// walking backwards from the destination.
//
// fromRound is stored explicitly rather than inferred: a transit leg found in
// round k departs from a label in round k-1, while a walk leg found in round k
// departs from a label in round k. Guessing this is how backtracking loses
// track of which round it is in and starts stitching together legs that do not
// connect.
type journalEntry struct {
	kind      model.LegKind
	fromRound int
	from      string // station this leg departs from
	tripKey   model.TripKey
	routeID   string
	departure int64
	arrival   int64
}

// searchState is the per-query working set. It is created fresh for every
// search and never shared, so nothing in here needs synchronisation.
type searchState struct {
	params  model.SearchParams
	rounds  int
	xferSec int64
	dest    string

	routes  *schedule.RouteBuffer
	signals *state.SignalBuffer
	allowed map[string]bool

	// tau[k][stop] is the earliest arrival at stop using at most k vehicles.
	tau []map[string]int64
	// journal[k][stop] is the final leg of that journey.
	journal []map[string]journalEntry
	// best[stop] is the earliest arrival over all rounds (tau* in the paper),
	// used for dominance checks and target pruning.
	best map[string]int64

	// preWalk[k][stop] is journal[k][stop] as it stood just before round k's
	// footpath pass — always the transit leg that made the stop a walk source.
	//
	// A walk relaxes from the label the stop had at that moment. If a walk from
	// somewhere else then lowers the same stop, journal[k] holds the walk and
	// backtracking would render two walks in a row for a journey that only ever
	// took one. This keeps the entry the walk actually left from.
	preWalk []map[string]journalEntry
}

// RaptorSearch performs a seat-aware RAPTOR traversal and returns up to topK
// Pareto-optimal journeys from origin to destination, best arrival first.
//
// The returned set is a true Pareto frontier: each journey arrives strictly
// earlier than the one before it and uses strictly more transfers. Because
// RAPTOR produces at most one journey per round, the frontier holds at most
// params.Rounds()+1 entries no matter how large topK is.
//
// ctx is honoured between rounds; a cancelled context aborts the search and
// returns nil.
func RaptorSearch(ctx context.Context, params model.SearchParams, topK int) []model.Path {
	start := time.Now()

	// CAPTURE ONCE — the snapshot rule. Both buffers are read exactly once so
	// the entire search sees a single consistent view of the world even while
	// background goroutines publish new ones.
	signals := state.LiveSignal()
	routes := schedule.LiveRoutes()

	if routes == nil || len(routes.Routes) == 0 {
		slog.Warn("search aborted: no routes in buffer")
		return nil
	}
	if params.Origin == "" || params.Destination == "" || params.Origin == params.Destination {
		return nil
	}

	s := &searchState{
		params:  params,
		rounds:  params.Rounds(),
		xferSec: params.TransferBuffer(),
		dest:    params.Destination,
		routes:  routes,
		signals: signals,
		allowed: computeAllowedDates(params.Date, params.DateWindow()),
		best:    map[string]int64{params.Origin: params.DepTime},
	}
	s.tau = make([]map[string]int64, s.rounds+1)
	s.journal = make([]map[string]journalEntry, s.rounds+1)
	s.preWalk = make([]map[string]journalEntry, s.rounds+1)
	s.tau[0] = map[string]int64{params.Origin: params.DepTime}
	s.journal[0] = map[string]journalEntry{}

	marked := map[string]struct{}{params.Origin: {}}

	// Round 0 footpaths: the passenger may walk away from the origin before
	// boarding anything.
	s.relaxFootpaths(0, []string{params.Origin}, marked)

	for k := 1; k <= s.rounds; k++ {
		if ctx.Err() != nil {
			slog.Warn("search cancelled", "round", k, "origin", params.Origin, "destination", params.Destination)
			return nil
		}

		// tau[k] starts as a copy of tau[k-1]: a journey using at most k-1
		// vehicles also uses at most k. Copying the journal alongside keeps
		// every label in tau[k] backed by a reconstructable chain.
		s.tau[k] = make(map[string]int64, len(s.tau[k-1]))
		s.journal[k] = make(map[string]journalEntry, len(s.journal[k-1]))
		for stop, v := range s.tau[k-1] {
			s.tau[k][stop] = v
		}
		for stop, e := range s.journal[k-1] {
			s.journal[k][stop] = e
		}

		// Collect (route → earliest boarding position) over stops improved last
		// round, then walk the routes in sorted order. Sorting is not cosmetic:
		// ranging over the map directly makes the result depend on Go's
		// randomised iteration order.
		queue := make(map[int]int)
		for stop := range marked {
			for _, rs := range routes.StopToRoutes[stop] {
				// The index comes from the published buffer, which a partial or
				// interrupted ingest can leave inconsistent. A negative position
				// would win the minimum below and then index a slice backwards,
				// so reject nonsense here rather than carry it into the scan.
				if rs.RouteIdx < 0 || rs.RouteIdx >= len(routes.Routes) || rs.StopPos < 0 {
					continue
				}
				if pos, seen := queue[rs.RouteIdx]; !seen || rs.StopPos < pos {
					queue[rs.RouteIdx] = rs.StopPos
				}
			}
		}
		order := make([]int, 0, len(queue))
		for ri := range queue {
			order = append(order, ri)
		}
		sort.Ints(order)

		improved := make(map[string]struct{})
		for _, ri := range order {
			s.scanRoute(k, ri, queue[ri], improved)
		}

		// Footpath relaxation, from the stops transit improved this round.
		// Snapshot the sources first so walk-reached stops do not expand their
		// own footpaths — the walk graph is already transitively closed, so one
		// pass finds every reachable neighbour.
		sources := make([]string, 0, len(improved))
		for stop := range improved {
			sources = append(sources, stop)
		}
		sort.Strings(sources)
		s.relaxFootpaths(k, sources, improved)

		marked = improved
		if len(marked) == 0 {
			break // nothing improved — no later round can improve either
		}
	}

	paths := s.extract(topK)

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("search complete",
			"origin", params.Origin, "destination", params.Destination,
			"date", params.Date, "rounds", s.rounds,
			"paths", len(paths), "duration", time.Since(start))
	}
	return paths
}

// scanRoute performs one route traversal for round k: ride forward from
// boardPos, alighting wherever the current trip improves a station, and
// boarding an earlier trip wherever round k-1 got the passenger there in time.
func (s *searchState) scanRoute(k, routeIdx, boardPos int, improved map[string]struct{}) {
	if routeIdx < 0 || routeIdx >= len(s.routes.StopTimes) || routeIdx >= len(s.routes.Routes) {
		return
	}
	route := s.routes.Routes[routeIdx]
	trips := s.routes.StopTimes[routeIdx]
	if len(trips) == 0 || len(route.StopIDs) == 0 {
		return
	}
	if boardPos < 0 {
		boardPos = 0 // defence in depth; the queue already rejects these
	}
	if !s.routes.IsFIFO(routeIdx) {
		// OVERTAKING ROUTE — one forward pass cannot be optimal here.
		//
		// The classic scan below carries a single "currently riding" trip and
		// only ever switches to an EARLIER-DEPARTING one. That is optimal
		// exactly when no trip overtakes another, which is what FIFO means. On
		// a route where an express passes a local, the earliest departure can
		// arrive last, and one pass will ride the local straight past it.
		//
		// Scanning each trip independently and keeping the best arrival fixes
		// that. It is equivalent to the classic scan on a FIFO route, and it is
		// exact here because boarding always reads tau[k-1] and never the ride
		// in progress — so switching trips mid-route is the same thing as
		// boarding that trip directly. Cost is O(trips × stops), paid only on
		// routes the loader actually measured as overtaking.
		for ti := range trips {
			s.scanTrip(k, route, &trips[ti], boardPos, improved)
		}
		return
	}

	prevTau := s.tau[k-1]
	prevJournal := s.journal[k-1]
	curTau := s.tau[k]
	curJournal := s.journal[k]

	tripIdx := -1
	boardStation := ""
	boardDep := int64(0)

	for pos := boardPos; pos < len(route.StopIDs); pos++ {
		station := route.StopIDs[pos]

		// ── Step A: alight ────────────────────────────────────────────────
		// Propagate the arrival of the trip currently being ridden. Arrivals,
		// not departures: the passenger gets off when the vehicle pulls in.
		if tripIdx >= 0 && pos < len(trips[tripIdx].Arrivals) {
			t := &trips[tripIdx]
			arr := t.Arrivals[pos]
			if s.improves(arr, station) {
				curTau[station] = arr
				s.best[station] = arr
				curJournal[station] = journalEntry{
					kind:      model.LegTransit,
					fromRound: k - 1,
					from:      boardStation,
					tripKey:   t.Key,
					routeID:   route.RouteID,
					departure: boardDep,
					arrival:   arr,
				}
				improved[station] = struct{}{}
			}
		}

		// ── Step B: board ─────────────────────────────────────────────────
		// Read ONLY the previous round's label. This is what makes round k
		// mean "at most k vehicles".
		ready, reachable := prevTau[station]
		if !reachable {
			continue
		}
		// Changing vehicles takes time; arriving on foot or starting here does not.
		if e, had := prevJournal[station]; had && e.kind == model.LegTransit {
			ready += s.xferSec
		}

		curDep := infinity
		if tripIdx >= 0 && pos < len(trips[tripIdx].Departures) {
			curDep = trips[tripIdx].Departures[pos]
		}
		if ready > curDep {
			continue // cannot beat the trip already being ridden
		}

		if cand := s.earliestTrip(trips, pos, ready, curDep); cand >= 0 {
			tripIdx = cand
			boardStation = station
			boardDep = trips[cand].Departures[pos]
		}
	}
}

// scanTrip rides one specific trip forward from boardPos: board at the first
// stop round k-1 gets the passenger to in time, then propagate that trip's
// arrivals to every stop after it.
//
// Used only for overtaking routes, where the route-wide scan is not valid.
// Boarding as early as the trip allows dominates boarding later on the same
// trip — the arrival times are fixed, and an earlier boarding point reaches a
// superset of the stops — so one boarding decision per trip is enough.
func (s *searchState) scanTrip(k int, route model.RouteEntry, t *model.TripStopTimes, boardPos int, improved map[string]struct{}) {
	if !s.tripUsable(t) {
		return
	}

	prevTau := s.tau[k-1]
	prevJournal := s.journal[k-1]
	curTau := s.tau[k]
	curJournal := s.journal[k]

	boarded := false
	boardStation := ""
	boardDep := int64(0)

	for pos := boardPos; pos < len(route.StopIDs); pos++ {
		station := route.StopIDs[pos]

		// ── Step A: alight ────────────────────────────────────────────────
		if boarded && pos < len(t.Arrivals) {
			arr := t.Arrivals[pos]
			if s.improves(arr, station) {
				curTau[station] = arr
				s.best[station] = arr
				curJournal[station] = journalEntry{
					kind:      model.LegTransit,
					fromRound: k - 1,
					from:      boardStation,
					tripKey:   t.Key,
					routeID:   route.RouteID,
					departure: boardDep,
					arrival:   arr,
				}
				improved[station] = struct{}{}
			}
		}

		// ── Step B: board ─────────────────────────────────────────────────
		if boarded || pos >= len(t.Departures) {
			continue
		}
		ready, reachable := prevTau[station]
		if !reachable {
			continue
		}
		if e, had := prevJournal[station]; had && e.kind == model.LegTransit {
			ready += s.xferSec
		}
		if t.Departures[pos] >= ready {
			boarded = true
			boardStation = station
			boardDep = t.Departures[pos]
		}
	}
}

// earliestTrip returns the index of the earliest trip departing stop position
// pos at or after ready, strictly before mustBeat, that passes the date and
// seat filters. It returns -1 when no such trip exists.
//
// Binary search plus a short forward scan. This is only ever called for routes
// the loader measured as FIFO, which computeFIFO now guarantees also means
// every trip serves the same number of stops — so the predicate below is
// monotone and the early break is sound. Overtaking routes never reach here;
// scanRoute sends them to scanTrip instead.
func (s *searchState) earliestTrip(trips []model.TripStopTimes, pos int, ready, mustBeat int64) int {
	lo := sort.Search(len(trips), func(i int) bool {
		d := trips[i].Departures
		if pos >= len(d) {
			return true
		}
		return d[pos] >= ready
	})
	for i := lo; i < len(trips); i++ {
		d := trips[i].Departures
		if pos >= len(d) {
			continue
		}
		if d[pos] >= mustBeat {
			break // FIFO: no later trip departs earlier
		}
		if s.tripUsable(&trips[i]) {
			return i
		}
	}
	return -1
}

// tripUsable applies the calendar-date window and the optimistic seat pre-filter.
func (s *searchState) tripUsable(t *model.TripStopTimes) bool {
	if !s.allowed[t.Key.Date] {
		return false
	}
	return canBoard(s.signals, t.Key, s.params.SeatClass, s.params.Passengers)
}

// improves reports whether arriving at station at time arr is worth recording,
// applying both local dominance and target pruning.
func (s *searchState) improves(arr int64, station string) bool {
	// Target pruning: a label that lands after we can already be standing at
	// the destination cannot lead to a better journey.
	if bd, ok := s.best[s.dest]; ok && arr >= bd {
		return false
	}
	b, ok := s.best[station]
	return !ok || arr < b
}

// relaxFootpaths walks from each source station into its (transitively closed)
// walk neighbourhood, recording walk legs in round k.
func (s *searchState) relaxFootpaths(k int, sources []string, improved map[string]struct{}) {
	curTau := s.tau[k]
	curJournal := s.journal[k]

	// Snapshot each source's departure time BEFORE any walk is written.
	//
	// Snapshotting only the source LIST is not enough. A station can be both a
	// source (transit improved it this round) and the target of a walk from an
	// earlier source in the same pass. Reading the live label there lets it set
	// off from a time this pass just wrote, chaining two walk legs into one
	// round and rebuilding footpath edges the loader's closure deliberately
	// dropped for exceeding MaxWalkSeconds. Worse, sources is sorted by station
	// ID, so whether that happens depends on how the IDs collate — the same
	// network answers differently under a rename.
	type walkSource struct {
		from   string
		depart int64
	}
	var srcs []walkSource
	for _, from := range sources {
		if len(s.routes.Footpaths[from]) == 0 {
			continue // nothing to walk to; nothing worth remembering
		}
		depart, ok := curTau[from]
		if !ok {
			continue
		}
		srcs = append(srcs, walkSource{from: from, depart: depart})
		if e, had := curJournal[from]; had {
			if s.preWalk[k] == nil {
				s.preWalk[k] = make(map[string]journalEntry, len(sources))
			}
			s.preWalk[k][from] = e
		}
	}

	for _, src := range srcs {
		from, depart := src.from, src.depart
		for _, fp := range s.routes.Footpaths[from] {
			arr := depart + int64(fp.WalkSeconds)
			if !s.improves(arr, fp.NeighbourStop) {
				continue
			}
			curTau[fp.NeighbourStop] = arr
			s.best[fp.NeighbourStop] = arr
			curJournal[fp.NeighbourStop] = journalEntry{
				kind:      model.LegWalk,
				fromRound: k, // a walk happens inside the round that reached `from`
				from:      from,
				routeID:   model.WalkRouteID,
				departure: depart,
				arrival:   arr,
			}
			improved[fp.NeighbourStop] = struct{}{}
		}
	}
}

// extract turns the round labels into the Pareto frontier of journeys.
//
// tau[k][dest] is non-increasing in k by construction, so a round only earns a
// place in the answer when it arrives strictly earlier than every round before
// it. That set is exactly the Pareto frontier over (arrival, transfers).
func (s *searchState) extract(topK int) []model.Path {
	var out []model.Path
	bestSoFar := infinity

	for k := 0; k <= s.rounds; k++ {
		if s.tau[k] == nil {
			continue
		}
		arr, ok := s.tau[k][s.dest]
		if !ok || arr >= bestSoFar {
			continue
		}
		p, ok := s.buildPath(k)
		if !ok {
			// A label without a reconstructable, self-consistent chain is a bug
			// signal, not something to hand to a passenger.
			slog.Warn("discarding unreconstructable label",
				"round", k, "origin", s.params.Origin, "destination", s.dest)
			continue
		}
		bestSoFar = arr
		out = append(out, p)
	}

	out = deduplicatePaths(out)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ArrivalUnix != out[j].ArrivalUnix {
			return out[i].ArrivalUnix < out[j].ArrivalUnix
		}
		return out[i].Transfers < out[j].Transfers
	})
	out = paretoFilter(out)

	if topK > 0 && len(out) > topK {
		out = out[:topK]
	}
	return out
}

// buildPath backtracks from the destination label in the given round to the
// origin, producing an ordered, validated journey.
func (s *searchState) buildPath(round int) (model.Path, bool) {
	var legs []model.Leg

	cur := s.dest
	r := round
	// Keyed on (round, station): that is the pair the journal can repeat. An
	// earlier version keyed on (round, len(legs)), and because len(legs) grows
	// on every iteration the key was never repeated and the guard never fired.
	type visitKey struct {
		round   int
		station string
	}
	visited := make(map[visitKey]bool)
	maxSteps := 2*(s.rounds+1) + 2

	prevKind := model.LegKind("")
	prevDeparture := int64(0)

	for cur != s.params.Origin {
		if len(legs) > maxSteps || r < 0 || r >= len(s.journal) {
			return model.Path{}, false
		}
		e, ok := s.journal[r][cur]
		if !ok || e.from == "" || e.from == cur {
			return model.Path{}, false
		}
		// Two walks in a row cannot be right: the footpath graph is closed, so a
		// second hop is either redundant or past the walking cap the loader
		// enforced. It only appears when a later walk overwrote the label this
		// leg departed from — so use the entry that was there at the time.
		if prevKind == model.LegWalk && e.kind == model.LegWalk {
			if pre, had := s.preWalk[r][cur]; had && pre.from != "" && pre.arrival <= prevDeparture {
				e = pre
			}
		}
		// A (round, station) pair may only be consumed once; revisiting one
		// means the journal contains a cycle.
		mark := visitKey{round: r, station: cur}
		if visited[mark] {
			return model.Path{}, false
		}
		visited[mark] = true

		leg := model.Leg{
			Kind:          e.kind,
			RouteID:       e.routeID,
			BoardStation:  e.from,
			AlightStation: cur,
			DepartureUnix: e.departure,
			ArrivalUnix:   e.arrival,
		}
		if e.kind == model.LegTransit {
			leg.TripID = e.tripKey.TripID
			leg.Date = e.tripKey.Date
		}
		legs = append(legs, leg)

		prevKind, prevDeparture = e.kind, e.departure
		cur = e.from
		r = e.fromRound
	}

	if len(legs) == 0 {
		return model.Path{}, false
	}
	for i, j := 0, len(legs)-1; i < j; i, j = i+1, j-1 {
		legs[i], legs[j] = legs[j], legs[i]
	}

	p := model.Path{
		Legs:          legs,
		Rounds:        round,
		DepartureUnix: legs[0].DepartureUnix,
		ArrivalUnix:   legs[len(legs)-1].ArrivalUnix,
	}
	p.TotalTimeSeconds = p.ArrivalUnix - p.DepartureUnix
	p.WaitSeconds = p.DepartureUnix - s.params.DepTime

	transit := 0
	for _, l := range legs {
		if l.Kind == model.LegTransit {
			transit++
		} else {
			p.WalkSeconds += l.ArrivalUnix - l.DepartureUnix
		}
	}
	if transit > 0 {
		p.Transfers = transit - 1
	}
	if transit > s.rounds {
		// Should be unreachable: round k boards only off tau[k-1]. Kept as a
		// live assertion because the failure it guards is invisible otherwise.
		slog.Error("round bound violated", "round", round, "transit_legs", transit, "max_rounds", s.rounds)
		return model.Path{}, false
	}

	if err := ValidatePath(p, s.params); err != nil {
		slog.Warn("discarding malformed path", "error", err, "round", round)
		return model.Path{}, false
	}
	return p, true
}

// paretoFilter keeps only non-dominated journeys. Input must already be sorted
// by arrival ascending; the sweep then keeps a journey only when it uses fewer
// transfers than everything kept before it.
func paretoFilter(paths []model.Path) []model.Path {
	out := paths[:0:0]
	bestTransfers := math.MaxInt32
	for _, p := range paths {
		if p.Transfers < bestTransfers {
			out = append(out, p)
			bestTransfers = p.Transfers
		}
	}
	return out
}

// deduplicatePaths removes journeys that are the same physical sequence of
// legs. Uses a fresh allocation so the result never shares backing storage
// with the caller's slice.
func deduplicatePaths(paths []model.Path) []model.Path {
	seen := make(map[string]bool, len(paths))
	out := make([]model.Path, 0, len(paths))
	for _, p := range paths {
		fp := pathFingerprint(p)
		if seen[fp] {
			continue
		}
		seen[fp] = true
		out = append(out, p)
	}
	return out
}

// pathFingerprint identifies a journey by its physical legs.
func pathFingerprint(p model.Path) string {
	b := make([]byte, 0, len(p.Legs)*48)
	for _, leg := range p.Legs {
		b = append(b, leg.RouteID...)
		b = append(b, '|')
		b = append(b, leg.TripID...)
		b = append(b, '|')
		b = append(b, leg.BoardStation...)
		b = append(b, '>')
		b = append(b, leg.AlightStation...)
		b = append(b, ';')
	}
	return string(b)
}

// computeAllowedDates returns the set of operating dates a search may use.
//
// The window runs from the day BEFORE the search date (an overnight service
// that departed yesterday and is still running) through windowDays after it
// (a multi-day journey whose later legs are seeded on later calendar days).
// A malformed date yields the single-date fallback rather than an empty set,
// so a bad input degrades to a same-day search instead of zero results.
func computeAllowedDates(date string, windowDays int) map[string]bool {
	allowed := make(map[string]bool, windowDays+2)
	t, err := time.Parse(model.DateLayout, date)
	if err != nil {
		allowed[date] = true
		return allowed
	}
	if windowDays < 0 {
		windowDays = 0
	}
	for d := -1; d <= windowDays; d++ {
		allowed[t.AddDate(0, 0, d).Format(model.DateLayout)] = true
	}
	return allowed
}
