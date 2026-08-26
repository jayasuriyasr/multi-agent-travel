// Package model defines the core domain types shared across all packages.
// This is the leaf of the dependency graph — it imports nothing from internal/.
package model

import "time"

// ── Search tuning defaults ───────────────────────────────────────────────────

const (
	// DefaultMaxRounds is the default RAPTOR round limit. Round k explores
	// journeys using at most k vehicle legs, so k rounds allows k-1 transfers.
	DefaultMaxRounds = 4

	// MaxAllowedRounds caps what a caller may request, bounding worst-case
	// search work regardless of what arrives on the query string.
	MaxAllowedRounds = 8

	// DefaultMinTransferSeconds is the minimum time a passenger needs to change
	// vehicles at a station. Zero reproduces the pure timetable semantics of the
	// RAPTOR paper; operators with real station geometry should raise it via
	// MIN_TRANSFER_SECONDS or the min_transfer query parameter.
	DefaultMinTransferSeconds = 0

	// DefaultDateWindowDays is how many calendar days after the search date the
	// engine will accept trips from, so multi-day journeys resolve. The previous
	// day is always included as well, for overnight trips already in motion.
	DefaultDateWindowDays = 2

	// MaxPassengers bounds the passenger count accepted by the API.
	MaxPassengers = 20
)

// ── Schedule types ───────────────────────────────────────────────────────────

// TripKey uniquely identifies a trip on a specific operating date.
type TripKey struct {
	TripID string `json:"trip_id"`
	Date   string `json:"date"` // YYYY-MM-DD
}

// StopTime represents a single scheduled stop within a trip.
// ArrivalUnix is when the vehicle physically arrives at the stop.
// DepartureUnix is when it leaves — always >= ArrivalUnix.
type StopTime struct {
	TripID        string `json:"trip_id"`
	Date          string `json:"date"`
	StopSeq       int    `json:"stop_seq"`
	StationID     string `json:"station_id"`
	ArrivalUnix   int64  `json:"arrival_unix"`
	DepartureUnix int64  `json:"departure_unix"`
}

// SeatSignal holds the in-memory seat availability snapshot for a trip.
type SeatSignal struct {
	ByClass    map[string]int `json:"by_class"` // e.g. {"lower":3, "upper":6}
	Total      int            `json:"total"`
	Stale      bool           `json:"stale"`
	SnapshotTs float64        `json:"snapshot_ts"`
}

// RouteEntry describes a route: its ID, ordered stop IDs, and associated trips.
type RouteEntry struct {
	RouteID  string    `json:"route_id"`
	StopIDs  []string  `json:"stop_ids"`
	TripKeys []TripKey `json:"trip_keys"`
}

// TripStopTimes holds per-trip stop times indexed by stop position within a route.
// Arrivals and Departures are parallel slices — index i is the same stop.
// Use Arrivals[i] when alighting, Departures[i] when checking if a trip can be caught.
type TripStopTimes struct {
	Key        TripKey  `json:"key"`
	Arrivals   []int64  `json:"arrivals"`
	Departures []int64  `json:"departures"`
	StationIDs []string `json:"station_ids"`
}

// TripLocation identifies where a trip lives inside the RouteBuffer arrays.
type TripLocation struct {
	RouteIdx int
	TripIdx  int
}

// Footpath represents a walkable transfer between two nearby stations.
type Footpath struct {
	NeighbourStop string `json:"neighbour_stop"`
	WalkSeconds   int    `json:"walk_seconds"`
}

// ── Search types ─────────────────────────────────────────────────────────────

// SearchParams captures a single journey query.
type SearchParams struct {
	Origin      string
	Destination string
	Date        string // YYYY-MM-DD
	DepTime     int64  // earliest departure, unix seconds
	SeatClass   string
	Passengers  int

	// MaxRounds overrides DefaultMaxRounds when > 0.
	MaxRounds int
	// MinTransferSeconds overrides DefaultMinTransferSeconds when > 0.
	MinTransferSeconds int
	// DateWindowDays overrides DefaultDateWindowDays when > 0.
	DateWindowDays int
}

// Rounds returns the effective round limit, clamped to MaxAllowedRounds.
func (p SearchParams) Rounds() int {
	r := p.MaxRounds
	if r <= 0 {
		r = DefaultMaxRounds
	}
	if r > MaxAllowedRounds {
		r = MaxAllowedRounds
	}
	return r
}

// TransferBuffer returns the effective minimum transfer time in seconds.
func (p SearchParams) TransferBuffer() int64 {
	if p.MinTransferSeconds > 0 {
		return int64(p.MinTransferSeconds)
	}
	return DefaultMinTransferSeconds
}

// DateWindow returns how many days after Date the engine may use trips from.
func (p SearchParams) DateWindow() int {
	if p.DateWindowDays > 0 {
		return p.DateWindowDays
	}
	return DefaultDateWindowDays
}

// LegKind distinguishes riding a vehicle from walking between stations.
type LegKind string

const (
	LegTransit LegKind = "transit"
	LegWalk    LegKind = "walk"
)

// WalkRouteID is the sentinel RouteID carried by walking legs. Seat validation
// skips any leg with this route ID.
const WalkRouteID = "WALK"

// Leg represents a single segment within a journey — one vehicle ride or one walk.
type Leg struct {
	Kind          LegKind `json:"kind"`
	TripID        string  `json:"trip_id,omitempty"`
	Date          string  `json:"date,omitempty"`
	RouteID       string  `json:"route_id"`
	BoardStation  string  `json:"board_station"`
	AlightStation string  `json:"alight_station"`
	DepartureUnix int64   `json:"departure_unix"`
	ArrivalUnix   int64   `json:"arrival_unix"`
}

// Path represents a complete journey from origin to destination.
type Path struct {
	Legs []Leg `json:"legs"`

	// DepartureUnix is when the first leg leaves the origin — which may be later
	// than the requested departure time if the passenger has to wait.
	DepartureUnix int64 `json:"departure_unix"`
	ArrivalUnix   int64 `json:"arrival_unix"`

	// TotalTimeSeconds is the in-journey duration (first departure → final arrival).
	TotalTimeSeconds int64 `json:"total_time_seconds"`
	// WaitSeconds is how long the passenger waits at the origin before departing.
	WaitSeconds int64 `json:"wait_seconds"`

	// Transfers counts vehicle changes: (number of transit legs) - 1.
	Transfers int `json:"transfers"`
	// WalkSeconds is the total time spent walking across all footpath legs.
	WalkSeconds int64 `json:"walk_seconds"`
	// Rounds is the RAPTOR round that produced this path.
	Rounds int `json:"rounds"`
}

// TransitLegs returns the number of vehicle rides in the path.
func (p Path) TransitLegs() int {
	n := 0
	for _, l := range p.Legs {
		if l.Kind == LegTransit {
			n++
		}
	}
	return n
}

// Dominates reports whether p is at least as good as q on both objectives
// (arrival time and transfer count) and strictly better on at least one.
func (p Path) Dominates(q Path) bool {
	betterOrEqual := p.ArrivalUnix <= q.ArrivalUnix && p.Transfers <= q.Transfers
	strictly := p.ArrivalUnix < q.ArrivalUnix || p.Transfers < q.Transfers
	return betterOrEqual && strictly
}

// ── Date helpers ─────────────────────────────────────────────────────────────

// DateLayout is the canonical calendar-date format used throughout Axentra.
const DateLayout = "2006-01-02"

// ValidDate reports whether s parses as a YYYY-MM-DD calendar date.
func ValidDate(s string) bool {
	_, err := time.Parse(DateLayout, s)
	return err == nil
}

// ── Redis seat key helpers ───────────────────────────────────────────────────
// All seat-related Redis keys MUST be constructed via these helpers so the
// format cannot drift between the seeder, the Lua gate, the refresher and the
// validator.
//
//	seat:map:<tripID>:<date>   — JSON seat availability map
//	seat:ts:<tripID>:<date>    — unix float64 snapshot timestamp
//	seat:hash:<tripID>:<date>  — canonical hash for change detection
//	seat:dirty_stream          — stream of changed trips (constant)

// DirtyStreamKey is the Redis stream every seat change is announced on.
const DirtyStreamKey = "seat:dirty_stream"

// SeatTripDate returns the compound "tripID:date" used as the stream entry
// field value and as the suffix of every seat key.
func SeatTripDate(tripID, date string) string { return tripID + ":" + date }

// SeatMapKey returns the Redis key for a trip's live seat availability JSON.
func SeatMapKey(tripID, date string) string { return "seat:map:" + tripID + ":" + date }

// SeatTSKey returns the Redis key for a trip's seat snapshot unix timestamp.
func SeatTSKey(tripID, date string) string { return "seat:ts:" + tripID + ":" + date }

// SeatHashKey returns the Redis key for a trip's canonical seat hash.
func SeatHashKey(tripID, date string) string { return "seat:hash:" + tripID + ":" + date }

// ParseSeatTripDate splits a "tripID:date" stream value back into its parts.
// The date is always the final colon-separated field, so trip IDs containing
// colons round-trip correctly.
func ParseSeatTripDate(s string) (TripKey, bool) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			tripID, date := s[:i], s[i+1:]
			if tripID == "" || !ValidDate(date) {
				return TripKey{}, false
			}
			return TripKey{TripID: tripID, Date: date}, true
		}
	}
	return TripKey{}, false
}
