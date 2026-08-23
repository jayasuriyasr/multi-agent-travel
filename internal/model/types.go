// Package model defines the core domain types shared across all packages.
// This is the leaf of the dependency graph — it imports nothing from internal/.
package model

// TripKey uniquely identifies a trip on a specific operating date.
type TripKey struct {
	TripID string `json:"trip_id"`
	Date   string `json:"date"` // YYYY-MM-DD
}

// StopTime represents a single scheduled stop within a trip.
type StopTime struct {
	TripID        string `json:"trip_id"`
	Date          string `json:"date"`
	StopSeq       int    `json:"stop_seq"`
	StationID     string `json:"station_id"`
	DepartureUnix int64  `json:"departure_unix"`
}

// SeatSignal holds the in-memory seat availability snapshot for a trip.
type SeatSignal struct {
	ByClass    map[string]int `json:"by_class"`    // e.g. {"lower":3, "upper":6}
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

// TripStopTimes holds per-trip departure times indexed by stop position within a route.
type TripStopTimes struct {
	Key        TripKey  `json:"key"`
	Departures []int64  `json:"departures"`  // indexed by stop position within route
	StationIDs []string `json:"station_ids"` // parallel to Departures
}

// TripLocation identifies where a trip lives inside the RouteBuffer arrays.
type TripLocation struct {
	RouteIdx int
	TripIdx  int
}

// SearchParams captures the user's search request.
type SearchParams struct {
	Origin      string
	Destination string
	Date        string
	DepTime     int64  // departure time as unix timestamp
	SeatClass   string // e.g. "lower", "upper"
	Passengers  int
}

// Leg represents a single boarding segment within a journey.
type Leg struct {
	TripID        string `json:"trip_id"`
	Date          string `json:"date"`
	RouteID       string `json:"route_id"`
	BoardStation  string `json:"board_station"`
	AlightStation string `json:"alight_station"`
	DepartureUnix int64  `json:"departure_unix"`
	ArrivalUnix   int64  `json:"arrival_unix"`
}

// Footpath represents a walkable transfer between two nearby stations.
// Used by the RAPTOR footpath relaxation step (paper Section 3.1).
type Footpath struct {
	NeighbourStop string `json:"neighbour_stop"`
	WalkSeconds   int    `json:"walk_seconds"`
}

// Path represents a complete journey from origin to destination.
type Path struct {
	Legs        []Leg   `json:"legs"`
	TotalTime   int64   `json:"total_time_seconds"`
	Transfers   int     `json:"transfers"`
	ArrivalUnix int64   `json:"arrival_unix"`
	Score       float64 `json:"-"` // internal ranking score
}
