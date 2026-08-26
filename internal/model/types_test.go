package model

import "testing"

func TestValidDate(t *testing.T) {
	for _, s := range []string{"2026-08-23", "2026-02-28", "2024-02-29"} {
		if !ValidDate(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range []string{"", "23-08-2026", "2026/08/23", "2026-13-01", "2026-02-30", "not a date"} {
		if ValidDate(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func TestSeatKeyHelpers(t *testing.T) {
	if got := SeatMapKey("T1", "2026-08-23"); got != "seat:map:T1:2026-08-23" {
		t.Errorf("SeatMapKey = %q", got)
	}
	if got := SeatTSKey("T1", "2026-08-23"); got != "seat:ts:T1:2026-08-23" {
		t.Errorf("SeatTSKey = %q", got)
	}
	if got := SeatHashKey("T1", "2026-08-23"); got != "seat:hash:T1:2026-08-23" {
		t.Errorf("SeatHashKey = %q", got)
	}
}

// Trip IDs contain colons in the seeded data ("ROUTE-EXP-1_2026-08-23_T01" does
// not, but nothing stops an operator's IDs from doing so), so the date must be
// split off from the right.
func TestParseSeatTripDate(t *testing.T) {
	cases := []struct {
		in       string
		wantTrip string
		wantOK   bool
	}{
		{"T1:2026-08-23", "T1", true},
		{"ROUTE:EXP:1_T01:2026-08-23", "ROUTE:EXP:1_T01", true},
		{"T1", "", false},
		{":2026-08-23", "", false},
		{"T1:not-a-date", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := ParseSeatTripDate(tc.in)
		if ok != tc.wantOK {
			t.Errorf("ParseSeatTripDate(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			continue
		}
		if ok && got.TripID != tc.wantTrip {
			t.Errorf("ParseSeatTripDate(%q) trip = %q, want %q", tc.in, got.TripID, tc.wantTrip)
		}
	}
}

func TestSeatTripDateRoundTrip(t *testing.T) {
	key, ok := ParseSeatTripDate(SeatTripDate("TRIP_01", "2026-08-23"))
	if !ok || key.TripID != "TRIP_01" || key.Date != "2026-08-23" {
		t.Fatalf("round trip failed: %+v ok=%v", key, ok)
	}
}

func TestPathDominates(t *testing.T) {
	fast := Path{ArrivalUnix: 100, Transfers: 1}
	slow := Path{ArrivalUnix: 200, Transfers: 2}
	tradeoff := Path{ArrivalUnix: 200, Transfers: 0}

	if !fast.Dominates(slow) {
		t.Error("earlier arrival with fewer transfers should dominate")
	}
	if slow.Dominates(fast) {
		t.Error("dominance must not be symmetric")
	}
	if fast.Dominates(tradeoff) || tradeoff.Dominates(fast) {
		t.Error("a genuine trade-off must not be dominated in either direction")
	}
	if fast.Dominates(fast) {
		t.Error("an identical path does not dominate itself")
	}
}

func TestPathTransitLegs(t *testing.T) {
	p := Path{Legs: []Leg{
		{Kind: LegTransit}, {Kind: LegWalk}, {Kind: LegTransit},
	}}
	if got := p.TransitLegs(); got != 2 {
		t.Errorf("TransitLegs = %d, want 2 (walks are not vehicles)", got)
	}
}

func TestSearchParamsDefaults(t *testing.T) {
	var p SearchParams
	if p.Rounds() != DefaultMaxRounds {
		t.Errorf("Rounds = %d, want %d", p.Rounds(), DefaultMaxRounds)
	}
	if p.TransferBuffer() != DefaultMinTransferSeconds {
		t.Errorf("TransferBuffer = %d, want %d", p.TransferBuffer(), DefaultMinTransferSeconds)
	}
	if p.DateWindow() != DefaultDateWindowDays {
		t.Errorf("DateWindow = %d, want %d", p.DateWindow(), DefaultDateWindowDays)
	}
}
