package schedule

import (
	"strings"
	"testing"

	"axentra/internal/model"
)

func stop(seq int, station string, arr, dep int64) model.StopTime {
	return model.StopTime{
		TripID: "T1", Date: "2026-08-23", StopSeq: seq,
		StationID: station, ArrivalUnix: arr, DepartureUnix: dep,
	}
}

func TestValidateBatch(t *testing.T) {
	valid := IngestTrip{
		TripID: "T1", Date: "2026-08-23", RouteID: "R1", DepUnix: 100,
		StopTimes: []model.StopTime{stop(0, "A", 100, 100), stop(1, "B", 200, 260)},
	}

	cases := []struct {
		name    string
		trips   []IngestTrip
		wantErr string
	}{
		{"well-formed batch", []IngestTrip{valid}, ""},
		{
			name:    "duplicate trip and date",
			trips:   []IngestTrip{valid, valid},
			wantErr: "duplicate",
		},
		{
			name:    "trip with no stops",
			trips:   []IngestTrip{{TripID: "T2", Date: "2026-08-23"}},
			wantErr: "no stop times",
		},
		{
			name: "departure before arrival",
			trips: []IngestTrip{{
				TripID: "T3", Date: "2026-08-23",
				StopTimes: []model.StopTime{stop(0, "A", 500, 100)},
			}},
			wantErr: "cannot leave before arriving",
		},
		{
			name: "negative arrival",
			trips: []IngestTrip{{
				TripID: "T4", Date: "2026-08-23",
				StopTimes: []model.StopTime{stop(0, "A", -1, 100)},
			}},
			wantErr: "negative arrival_unix",
		},
		{
			name: "non-monotone stop order",
			trips: []IngestTrip{{
				TripID: "T5", Date: "2026-08-23",
				StopTimes: []model.StopTime{stop(0, "A", 100, 300), stop(1, "B", 200, 200)},
			}},
			wantErr: "non-monotone",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBatch(tc.trips)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want an error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
