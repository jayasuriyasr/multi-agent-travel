package schedule

import (
	"context"
	"fmt"

	"axentra/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IngestTrip represents a single trip payload for batch ingestion.
type IngestTrip struct {
	TripID    string
	Date      string // YYYY-MM-DD
	RouteID   string
	DepUnix   int64 // trip-level departure (first stop)
	StopTimes []model.StopTime
}

// ValidateBatch validates the entire batch BEFORE opening a Postgres transaction.
// On any error, the whole payload is rejected — no partial inserts (G2).
func ValidateBatch(trips []IngestTrip) error {
	seen := make(map[model.TripKey]bool, len(trips))
	for _, t := range trips {
		key := model.TripKey{TripID: t.TripID, Date: t.Date}
		if seen[key] {
			return fmt.Errorf("duplicate (trip_id, date): %v", key)
		}
		seen[key] = true

		if len(t.StopTimes) == 0 {
			return fmt.Errorf("trip %v has no stop times", key)
		}

		for i, st := range t.StopTimes {
			// L2 fix: validate that ArrivalUnix is set and non-negative.
			// Guard is < 0 (not <= 0): Unix epoch (0) is technically valid,
			// though no real train departs at 1970-01-01 00:00 UTC.
			if st.ArrivalUnix < 0 {
				return fmt.Errorf("trip %v stop %d: negative arrival_unix %d", key, i, st.ArrivalUnix)
			}
			if st.DepartureUnix < st.ArrivalUnix {
				return fmt.Errorf("trip %v stop %d: departure_unix %d < arrival_unix %d (train cannot leave before arriving)",
					key, i, st.DepartureUnix, st.ArrivalUnix)
			}
		}

		// Verify monotonically increasing departure times across stops
		for i := 1; i < len(t.StopTimes); i++ {
			if t.StopTimes[i].DepartureUnix <= t.StopTimes[i-1].DepartureUnix {
				return fmt.Errorf("non-monotone stops in trip %v at seq %d: %d <= %d",
					key, i, t.StopTimes[i].DepartureUnix, t.StopTimes[i-1].DepartureUnix)
			}
		}
	}
	return nil
}

// IngestBatch validates and inserts a batch of trips into Postgres within
// a single transaction. If anything fails, the entire batch is rolled back.
func IngestBatch(ctx context.Context, pool *pgxpool.Pool, trips []IngestTrip) error {
	// G2: Validate BEFORE opening a transaction
	if err := ValidateBatch(trips); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, t := range trips {
		// Upsert trip
		_, err := tx.Exec(ctx,
			`INSERT INTO trips (trip_id, date, route_id, departure_unix)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (trip_id, date) DO UPDATE SET
			   route_id = EXCLUDED.route_id,
			   departure_unix = EXCLUDED.departure_unix`,
			t.TripID, t.Date, t.RouteID, t.DepUnix,
		)
		if err != nil {
			return fmt.Errorf("insert trip %s/%s: %w", t.TripID, t.Date, err)
		}

		// Delete existing stop times and re-insert
		_, err = tx.Exec(ctx,
			`DELETE FROM stop_times WHERE trip_id = $1 AND date = $2`,
			t.TripID, t.Date,
		)
		if err != nil {
			return fmt.Errorf("delete stop_times %s/%s: %w", t.TripID, t.Date, err)
		}

		for _, st := range t.StopTimes {
			// L2 fix: insert both arrival_unix and departure_unix.
			_, err := tx.Exec(ctx,
				`INSERT INTO stop_times
				   (trip_id, date, stop_seq, station_id, arrival_unix, departure_unix)
				   VALUES ($1, $2, $3, $4, $5, $6)`,
				st.TripID, st.Date, st.StopSeq, st.StationID, st.ArrivalUnix, st.DepartureUnix,
			)
			if err != nil {
				return fmt.Errorf("insert stop_time seq %d for %s/%s: %w",
					st.StopSeq, st.TripID, st.Date, err)
			}
		}
	}

	// Bump schema_version watermark to trigger watcher reload
	_, err = tx.Exec(ctx, `UPDATE schema_version SET updated_at = NOW() WHERE id = 1`)
	if err != nil {
		return fmt.Errorf("bump schema_version: %w", err)
	}

	return tx.Commit(ctx)
}
