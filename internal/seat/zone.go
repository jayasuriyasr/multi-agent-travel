package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"axentra/internal/schedule"

	"github.com/hibiken/asynq"
)

// Zone defines a polling urgency tier based on time-to-departure.
type Zone struct {
	Name     string
	MaxHours float64
	Interval time.Duration
}

// Zones ordered by urgency — first match wins in ClassifyZone.
// RED  : departure < 12 h  → poll every 5 min
// YELLOW: departure < 48 h  → poll every 30 min
// GREEN : departure < 168 h → poll every 4 h
// COLD  : default (no near departure) → poll every 24 h
var Zones = []Zone{
	{"RED", 12, 5 * time.Minute},
	{"YELLOW", 48, 30 * time.Minute},
	{"GREEN", 168, 4 * time.Hour},
	{"COLD", 1 << 31, 24 * time.Hour},
}

// ClassifyZone returns the polling zone for a trip based on hours until departure.
func ClassifyZone(departureUnix int64) Zone {
	hours := float64(departureUnix-time.Now().Unix()) / 3600
	for _, z := range Zones {
		if hours < z.MaxHours {
			return z
		}
	}
	return Zones[len(Zones)-1]
}

// ZoneClassifyLoop is a background goroutine that ticks every 5 minutes.
// It reads all known trips from the in-memory RAM buffer (NOT the DB),
// classifies each trip into a polling zone, and enqueues a "seat:poll" task
// with a ProcessIn delay equal to the zone interval.
//
// The TaskID option prevents duplicate tasks: Asynq will reject an enqueue
// if a task with the same ID already exists in the queue (idempotent enqueue).
func ZoneClassifyLoop(ctx context.Context, client *asynq.Client) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	log.Println("[seat] zone classifier: started")

	for {
		select {
		case <-ctx.Done():
			log.Println("[seat] zone classifier: context cancelled, stopping")
			return
		case <-ticker.C:
		}

		buf := schedule.LiveRoutes()
		enqueued := 0

		for key, loc := range buf.TripIndex {
			// Retrieve departure from RAM — no DB call here (G5 / spec requirement)
			depUnix := int64(0)
			if loc.RouteIdx < len(buf.StopTimes) && loc.TripIdx < len(buf.StopTimes[loc.RouteIdx]) {
				deps := buf.StopTimes[loc.RouteIdx][loc.TripIdx].Departures
				if len(deps) > 0 {
					depUnix = deps[0]
				}
			}
			if depUnix == 0 {
				continue // no departure time available, skip
			}

			zone := ClassifyZone(depUnix)

			payload, err := json.Marshal(map[string]string{
				"trip_id": key.TripID,
				"date":    key.Date,
			})
			if err != nil {
				log.Printf("[seat] zone classifier: marshal error for %s/%s: %v", key.TripID, key.Date, err)
				continue
			}

			task := asynq.NewTask("seat:poll", payload)
			_, err = client.Enqueue(task,
				asynq.ProcessIn(zone.Interval),
				// Unique deduplication key — Asynq rejects re-enqueue if task already exists
				asynq.TaskID(fmt.Sprintf("poll:%s:%s", key.TripID, key.Date)),
				// Retain result for one full zone interval to aid observability
				asynq.Retention(zone.Interval),
			)
			if err != nil {
				// ErrTaskIDConflict is expected and safe — task is already scheduled
				continue
			}
			enqueued++
		}

		log.Printf("[seat] zone classifier: scanned %d trips, enqueued %d new tasks",
			len(buf.TripIndex), enqueued)
	}
}

// getTripDeparture looks up a trip's departure from the in-memory route buffer.
func getTripDeparture(tripID, date string) int64 {
	return schedule.GetTripDeparture(tripID, date)
}
