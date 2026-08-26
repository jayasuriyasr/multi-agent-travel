package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"axentra/internal/schedule"

	"github.com/hibiken/asynq"
)

// TaskSeatPoll is the asynq task type for a single trip's seat poll.
const TaskSeatPoll = "seat:poll"

// Zone defines a polling urgency tier based on time to departure.
type Zone struct {
	Name     string
	MaxHours float64
	Interval time.Duration
}

// Zones ordered by urgency — the first match wins in ClassifyZone.
//
//	RED    : departs in < 12 h  → poll every 5 min
//	YELLOW : departs in < 48 h  → poll every 30 min
//	GREEN  : departs in < 168 h → poll every 4 h
//	COLD   : everything else    → poll every 24 h
var Zones = []Zone{
	{"RED", 12, 5 * time.Minute},
	{"YELLOW", 48, 30 * time.Minute},
	{"GREEN", 168, 4 * time.Hour},
	{"COLD", math.MaxFloat64, 24 * time.Hour},
}

// ClassifyZone returns the polling zone for a trip departing at departureUnix.
// Trips already departed fall in RED: their seat state matters most right up
// to the moment they leave.
func ClassifyZone(departureUnix int64) Zone {
	hours := float64(departureUnix-time.Now().Unix()) / 3600
	for _, z := range Zones {
		if hours < z.MaxHours {
			return z
		}
	}
	return Zones[len(Zones)-1]
}

// DefaultClassifyInterval is how often the classifier re-scans the schedule.
const DefaultClassifyInterval = 5 * time.Minute

// ZoneClassifyLoop enqueues a seat poll for every known trip, at a cadence set
// by the trip's urgency zone.
//
// It reads the in-memory route buffer only — zero database I/O on this path.
// The asynq TaskID makes the enqueue idempotent: re-enqueueing a task that is
// still pending is rejected rather than duplicated.
//
// The first sweep runs immediately. Waiting a full tick before the first scan
// left the whole fleet unpolled for minutes after every boot, which is exactly
// when seat data is least likely to already exist.
func ZoneClassifyLoop(ctx context.Context, client *asynq.Client, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultClassifyInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("zone classifier started", "interval", interval)

	for {
		classifyOnce(ctx, client)

		select {
		case <-ctx.Done():
			slog.Info("zone classifier stopping")
			return
		case <-ticker.C:
		}
	}
}

// classifyOnce performs a single classification sweep over the route buffer.
func classifyOnce(ctx context.Context, client *asynq.Client) {
	buf := schedule.LiveRoutes()
	enqueued, skipped := 0, 0

	byZone := map[string]int{}

	for key := range buf.TripIndex {
		if ctx.Err() != nil {
			return
		}
		depUnix := buf.TripDeparture(key)
		if depUnix == 0 {
			skipped++
			continue
		}
		zone := ClassifyZone(depUnix)
		byZone[zone.Name]++

		payload, err := json.Marshal(pollPayload{TripID: key.TripID, Date: key.Date})
		if err != nil {
			slog.Error("zone classifier could not marshal payload",
				"trip", key.TripID, "date", key.Date, "error", err)
			continue
		}

		_, err = client.EnqueueContext(ctx, asynq.NewTask(TaskSeatPoll, payload),
			asynq.ProcessIn(zone.Interval),
			// Idempotent: a pending task with this ID is not duplicated.
			asynq.TaskID(fmt.Sprintf("poll:%s:%s", key.TripID, key.Date)),
			asynq.Retention(zone.Interval),
			asynq.MaxRetry(3),
		)
		if err != nil {
			// A TaskID conflict means the poll is already scheduled — expected
			// and harmless, so it is not logged at error level.
			continue
		}
		enqueued++
	}

	slog.Info("zone classifier sweep complete",
		"trips", len(buf.TripIndex), "enqueued", enqueued, "no_departure", skipped,
		"red", byZone["RED"], "yellow", byZone["YELLOW"],
		"green", byZone["GREEN"], "cold", byZone["COLD"])
}
