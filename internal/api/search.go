package api

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"axentra/internal/model"
	"axentra/internal/raptor"
	"axentra/internal/schedule"

	"github.com/gofiber/fiber/v2"
)

// maxDepartureUnix is an absurdity ceiling on dep_time: 2200-01-01.
//
// Its job is to catch the common mistake of sending a millisecond timestamp —
// today's milliseconds read as seconds land tens of thousands of years out —
// without second-guessing legitimately distant travel dates. It is deliberately
// not a "reasonable booking window": rejecting a real date is a worse failure
// than searching one that finds nothing.
const maxDepartureUnix int64 = 7258118400

// handleSearch runs a journey search: RAPTOR over the in-memory schedule, then
// authoritative seat validation against Redis.
func (s *Server) handleSearch(c *fiber.Ctx) error {
	params, err := s.parseSearch(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Stations are validated against what is actually loaded, so a typo comes
	// back as "unknown station" instead of an empty result set that looks like
	// "no service".
	buf := schedule.LiveRoutes()
	if !stationKnown(buf, params.Origin) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fmt.Sprintf("unknown origin station %q", params.Origin),
		})
	}
	if !stationKnown(buf, params.Destination) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fmt.Sprintf("unknown destination station %q", params.Destination),
		})
	}

	// Bound the work a single request can cause, and stop it promptly if the
	// client disconnects.
	ctx, cancel := context.WithTimeout(c.UserContext(), s.cfg.RequestTimeout)
	defer cancel()

	t0 := time.Now()
	candidates := raptor.RaptorSearch(ctx, params, s.cfg.SearchCandidates)
	searchMs := durationMs(time.Since(t0))

	t1 := time.Now()
	final := s.validator.Validate(ctx, candidates, params.SeatClass, params.Passengers)
	validateMs := durationMs(time.Since(t1))

	if final == nil {
		final = []model.Path{} // render as [] rather than null
	}

	return c.JSON(fiber.Map{
		"paths":                final,
		"result_count":         len(final),
		"candidate_count":      len(candidates),
		"rejected_by_seats":    len(candidates) - len(final),
		"search_duration_ms":   searchMs,
		"validate_duration_ms": validateMs,
		"query_duration_ms":    durationMs(time.Since(t0)),
		"seat_validation":      s.validator.Status(),
		"max_rounds":           params.Rounds(),
		"min_transfer_seconds": params.TransferBuffer(),
		"validated_at":         time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// parseSearch validates and normalises the query string into SearchParams.
func (s *Server) parseSearch(c *fiber.Ctx) (model.SearchParams, error) {
	var p model.SearchParams

	p.Origin = strings.TrimSpace(c.Query("origin"))
	p.Destination = strings.TrimSpace(c.Query("destination"))
	p.Date = strings.TrimSpace(c.Query("date"))

	if p.Origin == "" || p.Destination == "" || p.Date == "" {
		return p, fmt.Errorf("origin, destination and date are required")
	}
	if p.Origin == p.Destination {
		return p, fmt.Errorf("origin and destination must differ")
	}
	if !model.ValidDate(p.Date) {
		return p, fmt.Errorf("date %q must be in YYYY-MM-DD format", p.Date)
	}

	// Parsed as a 64-bit integer, not via QueryInt: Atoi on a 32-bit platform
	// silently mangles any unix timestamp past 2038 — a real limit for a
	// service whose whole job is future-dated travel.
	rawDep := strings.TrimSpace(c.Query("dep_time"))
	if rawDep == "" {
		return p, fmt.Errorf("dep_time (unix timestamp in seconds) is required")
	}
	dep, err := strconv.ParseInt(rawDep, 10, 64)
	if err != nil {
		return p, fmt.Errorf("dep_time %q must be a unix timestamp in seconds", rawDep)
	}
	if dep <= 0 {
		return p, fmt.Errorf("dep_time must be positive")
	}
	if dep > maxDepartureUnix {
		return p, fmt.Errorf("dep_time %d is implausibly far in the future "+
			"(milliseconds instead of seconds?)", dep)
	}
	p.DepTime = dep

	p.SeatClass = strings.TrimSpace(c.Query("seat_class", "lower"))
	if p.SeatClass == "" {
		p.SeatClass = "lower"
	}
	if len(p.SeatClass) > 32 {
		return p, fmt.Errorf("seat_class is too long")
	}

	p.Passengers = c.QueryInt("passengers", 1)
	if p.Passengers < 1 {
		p.Passengers = 1
	}
	if p.Passengers > model.MaxPassengers {
		return p, fmt.Errorf("passengers must be between 1 and %d", model.MaxPassengers)
	}

	p.MaxRounds = c.QueryInt("max_rounds", s.cfg.MaxRounds)
	if p.MaxRounds < 1 || p.MaxRounds > model.MaxAllowedRounds {
		return p, fmt.Errorf("max_rounds must be between 1 and %d", model.MaxAllowedRounds)
	}

	p.MinTransferSeconds = c.QueryInt("min_transfer", s.cfg.MinTransferSeconds)
	if p.MinTransferSeconds < 0 || p.MinTransferSeconds > 6*3600 {
		return p, fmt.Errorf("min_transfer must be between 0 and 21600 seconds")
	}

	p.DateWindowDays = s.cfg.DateWindowDays
	return p, nil
}

// stationKnown reports whether a station is served by any route or reachable
// on foot in the currently loaded schedule.
func stationKnown(buf *schedule.RouteBuffer, station string) bool {
	if _, ok := buf.StopToRoutes[station]; ok {
		return true
	}
	_, ok := buf.Footpaths[station]
	return ok
}

func durationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
