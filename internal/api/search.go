package api

import (
	"time"

	"axentra/internal/model"
	"axentra/internal/raptor"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

// searchHandler creates the GET /search handler that orchestrates
// RAPTOR search → MGET validation → JSON response.
func searchHandler(rdb *redis.Client) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// R4 fix: expose max_rounds as an optional query param (L4 completeness).
		// Capped at 8 to prevent runaway searches; defaults to model.DefaultMaxRounds.
		maxRoundsParam := c.QueryInt("max_rounds", model.DefaultMaxRounds)
		if maxRoundsParam < 1 || maxRoundsParam > 8 {
			maxRoundsParam = model.DefaultMaxRounds
		}

		params := model.SearchParams{
			Origin:      c.Query("origin"),
			Destination: c.Query("destination"),
			Date:        c.Query("date"),
			DepTime:     int64(c.QueryInt("dep_time", 0)),
			SeatClass:   c.Query("seat_class", "lower"),
			Passengers:  c.QueryInt("passengers", 1),
			MaxRounds:   maxRoundsParam,
		}

		// Basic validation
		if params.Origin == "" || params.Destination == "" || params.Date == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "origin, destination, and date are required",
			})
		}
		if params.DepTime == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "dep_time (unix timestamp) is required",
			})
		}
		if params.Passengers < 1 {
			params.Passengers = 1
		}

		t0 := time.Now()

		// Over-fetch: RAPTOR computes top-100 Pareto paths in RAM
		candidates := raptor.RaptorSearch(params, 100)

		// Single MGET validates all legs, truncate to top-5.
		// strictMode=false: transient Redis failures degrade gracefully for
		// read-only search results. Booking endpoints should use strictMode=true.
		final := raptor.ValidateAndTruncate(c.Context(), rdb,
			candidates, params.SeatClass, params.Passengers, 5, false)

		durationMs := float64(time.Since(t0).Microseconds()) / 1000.0

		return c.JSON(fiber.Map{
			"paths":            final,
			"result_count":     len(final),
			"query_duration_ms": durationMs,
			"validated_at":     time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
}
