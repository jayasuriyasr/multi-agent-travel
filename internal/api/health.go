package api

import (
	"axentra/internal/schedule"
	"axentra/internal/state"

	"github.com/gofiber/fiber/v2"
)

// staleAlertFraction is the share of stale seat signals above which the
// service reports itself degraded.
//
// Past this point the optimistic in-memory pre-filter has effectively stopped
// filtering and every result rides entirely on the strict Redis validator.
// That is survivable, but it is a different mode of operation and an operator
// should be able to see it before it becomes an incident rather than inferring
// it from a rising rejection rate.
const staleAlertFraction = 0.5

// handleLive is the liveness probe: the process is running and can serve.
func (s *Server) handleLive(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "alive"})
}

// handleReady is the readiness probe.
//
// It reports 200 once cold start has completed and 503 while warming up, and
// carries a body describing what is actually loaded and how healthy the seat
// path is. "Ready" and "answering usefully" are separate facts: without seat
// data, or with a tripped validation breaker, the engine still finds routes
// but they cannot be confirmed — which looks identical to "no routes exist"
// unless the difference is said out loud.
func (s *Server) handleReady(c *fiber.Ctx) error {
	body := s.healthBody()

	if !state.IsReady() {
		c.Set("Retry-After", "5")
		body["status"] = "warming_up"
		return c.Status(fiber.StatusServiceUnavailable).JSON(body)
	}
	body["status"] = s.degradation()
	return c.JSON(body)
}

// degradation names the most serious problem currently affecting results, or
// "ready" when there is none. Ordered worst first: a tripped breaker means no
// result can be confirmed at all, which subsumes staleness.
func (s *Server) degradation() string {
	switch {
	case !state.HasSeatData():
		return "degraded_no_seat_data"
	case !s.validator.Healthy():
		return "degraded_seat_validation_unavailable"
	case state.StaleFraction() > staleAlertFraction:
		return "degraded_stale_seat_data"
	default:
		return "ready"
	}
}

func (s *Server) healthBody() fiber.Map {
	st := schedule.LiveRoutes().Stats()
	return fiber.Map{
		"ready":                 state.IsReady(),
		"routes":                st.Routes,
		"trips":                 st.Trips,
		"stations":              st.Stations,
		"footpath_origins":      st.FootpathOrigin,
		"non_fifo_routes":       st.NonFIFORoutes,
		"seat_signals":          state.SignalCount(),
		"stale_seat_signals":    state.StaleSignalCount(),
		"stale_fraction":        round2(state.StaleFraction()),
		"seat_data_age_seconds": int(state.SeatDataAge().Seconds()),
		"has_seat_data":         state.HasSeatData(),
		"seat_validation":       s.validator.Status(),
		"seat_provider":         s.cfg.SeatProvider,
		"strict_seat_mode":      s.cfg.StrictSeatMode,
	}
}

// handleStats exposes what the engine currently holds in memory.
func (s *Server) handleStats(c *fiber.Ctx) error {
	st := schedule.LiveRoutes().Stats()
	return c.JSON(fiber.Map{
		"schedule": fiber.Map{
			"routes":           st.Routes,
			"trips":            st.Trips,
			"stations":         st.Stations,
			"footpath_origins": st.FootpathOrigin,
			"non_fifo_routes":  st.NonFIFORoutes,
		},
		"seats": fiber.Map{
			"signals":               state.SignalCount(),
			"stale_signals":         state.StaleSignalCount(),
			"stale_fraction":        round2(state.StaleFraction()),
			"seat_data_age_seconds": int(state.SeatDataAge().Seconds()),
			"provider":              s.cfg.SeatProvider,
			"validation":            s.validator.Status(),
			"validation_healthy":    s.validator.Healthy(),
		},
		"search": fiber.Map{
			"max_rounds":           s.cfg.MaxRounds,
			"min_transfer_seconds": s.cfg.MinTransferSeconds,
			"date_window_days":     s.cfg.DateWindowDays,
			"results":              s.cfg.SearchResults,
			"strict_seat_mode":     s.cfg.StrictSeatMode,
			"mget_chunk":           s.cfg.RedisMGetChunk,
		},
	})
}

// round2 trims a fraction to two decimals so the health body stays readable.
func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
