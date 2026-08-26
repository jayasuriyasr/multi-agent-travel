package api

import (
	"axentra/internal/state"

	"github.com/gofiber/fiber/v2"
)

// readyGate blocks API traffic with a 503 until cold start has finished, so no
// query ever runs against an empty route or seat buffer and gets back a
// confidently wrong "no routes found".
func (s *Server) readyGate(c *fiber.Ctx) error {
	if !state.IsReady() {
		c.Set("Retry-After", "5")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":               "warming_up",
			"retry_after_seconds": 5,
		})
	}
	return c.Next()
}
