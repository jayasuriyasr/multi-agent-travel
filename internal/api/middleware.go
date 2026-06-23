package api

import (
	"axentra/internal/state"

	"github.com/gofiber/fiber/v2"
)

// ReadyGate is Fiber middleware that blocks all traffic with a 503 until
// the system has completed cold-start initialization (state.READY == 1).
// This prevents queries against empty route arrays or seat signal buffers.
func ReadyGate(c *fiber.Ctx) error {
	if !state.IsReady() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":               "warming_up",
			"retry_after_seconds": 5,
		})
	}
	return c.Next()
}
