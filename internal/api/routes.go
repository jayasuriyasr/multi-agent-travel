package api

import (
	"axentra/internal/state"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

// RegisterRoutes mounts all HTTP endpoints on the Fiber app.
func RegisterRoutes(app *fiber.App, rdb *redis.Client) {
	// Health endpoints (no ReadyGate — these must respond even during warm-up)
	app.Get("/healthz/live", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	app.Get("/healthz/ready", func(c *fiber.Ctx) error {
		if state.IsReady() {
			return c.SendStatus(fiber.StatusOK)
		}
		return c.SendStatus(fiber.StatusServiceUnavailable)
	})

	// API routes (behind ReadyGate)
	api := app.Group("/api", ReadyGate)
	api.Get("/search", searchHandler(rdb))
}
