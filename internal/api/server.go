// Package api exposes the HTTP surface: the journey search endpoint, health
// probes and the static demo page.
package api

import (
	"log/slog"
	"time"

	"axentra/internal/config"
	"axentra/internal/raptor"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	fiberrecover "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/redis/go-redis/v9"
)

// Server owns the HTTP dependencies. Handlers are methods on it rather than
// closures over package-level state, so nothing here is global and everything
// is injectable in tests.
type Server struct {
	cfg       *config.Config
	rdb       *redis.Client
	validator *raptor.Validator
}

// NewServer builds the API server.
//
// The seat validator is constructed here and held for the process lifetime
// rather than per request, because it owns the circuit breaker: a breaker
// rebuilt on every call would never accumulate the failures it exists to
// count.
func NewServer(cfg *config.Config, rdb *redis.Client) *Server {
	return &Server{
		cfg: cfg,
		rdb: rdb,
		validator: raptor.NewValidator(rdb, raptor.ValidatorConfig{
			Strict:           cfg.StrictSeatMode,
			MGetChunk:        cfg.RedisMGetChunk,
			MaxResults:       cfg.SearchResults,
			BreakerThreshold: cfg.BreakerThreshold,
			BreakerCooldown:  cfg.BreakerCooldown,
		}),
	}
}

// BuildApp constructs the Fiber application with all middleware and routes.
func (s *Server) BuildApp() *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:               "Axentra",
		ReadTimeout:           s.cfg.HTTPReadTimeout,
		WriteTimeout:          s.cfg.HTTPReadTimeout,
		IdleTimeout:           60 * time.Second,
		BodyLimit:             s.cfg.BodyLimitBytes,
		DisableStartupMessage: true,
		ErrorHandler:          errorHandler,
	})

	// A panic in one handler must not take the process down with it.
	app.Use(fiberrecover.New(fiberrecover.Config{EnableStackTrace: true}))
	app.Use(requestid.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: s.cfg.CORSOrigins,
		AllowMethods: "GET,OPTIONS",
	}))

	// Health endpoints are registered before the rate limiter: a probe must
	// never be throttled, or an instance under load looks dead to its
	// orchestrator and gets restarted at exactly the wrong moment.
	app.Get("/healthz/live", s.handleLive)
	app.Get("/healthz/ready", s.handleReady)

	if s.cfg.RateLimitPerMin > 0 {
		app.Use(limiter.New(limiter.Config{
			Max:        s.cfg.RateLimitPerMin,
			Expiration: time.Minute,
			LimitReached: func(c *fiber.Ctx) error {
				return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
					"error":               "rate_limited",
					"retry_after_seconds": 60,
				})
			},
		}))
	}

	api := app.Group("/api", s.readyGate)
	api.Get("/search", s.handleSearch)
	api.Get("/stats", s.handleStats)

	app.Static("/", s.cfg.WebRoot)

	return app
}

// errorHandler renders every unhandled error as JSON, so a client never has to
// parse an HTML error page from an API endpoint.
func errorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	var fe *fiber.Error
	if e, ok := err.(*fiber.Error); ok {
		fe = e
		code = e.Code
	}
	if code >= 500 {
		slog.Error("request failed",
			"path", c.Path(), "method", c.Method(),
			"request_id", requestID(c), "error", err)
	}
	msg := "internal_error"
	if fe != nil {
		msg = fe.Message
	}
	return c.Status(code).JSON(fiber.Map{
		"error":      msg,
		"request_id": requestID(c),
	})
}

// requestID reads the ID injected by the requestid middleware.
func requestID(c *fiber.Ctx) string {
	if v, ok := c.Locals(requestid.ConfigDefault.ContextKey).(string); ok {
		return v
	}
	return ""
}
