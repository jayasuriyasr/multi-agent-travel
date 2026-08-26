package api

// Routing lives in server.go: Server.BuildApp mounts every endpoint alongside
// the middleware it depends on, so the ordering constraints between them (for
// example: health probes must sit outside the rate limiter) are visible in one
// place rather than split across files.
//
// Endpoints:
//
//	GET /healthz/live   — process is up
//	GET /healthz/ready  — cold start finished; body reports what is loaded
//	GET /api/search     — journey search (behind the ready gate)
//	GET /api/stats      — in-memory schedule and seat counters
//	GET /*              — static demo page from WEB_ROOT
