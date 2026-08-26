package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"axentra/internal/config"
	"axentra/internal/model"
	"axentra/internal/schedule"
	"axentra/internal/state"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

const testDate = "2026-08-23"

func at(hour, min int) int64 {
	d, _ := time.Parse(model.DateLayout, testDate)
	return d.Add(time.Duration(hour)*time.Hour + time.Duration(min)*time.Minute).Unix()
}

// testApp publishes a tiny schedule and returns a ready-to-query Fiber app.
func testApp(t *testing.T) *fiber.App {
	t.Helper()

	buf := &schedule.RouteBuffer{
		TripIndex:    map[model.TripKey]model.TripLocation{},
		StopToRoutes: map[string][]schedule.RouteStop{},
		Footpaths:    map[string][]model.Footpath{},
	}
	key := model.TripKey{TripID: "T1", Date: testDate}
	buf.Routes = append(buf.Routes, model.RouteEntry{RouteID: "R1", StopIDs: []string{"A", "B"}})
	buf.StopTimes = append(buf.StopTimes, []model.TripStopTimes{{
		Key: key, StationIDs: []string{"A", "B"},
		Arrivals: []int64{at(9, 0), at(10, 0)}, Departures: []int64{at(9, 0), at(10, 0)},
	}})
	buf.RouteFIFO = append(buf.RouteFIFO, true)
	buf.TripIndex[key] = model.TripLocation{}
	buf.StopToRoutes["A"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: 0}}
	buf.StopToRoutes["B"] = []schedule.RouteStop{{RouteIdx: 0, StopPos: 1}}
	schedule.SwapRoutes(buf)
	state.SwapSignal(make(state.SignalBuffer))
	state.MarkReady()

	cfg := &config.Config{
		HTTPPort: "0", RequestTimeout: 5 * time.Second, HTTPReadTimeout: 5 * time.Second,
		BodyLimitBytes: 1 << 20, RateLimitPerMin: 0, CORSOrigins: "*", WebRoot: ".",
		MaxRounds: model.DefaultMaxRounds, DateWindowDays: model.DefaultDateWindowDays,
		SearchCandidates: 9, SearchResults: 5, SeatProvider: "mock",
		RedisMGetChunk: 512, BreakerThreshold: 3, BreakerCooldown: time.Second,
	}
	// No Redis client: seat validation runs in lenient mode, so search results
	// come back unvalidated, which is what these tests want to inspect.
	return NewServer(cfg, nil).BuildApp()
}

func do(t *testing.T, app *fiber.App, target string) (*http.Response, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("request %s: %v", target, err)
	}
	body, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &parsed)
	}
	return resp, parsed
}

func searchURL(extra string) string {
	return fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=%d%s", testDate, at(8, 0), extra)
}

func TestHealthEndpoints(t *testing.T) {
	app := testApp(t)

	resp, body := do(t, app, "/healthz/live")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("live probe = %d, want 200", resp.StatusCode)
	}
	if body["status"] != "alive" {
		t.Errorf("live body = %v", body)
	}

	resp, body = do(t, app, "/healthz/ready")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("ready probe = %d, want 200", resp.StatusCode)
	}
	// Serving without seat data is a real, nameable state, not silent success.
	if body["status"] != "degraded_no_seat_data" {
		t.Errorf("want the no-seat-data state to be reported, got %v", body["status"])
	}
	for _, field := range []string{
		"routes", "trips", "stations", "seat_signals", "has_seat_data",
		"stale_seat_signals", "stale_fraction", "seat_data_age_seconds", "seat_validation",
	} {
		if _, ok := body[field]; !ok {
			t.Errorf("readiness body is missing %q: %v", field, body)
		}
	}
}

func TestReadyGate_BlocksWhileWarmingUp(t *testing.T) {
	app := testApp(t)
	state.MarkNotReady()
	t.Cleanup(state.MarkReady)

	resp, body := do(t, app, searchURL(""))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 while warming up, got %d", resp.StatusCode)
	}
	if body["error"] != "warming_up" {
		t.Errorf("want a warming_up error, got %v", body)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("a 503 should tell the client when to retry")
	}

	// Health probes must stay answerable while the gate is shut, or an
	// orchestrator cannot tell "starting" from "broken".
	if resp, _ := do(t, app, "/healthz/live"); resp.StatusCode != http.StatusOK {
		t.Errorf("liveness must not be gated, got %d", resp.StatusCode)
	}
	if resp, _ := do(t, app, "/healthz/ready"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readiness should report 503 while warming up, got %d", resp.StatusCode)
	}
}

func TestSearch_Success(t *testing.T) {
	app := testApp(t)
	resp, body := do(t, app, searchURL(""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %v", resp.StatusCode, body)
	}
	if body["result_count"].(float64) != 1 {
		t.Fatalf("want 1 result, got %v", body["result_count"])
	}
	paths := body["paths"].([]any)
	leg := paths[0].(map[string]any)["legs"].([]any)[0].(map[string]any)
	if leg["trip_id"] != "T1" {
		t.Errorf("want trip T1, got %v", leg["trip_id"])
	}
	if leg["kind"] != string(model.LegTransit) {
		t.Errorf("legs should be labelled by kind, got %v", leg["kind"])
	}
	for _, field := range []string{"search_duration_ms", "validate_duration_ms", "candidate_count", "max_rounds"} {
		if _, ok := body[field]; !ok {
			t.Errorf("response is missing %q", field)
		}
	}
}

func TestSearch_EmptyResultIsAnArrayNotNull(t *testing.T) {
	app := testApp(t)
	// Depart after the only service has gone.
	_, body := do(t, app, fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=%d", testDate, at(23, 0)))
	if _, ok := body["paths"].([]any); !ok {
		t.Fatalf("paths should serialise as [] when empty, got %#v", body["paths"])
	}
}

func TestSearch_ValidationErrors(t *testing.T) {
	app := testApp(t)

	cases := []struct {
		name   string
		target string
		status int
	}{
		{"missing everything", "/api/search", http.StatusBadRequest},
		{"missing destination", fmt.Sprintf("/api/search?origin=A&date=%s&dep_time=%d", testDate, at(8, 0)), http.StatusBadRequest},
		{"missing dep_time", fmt.Sprintf("/api/search?origin=A&destination=B&date=%s", testDate), http.StatusBadRequest},
		{"same origin and destination", fmt.Sprintf("/api/search?origin=A&destination=A&date=%s&dep_time=%d", testDate, at(8, 0)), http.StatusBadRequest},
		{"malformed date", fmt.Sprintf("/api/search?origin=A&destination=B&date=23-08-2026&dep_time=%d", at(8, 0)), http.StatusBadRequest},
		{"non-numeric dep_time", fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=soon", testDate), http.StatusBadRequest},
		{"negative dep_time", fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=-5", testDate), http.StatusBadRequest},
		{"milliseconds instead of seconds", fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=%d", testDate, at(8, 0)*1000), http.StatusBadRequest},
		{"too many passengers", searchURL("&passengers=999"), http.StatusBadRequest},
		{"max_rounds out of range", searchURL("&max_rounds=99"), http.StatusBadRequest},
		{"negative min_transfer", searchURL("&min_transfer=-1"), http.StatusBadRequest},
		{"unknown origin", fmt.Sprintf("/api/search?origin=NOPE&destination=B&date=%s&dep_time=%d", testDate, at(8, 0)), http.StatusNotFound},
		{"unknown destination", fmt.Sprintf("/api/search?origin=A&destination=NOPE&date=%s&dep_time=%d", testDate, at(8, 0)), http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := do(t, app, tc.target)
			if resp.StatusCode != tc.status {
				t.Fatalf("want %d, got %d (%v)", tc.status, resp.StatusCode, body)
			}
			if body["error"] == nil || body["error"] == "" {
				t.Errorf("an error response should explain itself, got %v", body)
			}
		})
	}
}

// Timestamps past 2038 must survive: strconv.Atoi (which Fiber's QueryInt uses)
// silently mangles them on a 32-bit build, and future travel dates are the
// entire point of this service.
func TestSearch_AcceptsPost2038Timestamps(t *testing.T) {
	app := testApp(t)
	future := time.Date(2040, 6, 1, 9, 0, 0, 0, time.UTC)
	target := fmt.Sprintf("/api/search?origin=A&destination=B&date=%s&dep_time=%d",
		future.Format(model.DateLayout), future.Unix())

	resp, body := do(t, app, target)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 for a 2040 departure, got %d: %v", resp.StatusCode, body)
	}
}

func TestSearch_HonoursQueryOverrides(t *testing.T) {
	app := testApp(t)
	_, body := do(t, app, searchURL("&max_rounds=2&min_transfer=300"))
	if body["max_rounds"].(float64) != 2 {
		t.Errorf("max_rounds override ignored: %v", body["max_rounds"])
	}
	if body["min_transfer_seconds"].(float64) != 300 {
		t.Errorf("min_transfer override ignored: %v", body["min_transfer_seconds"])
	}
}

func TestStatsEndpoint(t *testing.T) {
	app := testApp(t)
	resp, body := do(t, app, "/api/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	for _, section := range []string{"schedule", "seats", "search"} {
		if _, ok := body[section]; !ok {
			t.Errorf("stats is missing the %q section: %v", section, body)
		}
	}
}

func TestUnknownRouteIsNotAServerError(t *testing.T) {
	app := testApp(t)
	resp, _ := do(t, app, "/no/such/path")
	if resp.StatusCode >= 500 {
		t.Fatalf("an unknown path should not be a server error, got %d", resp.StatusCode)
	}
}

func TestRateLimiter(t *testing.T) {
	cfg := &config.Config{
		RequestTimeout: time.Second, HTTPReadTimeout: time.Second, BodyLimitBytes: 1 << 20,
		RateLimitPerMin: 2, CORSOrigins: "*", WebRoot: ".",
		MaxRounds: model.DefaultMaxRounds, SearchCandidates: 9, SearchResults: 5,
		RedisMGetChunk: 512, BreakerThreshold: 3, BreakerCooldown: time.Second,
	}
	app := NewServer(cfg, nil).BuildApp()
	state.MarkReady()

	var limited bool
	for i := 0; i < 6; i++ {
		resp, _ := do(t, app, "/api/stats")
		if resp.StatusCode == http.StatusTooManyRequests {
			limited = true
		}
	}
	if !limited {
		t.Fatal("want a 429 once the per-minute limit is exceeded")
	}

	// Probes stay reachable under the limit, so an instance under load is never
	// mistaken for a dead one.
	resp, _ := do(t, app, "/healthz/live")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("health probes must sit outside the rate limiter, got %d", resp.StatusCode)
	}
}

// newServerWithHealthyValidator builds a Server whose validator has a client
// and has not failed yet, so it reports healthy without a live Redis. Nothing
// calls it, so no request is ever made — this isolates the health logic.
func newServerWithHealthyValidator(t *testing.T, threshold int, cooldown time.Duration) *Server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 20 * time.Millisecond,
		ReadTimeout: 20 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	return NewServer(&config.Config{
		RequestTimeout: time.Second, HTTPReadTimeout: time.Second, BodyLimitBytes: 1 << 20,
		CORSOrigins: "*", WebRoot: ".", MaxRounds: model.DefaultMaxRounds,
		SearchCandidates: 9, SearchResults: 5, SeatProvider: "mock",
		RedisMGetChunk: 512, BreakerThreshold: threshold, BreakerCooldown: cooldown,
	}, rdb)
}

func signals(pairs map[string]bool) state.SignalBuffer {
	buf := make(state.SignalBuffer, len(pairs))
	for id, stale := range pairs {
		buf[model.TripKey{TripID: id, Date: testDate}] = model.SeatSignal{Total: 1, Stale: stale}
	}
	return buf
}

// Readiness must name the specific degradation. "No seat data", "validation is
// down" and "the data has gone stale" all look identical from the outside —
// results that vanish — so the difference has to be said out loud.
func TestReady_ReportsTheSpecificDegradation(t *testing.T) {
	testApp(t) // publishes the schedule and marks ready
	t.Cleanup(func() { state.SwapSignal(make(state.SignalBuffer)) })

	srv := newServerWithHealthyValidator(t, 3, time.Hour)

	cases := []struct {
		name   string
		buffer state.SignalBuffer
		want   string
	}{
		{"no seat data at all", state.SignalBuffer{}, "degraded_no_seat_data"},
		{"most signals stale", signals(map[string]bool{"A": true, "B": true, "C": false}), "degraded_stale_seat_data"},
		{"a minority stale is fine", signals(map[string]bool{"A": true, "B": false, "C": false}), "ready"},
		{"all fresh", signals(map[string]bool{"A": false, "B": false}), "ready"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state.SwapSignal(tc.buffer)
			if got := srv.degradation(); got != tc.want {
				t.Fatalf("degradation = %q, want %q", got, tc.want)
			}
		})
	}
}

// A configuration with no Redis client cannot validate anything, and that is
// worth reporting rather than serving unvalidated results in silence.
func TestReady_NoRedisClientIsReportedAsUnavailable(t *testing.T) {
	app := testApp(t) // built with a nil client
	t.Cleanup(func() { state.SwapSignal(make(state.SignalBuffer)) })

	state.SwapSignal(signals(map[string]bool{"A": false}))
	_, body := do(t, app, "/healthz/ready")
	if body["status"] != "degraded_seat_validation_unavailable" {
		t.Fatalf("status = %v, want degraded_seat_validation_unavailable", body["status"])
	}
}

func TestReady_ReportsStaleCounts(t *testing.T) {
	app := testApp(t)
	t.Cleanup(func() { state.SwapSignal(make(state.SignalBuffer)) })

	state.SwapSignal(signals(map[string]bool{"A": true, "B": true, "C": false, "D": false}))
	_, body := do(t, app, "/healthz/ready")

	if body["seat_signals"].(float64) != 4 {
		t.Errorf("seat_signals = %v, want 4", body["seat_signals"])
	}
	if body["stale_seat_signals"].(float64) != 2 {
		t.Errorf("stale_seat_signals = %v, want 2", body["stale_seat_signals"])
	}
	if body["stale_fraction"].(float64) != 0.5 {
		t.Errorf("stale_fraction = %v, want 0.5", body["stale_fraction"])
	}
}

// A tripped validation breaker outranks staleness: if nothing can be confirmed,
// saying the data is merely stale understates the problem.
func TestReady_BreakerOutranksStaleness(t *testing.T) {
	testApp(t)
	srv := newServerWithHealthyValidator(t, 1, time.Hour)
	app := srv.BuildApp()

	state.SwapSignal(signals(map[string]bool{"A": true, "B": true}))
	t.Cleanup(func() { state.SwapSignal(make(state.SignalBuffer)) })

	if got := srv.degradation(); got != "degraded_stale_seat_data" {
		t.Fatalf("degradation = %q, want degraded_stale_seat_data before the breaker trips", got)
	}

	// One failed validation is enough at BreakerThreshold=1.
	srv.validator.Validate(context.Background(), []model.Path{{
		Legs: []model.Leg{{Kind: model.LegTransit, TripID: "A", Date: testDate}},
	}}, "lower", 1)

	if got := srv.degradation(); got != "degraded_seat_validation_unavailable" {
		t.Fatalf("degradation = %q, want the breaker state to win", got)
	}
	_, body := do(t, app, "/healthz/ready")
	if body["status"] != "degraded_seat_validation_unavailable" {
		t.Errorf("readiness status = %v", body["status"])
	}
}

// The search response says whether its results were actually confirmed. A
// validator with no client reporting a healthy-looking "closed" would be worse
// than saying nothing at all.
func TestSearch_ReportsValidationState(t *testing.T) {
	app := testApp(t) // built with no Redis client
	_, body := do(t, app, searchURL(""))
	if body["seat_validation"] != "unconfigured" {
		t.Errorf("seat_validation = %v, want unconfigured", body["seat_validation"])
	}

	srv := newServerWithHealthyValidator(t, 3, time.Hour)
	if got := srv.validator.Status(); got != "closed" {
		t.Errorf("a configured, unfailed validator should report closed, got %q", got)
	}
}

func TestStats_ReportsSeatHealth(t *testing.T) {
	app := testApp(t)
	_, body := do(t, app, "/api/stats")
	seats := body["seats"].(map[string]any)
	for _, field := range []string{"signals", "stale_signals", "stale_fraction", "validation", "validation_healthy"} {
		if _, ok := seats[field]; !ok {
			t.Errorf("stats.seats is missing %q: %v", field, seats)
		}
	}
}
