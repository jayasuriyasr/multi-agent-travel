package seat

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// SeatProvider is the interface for fetching live seat availability
// from an external data source (e.g., IRCTC API, bus operator API).
// Retained for future real-provider injection via dependency inversion.
type SeatProvider interface {
	FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error)
}

// FetchSeats is the package-level mock provider used by the poller during
// development. It simulates a real network call with a random 50–200 ms
// delay and returns realistic, randomized seat availability.
//
// Classes returned:
//   - "lower"  : 0–10 seats  (reserved berth, premium)
//   - "upper"  : 0–10 seats  (reserved berth)
//   - "seater" : 0–20 seats  (unreserved / general class)
//
// Replace with a real HTTPProvider call when live credentials are available.
func FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error) {
	// Simulate variable network latency (50–200 ms)
	delay := time.Duration(50+rand.Intn(151)) * time.Millisecond
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, fmt.Errorf("fetchSeats cancelled for %s/%s: %w", tripID, date, ctx.Err())
	}

	return map[string]int{
		"lower":  rand.Intn(11),  // 0–10
		"upper":  rand.Intn(11),  // 0–10
		"seater": rand.Intn(21),  // 0–20
	}, nil
}

// ── Real provider skeleton (wired on production day) ─────────────────────────

// HTTPProvider fetches seat data from a real external API.
type HTTPProvider struct {
	BaseURL string
}

// FetchSeats makes an HTTP request to the external provider.
// TODO: Implement with real HTTP client, circuit breaker, and retry logic.
func (h *HTTPProvider) FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error) {
	return nil, fmt.Errorf("HTTPProvider not implemented: configure BaseURL and implement")
}
