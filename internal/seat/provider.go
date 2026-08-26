package seat

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

// Provider fetches live seat availability for a trip from some source of
// truth. The poller depends on this interface, never on a concrete provider,
// so a deployment can swap the data source without touching the polling,
// locking or change-detection logic.
type Provider interface {
	// FetchSeats returns available seats per class for a trip, or an error.
	// Implementations must respect ctx and must not block indefinitely.
	FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error)
	// Name identifies the provider in logs and on the readiness endpoint.
	Name() string
}

// ── Mock provider ────────────────────────────────────────────────────────────

// MockProvider simulates an external seat API: a 50–200 ms round trip and
// plausible availability. It is the default so the system is runnable end to
// end without third-party credentials.
type MockProvider struct {
	// MinDelay and MaxDelay bound the simulated network latency. Zero values
	// mean 50 ms and 200 ms.
	MinDelay, MaxDelay time.Duration
	// rng is seeded per-provider; math/rand's global source is fine for
	// simulation but a dedicated one keeps tests reproducible.
	rnd *rand.Rand
}

// NewMockProvider builds a MockProvider with the given seed. Pass a fixed seed
// in tests for deterministic availability.
func NewMockProvider(seed int64) *MockProvider {
	return &MockProvider{
		MinDelay: 50 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond,
		rnd:      rand.New(rand.NewSource(seed)),
	}
}

func (m *MockProvider) Name() string { return "mock" }

// FetchSeats returns randomised availability across the three seat classes.
func (m *MockProvider) FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error) {
	lo, hi := m.MinDelay, m.MaxDelay
	if lo <= 0 {
		lo = 50 * time.Millisecond
	}
	if hi <= lo {
		hi = lo + 150*time.Millisecond
	}
	delay := lo + time.Duration(m.rnd.Int63n(int64(hi-lo)+1))

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, fmt.Errorf("fetch seats %s/%s: %w", tripID, date, ctx.Err())
	}

	return map[string]int{
		"lower":  m.rnd.Intn(11), // 0–10
		"upper":  m.rnd.Intn(11), // 0–10
		"seater": m.rnd.Intn(21), // 0–20
	}, nil
}

// ── HTTP provider ────────────────────────────────────────────────────────────

// HTTPProvider fetches seat data from a real external API. The response is
// expected to be a JSON object of {"<class>": <available>}.
type HTTPProvider struct {
	BaseURL string
	// APIKey, when set, is sent as an Authorization: Bearer header.
	APIKey string
	Client *http.Client
}

// NewHTTPProvider builds an HTTPProvider with a bounded client. A per-request
// timeout is mandatory: an external seat API that hangs must not pin an asynq
// worker forever.
func NewHTTPProvider(baseURL, apiKey string, timeout time.Duration) *HTTPProvider {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPProvider{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (h *HTTPProvider) Name() string { return "http" }

// FetchSeats queries BaseURL with trip_id and date query parameters.
func (h *HTTPProvider) FetchSeats(ctx context.Context, tripID, date string) (map[string]int, error) {
	if h.BaseURL == "" {
		return nil, fmt.Errorf("http provider: BaseURL is not configured")
	}
	u, err := url.Parse(h.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("http provider: bad BaseURL %q: %w", h.BaseURL, err)
	}
	q := u.Query()
	q.Set("trip_id", tripID)
	q.Set("date", date)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("http provider: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if h.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.APIKey)
	}

	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http provider: request %s/%s: %w", tripID, date, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http provider: %s/%s returned status %d", tripID, date, resp.StatusCode)
	}

	var seats map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&seats); err != nil {
		return nil, fmt.Errorf("http provider: decode %s/%s: %w", tripID, date, err)
	}
	if seats == nil {
		return nil, fmt.Errorf("http provider: %s/%s returned null body", tripID, date)
	}
	return seats, nil
}
