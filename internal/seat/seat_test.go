package seat

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

func TestCompareStreamIDs(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0", "1-0", -1},
		{"1-0", "1-0", 0},
		{"1-0", "1-1", -1},
		{"1-1", "1-0", 1},
		{"2-0", "10-0", -1},   // numeric, not lexicographic
		{"100-5", "100-4", 1}, // sequence breaks the tie
		{"1692800000000-0", "1692800000001-0", -1},
	}
	for _, tc := range cases {
		if got := compareStreamIDs(tc.a, tc.b); got != tc.want {
			t.Errorf("compareStreamIDs(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestClassifyZone(t *testing.T) {
	now := time.Now().Unix()
	cases := []struct {
		name string
		dep  int64
		want string
	}{
		{"departing in an hour", now + 3600, "RED"},
		{"already departed", now - 3600, "RED"},
		{"departing tomorrow", now + 30*3600, "YELLOW"},
		{"departing in four days", now + 96*3600, "GREEN"},
		{"departing in three weeks", now + 21*24*3600, "COLD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyZone(tc.dep).Name; got != tc.want {
				t.Errorf("ClassifyZone = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClassifyZone_IntervalsShortenWithUrgency(t *testing.T) {
	for i := 1; i < len(Zones); i++ {
		if Zones[i].Interval <= Zones[i-1].Interval {
			t.Errorf("zone %s should poll less often than %s", Zones[i].Name, Zones[i-1].Name)
		}
		if Zones[i].MaxHours <= Zones[i-1].MaxHours {
			t.Errorf("zone thresholds must increase: %s then %s", Zones[i-1].Name, Zones[i].Name)
		}
	}
}

// Go randomises map iteration, so hashing json.Marshal output directly gives a
// different hash for the same data on different runs — and every poll then
// looks like a change, firing a dirty-stream write and a full refresh for data
// that never moved.
func TestCanonicalHash_IsOrderIndependent(t *testing.T) {
	a := map[string]int{"lower": 3, "upper": 6, "seater": 12}
	first := canonicalHash(a)
	for i := 0; i < 200; i++ {
		b := map[string]int{"seater": 12, "upper": 6, "lower": 3}
		if got := canonicalHash(b); got != first {
			t.Fatalf("hash is not order independent: %s vs %s", first, got)
		}
	}
}

func TestCanonicalHash_DetectsChanges(t *testing.T) {
	base := canonicalHash(map[string]int{"lower": 3})
	if canonicalHash(map[string]int{"lower": 4}) == base {
		t.Error("a changed count must change the hash")
	}
	if canonicalHash(map[string]int{"lower": 3, "upper": 0}) == base {
		t.Error("an added class must change the hash")
	}
	if canonicalHash(map[string]int{"upper": 3}) == base {
		t.Error("a renamed class must change the hash")
	}
	if canonicalHash(map[string]int{}) == base {
		t.Error("an empty map must not collide with a populated one")
	}
}

func TestMockProvider_IsDeterministicForAGivenSeed(t *testing.T) {
	p1 := NewMockProvider(42)
	p1.MinDelay, p1.MaxDelay = time.Millisecond, 2*time.Millisecond
	p2 := NewMockProvider(42)
	p2.MinDelay, p2.MaxDelay = time.Millisecond, 2*time.Millisecond

	a, err := p1.FetchSeats(context.Background(), "T1", "2026-08-23")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p2.FetchSeats(context.Background(), "T1", "2026-08-23")
	if err != nil {
		t.Fatal(err)
	}
	for class, v := range a {
		if b[class] != v {
			t.Fatalf("same seed should give the same availability: %v vs %v", a, b)
		}
	}
	for _, class := range []string{"lower", "upper", "seater"} {
		if _, ok := a[class]; !ok {
			t.Errorf("missing seat class %q", class)
		}
	}
}

func TestMockProvider_RespectsCancellation(t *testing.T) {
	p := NewMockProvider(1)
	p.MinDelay, p.MaxDelay = time.Second, 2*time.Second

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if _, err := p.FetchSeats(ctx, "T1", "2026-08-23"); err == nil {
		t.Fatal("want an error from a cancelled context")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("cancellation should return promptly, took %v", elapsed)
	}
}

func TestHTTPProvider_RequiresBaseURL(t *testing.T) {
	p := NewHTTPProvider("", "", time.Second)
	if _, err := p.FetchSeats(context.Background(), "T1", "2026-08-23"); err == nil {
		t.Fatal("want an error when BaseURL is unset")
	}
}

func TestHTTPProvider_HasABoundedTimeout(t *testing.T) {
	// An external API that hangs must not pin a worker forever.
	p := NewHTTPProvider("http://example.invalid", "", 0)
	if p.Client.Timeout <= 0 {
		t.Fatal("a zero timeout must fall back to a bounded default")
	}
}

func TestSyncerCursor(t *testing.T) {
	s := NewSyncer(nil, SyncerConfig{})
	if s.Cursor() != "0" {
		t.Fatalf("a new syncer should start at the stream beginning, got %q", s.Cursor())
	}
	s.setCursor("1692800000000-3")
	if s.Cursor() != "1692800000000-3" {
		t.Fatalf("cursor = %q", s.Cursor())
	}
}

func TestSyncerConfig_ZeroValueUsesDefaults(t *testing.T) {
	got := SyncerConfig{}.withDefaults()
	checks := []struct {
		name      string
		got, want any
	}{
		{"MGetChunk", got.MGetChunk, DefaultMGetChunk},
		{"StreamBatch", got.StreamBatch, DefaultStreamBatch},
		{"StreamBlock", got.StreamBlock, DefaultStreamBlock},
		{"ColdStartAttempts", got.ColdStartAttempts, DefaultColdStartAttempts},
		{"MinResyncInterval", got.MinResyncInterval, DefaultMinResyncInterval},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// A cold start scans every known trip, and stream lag is exactly what a
// struggling Redis produces. Without a floor between re-synchronisations, the
// answer to "Redis is unhealthy" becomes "read everything from Redis",
// repeatedly, for as long as the lag lasts.
func TestSyncer_ResyncIsThrottled(t *testing.T) {
	s := NewSyncer(nil, SyncerConfig{MinResyncInterval: time.Hour})

	if wait := s.resyncWait(); wait != 0 {
		t.Fatalf("the first resync must not be delayed, got %v", wait)
	}

	s.markResynced()
	wait := s.resyncWait()
	if wait <= 0 {
		t.Fatal("a second resync immediately after the first must be throttled")
	}
	if wait > time.Hour {
		t.Fatalf("wait %v exceeds the configured interval", wait)
	}

	// A short interval lets the next one through promptly.
	fast := NewSyncer(nil, SyncerConfig{MinResyncInterval: time.Millisecond})
	fast.markResynced()
	time.Sleep(5 * time.Millisecond)
	if wait := fast.resyncWait(); wait != 0 {
		t.Fatalf("the throttle should have expired, got %v", wait)
	}
}
