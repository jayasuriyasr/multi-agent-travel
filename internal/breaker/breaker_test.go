package breaker

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets the tests move time without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := New(threshold, cooldown)
	b.now = clock.Now
	return b, clock
}

func TestBreaker_ClosedUntilThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("call %d should be allowed below the threshold", i)
		}
		b.Failure()
	}
	if !b.Allow() {
		t.Fatal("two failures out of three must not open the breaker")
	}
	if b.State() != StateClosed {
		t.Errorf("state = %s, want closed", b.State())
	}
}

func TestBreaker_OpensAtThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)
	for i := 0; i < 3; i++ {
		b.Allow()
		b.Failure()
	}
	if b.Allow() {
		t.Fatal("the breaker must reject calls once open")
	}
	if b.State() != StateOpen {
		t.Errorf("state = %s, want open", b.State())
	}
	if b.Healthy() {
		t.Error("an open breaker is not healthy")
	}
}

func TestBreaker_SuccessResetsTheFailureRun(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)
	b.Allow()
	b.Failure()
	b.Allow()
	b.Failure()
	b.Allow()
	b.Success() // an intervening success clears the run

	b.Allow()
	b.Failure()
	if !b.Allow() {
		t.Fatal("one failure after a success must not open the breaker")
	}
}

func TestBreaker_AdmitsOneProbeAfterCooldown(t *testing.T) {
	b, clock := newTestBreaker(2, 30*time.Second)
	b.Allow()
	b.Failure()
	b.Allow()
	b.Failure()

	if b.Allow() {
		t.Fatal("no probe before the cooldown elapses")
	}
	clock.Advance(29 * time.Second)
	if b.Allow() {
		t.Fatal("still no probe one second early")
	}

	clock.Advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("one probe should be admitted once the cooldown expires")
	}
	// The whole point: the backlog does not follow the probe through.
	if b.Allow() {
		t.Fatal("a second call must wait for the in-flight probe")
	}
}

func TestBreaker_ProbeSuccessCloses(t *testing.T) {
	b, clock := newTestBreaker(1, time.Second)
	b.Allow()
	b.Failure()

	clock.Advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be admitted")
	}
	b.Success()

	if !b.Allow() || b.State() != StateClosed {
		t.Fatalf("a successful probe must close the breaker, state = %s", b.State())
	}
}

// A dependency that is still down must not be probed on every request.
func TestBreaker_ProbeFailureRestartsTheCooldown(t *testing.T) {
	b, clock := newTestBreaker(1, 10*time.Second)
	b.Allow()
	b.Failure()

	clock.Advance(11 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be admitted")
	}
	b.Failure()

	if b.Allow() {
		t.Fatal("a failed probe must re-open the breaker immediately")
	}
	clock.Advance(9 * time.Second)
	if b.Allow() {
		t.Fatal("the cooldown must restart from the failed probe, not the original trip")
	}
	clock.Advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("a fresh probe should be admitted after the restarted cooldown")
	}
}

func TestBreaker_DefaultsForInvalidSettings(t *testing.T) {
	b := New(0, 0)
	if b.threshold != 5 || b.cooldown != 15*time.Second {
		t.Fatalf("got threshold=%d cooldown=%v, want the documented fallbacks", b.threshold, b.cooldown)
	}
}

// Searches call this concurrently; run with -race.
func TestBreaker_ConcurrentUse(t *testing.T) {
	b := New(10, time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if b.Allow() {
					if (i+j)%3 == 0 {
						b.Failure()
					} else {
						b.Success()
					}
				}
				_ = b.State()
			}
		}(i)
	}
	wg.Wait()
}
