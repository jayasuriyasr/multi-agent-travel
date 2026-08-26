// Package breaker implements a small circuit breaker.
//
// It exists for one specific failure mode: a dependency that is already
// struggling should not be handed every request that arrives while it
// struggles. Without one, a degraded Redis gets the full search load thrown at
// it — each request waiting out a timeout before failing — which turns a slow
// dependency into an unavailable one and keeps it that way.
package breaker

import (
	"sync"
	"time"
)

// State is the breaker's current disposition.
type State string

const (
	// StateClosed is normal operation: calls go through.
	StateClosed State = "closed"
	// StateOpen means the dependency is considered unhealthy and calls are
	// rejected immediately without touching it.
	StateOpen State = "open"
	// StateHalfOpen lets a single probe through to test recovery.
	StateHalfOpen State = "half_open"
)

// Breaker trips after a number of consecutive failures and recovers by letting
// one probe through after a cooldown.
//
// Consecutive rather than proportional failures is deliberate: seat validation
// either reaches Redis or it does not, so a run of failures is a much clearer
// signal than a rate, and it needs no window bookkeeping.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time // injectable for tests

	failures int
	state    State
	openedAt time.Time
	probing  bool
}

// New returns a Breaker that opens after threshold consecutive failures and
// admits a probe once cooldown has elapsed. Non-positive values fall back to
// 5 failures and 15 seconds.
func New(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 15 * time.Second
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, state: StateClosed, now: time.Now}
}

// Allow reports whether a call may proceed.
//
// While open it returns false until the cooldown expires, then admits exactly
// one probe. Further calls are refused until that probe reports back, so a
// recovering dependency gets one request rather than the whole backlog.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = StateHalfOpen
		b.probing = true
		return true
	default: // half-open
		if b.probing {
			return false // a probe is already in flight
		}
		b.probing = true
		return true
	}
}

// Success records a successful call, closing the breaker.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	b.state = StateClosed
}

// Failure records a failed call. A failure during a probe re-opens the breaker
// immediately and restarts the cooldown, so a dependency that is still down
// does not get probed every request.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probing = false
	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state = StateOpen
		b.openedAt = b.now()
	}
}

// State returns the current state, transitioning an expired open breaker to
// half-open so callers observing it see the same thing Allow would do.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		return StateHalfOpen
	}
	return b.state
}

// Healthy reports whether calls are currently getting through.
func (b *Breaker) Healthy() bool { return b.State() == StateClosed }
