// Package state owns the double-buffered seat signal pointer and the global
// readiness flag. It is deliberately tiny: this is the blast radius of Go's
// most dangerous concurrency gotcha — a concurrent map read and write is a
// fatal runtime panic that recover() cannot catch.
package state

import (
	"sync/atomic"
	"time"

	"axentra/internal/model"
)

// SignalBuffer is the in-memory seat availability map keyed by TripKey.
// A published buffer is immutable: writers build a new map and swap it in.
type SignalBuffer = map[model.TripKey]model.SeatSignal

var (
	liveSignal atomic.Pointer[SignalBuffer]

	// ready is the global readiness flag: false = warming up, true = serving.
	ready atomic.Bool
	// seatDataPresent records whether the seat buffer has ever been populated.
	// Readiness and seat coverage are separate facts: the service can serve
	// searches with an empty seat buffer, it just cannot validate them.
	seatDataPresent atomic.Bool

	// Counters kept alongside the buffer so health checks are O(1) and so
	// staleness is a reported fact rather than something an operator has to
	// infer from an empty result set.
	signalCount  atomic.Int64
	staleCount   atomic.Int64
	lastSwapUnix atomic.Int64
)

func init() {
	empty := make(SignalBuffer)
	liveSignal.Store(&empty)
}

// LiveSignal returns a pointer to the current read-only signal buffer.
// A search calls this ONCE at the start and uses that snapshot throughout.
func LiveSignal() *SignalBuffer { return liveSignal.Load() }

// SwapSignal atomically publishes a new buffer.
//
// The caller must have built staging as a completely new map. Never mutate the
// map that LiveSignal() currently returns — searches are reading it right now,
// without a lock, and a concurrent write is an unrecoverable crash.
func SwapSignal(staging SignalBuffer) {
	stale := int64(0)
	for _, sig := range staging {
		if sig.Stale {
			stale++
		}
	}

	liveSignal.Store(&staging)
	signalCount.Store(int64(len(staging)))
	staleCount.Store(stale)
	lastSwapUnix.Store(time.Now().Unix())
	if len(staging) > 0 {
		seatDataPresent.Store(true)
	}
}

// SignalCount returns the number of trips currently carrying a seat signal.
func SignalCount() int { return int(signalCount.Load()) }

// StaleSignalCount returns how many of those signals are older than their
// trip's polling cadence allows.
func StaleSignalCount() int { return int(staleCount.Load()) }

// SeatDataAge returns how long ago the seat buffer was last published, or zero
// if it never has been.
func SeatDataAge() time.Duration {
	last := lastSwapUnix.Load()
	if last == 0 {
		return 0
	}
	return time.Since(time.Unix(last, 0))
}

// StaleFraction returns the share of signals that are stale, in [0,1].
//
// A high value means the optimistic in-memory pre-filter has effectively
// stopped filtering and every result is riding on the strict validator — worth
// surfacing before it turns into an incident.
func StaleFraction() float64 {
	total := signalCount.Load()
	if total == 0 {
		return 0
	}
	return float64(staleCount.Load()) / float64(total)
}

// MarkReady opens the service to traffic.
func MarkReady() { ready.Store(true) }

// MarkNotReady closes the service to traffic, used during shutdown so
// load balancers drain this instance before the listener goes away.
func MarkNotReady() { ready.Store(false) }

// IsReady reports whether the service should accept API traffic.
func IsReady() bool { return ready.Load() }

// HasSeatData reports whether any seat signal has ever been loaded. When this
// is false every search result will be rejected by the pessimistic validator,
// so it is surfaced on the readiness endpoint rather than left to be guessed
// from an empty result set.
func HasSeatData() bool { return seatDataPresent.Load() }
