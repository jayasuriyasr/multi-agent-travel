// Package state owns the SEAT_SIGNAL double-buffered atomic pointers and the
// global READY flag. Deliberately kept tiny to minimize the blast radius of
// Go's most dangerous gotcha: concurrent map read+write = fatal panic (G7).
package state

import (
	"sync/atomic"

	"axentra/internal/model"
)

// SignalBuffer is the in-memory seat availability map keyed by TripKey.
type SignalBuffer = map[model.TripKey]model.SeatSignal

var (
	liveSignal atomic.Pointer[SignalBuffer]

	// READY is the global readiness flag. 0 = warming up, 1 = ready.
	// Set to 1 only after ColdStart completes successfully.
	ready int32
)

func init() {
	a := make(SignalBuffer)
	liveSignal.Store(&a)
}

// LiveSignal returns a pointer to the current read-only signal buffer.
// RAPTOR goroutines call this ONCE at the start of a search (G11).
func LiveSignal() *SignalBuffer {
	return liveSignal.Load()
}

// SwapSignal atomically swaps in a new staging buffer as the live buffer.
// The caller must have built 'staging' as a completely new map — never
// mutate the map that LiveSignal() currently returns (G7, G9).
func SwapSignal(staging SignalBuffer) {
	liveSignal.Store(&staging)
}

// MarkReady sets the global readiness flag to true.
// Called exactly once after ColdStart succeeds (G8).
func MarkReady() {
	atomic.StoreInt32(&ready, 1)
}

// IsReady returns true if the system has completed cold-start initialization.
func IsReady() bool {
	return atomic.LoadInt32(&ready) == 1
}
