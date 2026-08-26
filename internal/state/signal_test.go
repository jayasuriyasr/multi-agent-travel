package state

import (
	"sync"
	"testing"
	"time"

	"axentra/internal/model"
)

func TestLiveSignal_InitialisedEmpty(t *testing.T) {
	if buf := LiveSignal(); buf == nil {
		t.Fatal("LiveSignal must never return nil, even before the first swap")
	}
}

func TestSwapSignal_PublishesAndTracksCoverage(t *testing.T) {
	key := model.TripKey{TripID: "T1", Date: "2026-08-23"}
	SwapSignal(state(map[model.TripKey]model.SeatSignal{
		key: {ByClass: map[string]int{"lower": 4}, Total: 4},
	}))

	got := *LiveSignal()
	if got[key].Total != 4 {
		t.Fatalf("published buffer not visible: %+v", got)
	}
	if SignalCount() != 1 {
		t.Errorf("SignalCount = %d, want 1", SignalCount())
	}
	if !HasSeatData() {
		t.Error("HasSeatData should be true once a non-empty buffer is published")
	}
}

// A published buffer is immutable, so a snapshot taken by a search stays valid
// even as new buffers are published underneath it.
func TestSwapSignal_SnapshotIsStable(t *testing.T) {
	key := model.TripKey{TripID: "T1", Date: "2026-08-23"}
	SwapSignal(state(map[model.TripKey]model.SeatSignal{key: {Total: 1}}))
	snapshot := LiveSignal()

	SwapSignal(state(map[model.TripKey]model.SeatSignal{key: {Total: 99}}))

	if (*snapshot)[key].Total != 1 {
		t.Fatalf("a captured snapshot must not change under the reader, got %d", (*snapshot)[key].Total)
	}
	if (*LiveSignal())[key].Total != 99 {
		t.Fatal("the newest buffer should be visible to new readers")
	}
}

// The whole point of the double buffer: readers and writers must be able to run
// flat out without tripping Go's concurrent map detector. Run with -race.
func TestSwapSignal_ConcurrentReadersAndWriters(t *testing.T) {
	var wg sync.WaitGroup
	const writers, readers, iterations = 4, 8, 500

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := make(SignalBuffer, 4)
				for k := 0; k < 4; k++ {
					buf[model.TripKey{TripID: "T", Date: "2026-08-23"}] = model.SeatSignal{Total: i}
				}
				SwapSignal(buf)
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				for range *LiveSignal() {
				}
			}
		}()
	}
	wg.Wait()
}

func TestReadyFlag(t *testing.T) {
	MarkNotReady()
	if IsReady() {
		t.Fatal("expected not ready")
	}
	MarkReady()
	if !IsReady() {
		t.Fatal("expected ready")
	}
	MarkNotReady()
	if IsReady() {
		t.Fatal("MarkNotReady must close the gate again, so shutdown can drain traffic")
	}
	MarkReady()
}

func state(m map[model.TripKey]model.SeatSignal) SignalBuffer {
	buf := make(SignalBuffer, len(m))
	for k, v := range m {
		buf[k] = v
	}
	return buf
}

// Staleness is a reported fact, not something an operator has to infer from a
// rising rejection rate.
func TestSwapSignal_TracksStaleness(t *testing.T) {
	key := func(id string) model.TripKey { return model.TripKey{TripID: id, Date: "2026-08-23"} }

	SwapSignal(SignalBuffer{
		key("A"): {Total: 1},
		key("B"): {Total: 1, Stale: true},
		key("C"): {Total: 1, Stale: true},
		key("D"): {Total: 1},
	})

	if got := SignalCount(); got != 4 {
		t.Errorf("SignalCount = %d, want 4", got)
	}
	if got := StaleSignalCount(); got != 2 {
		t.Errorf("StaleSignalCount = %d, want 2", got)
	}
	if got := StaleFraction(); got != 0.5 {
		t.Errorf("StaleFraction = %v, want 0.5", got)
	}
	if age := SeatDataAge(); age < 0 || age > time.Minute {
		t.Errorf("SeatDataAge = %v, want something just published", age)
	}
}

func TestStaleFraction_EmptyBufferIsNotStale(t *testing.T) {
	SwapSignal(SignalBuffer{})
	if got := StaleFraction(); got != 0 {
		t.Errorf("an empty buffer has no stale fraction, got %v", got)
	}
	if got := StaleSignalCount(); got != 0 {
		t.Errorf("StaleSignalCount = %d, want 0", got)
	}
}

// Counters must follow the buffer: a refresh that clears staleness has to
// clear the reported staleness too.
func TestSwapSignal_CountersFollowTheBuffer(t *testing.T) {
	key := model.TripKey{TripID: "A", Date: "2026-08-23"}

	SwapSignal(SignalBuffer{key: {Stale: true}})
	if StaleSignalCount() != 1 {
		t.Fatalf("StaleSignalCount = %d, want 1", StaleSignalCount())
	}
	SwapSignal(SignalBuffer{key: {Stale: false}})
	if StaleSignalCount() != 0 {
		t.Fatalf("a fresh buffer must reset the stale count, got %d", StaleSignalCount())
	}
}
