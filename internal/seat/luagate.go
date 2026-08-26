package seat

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"axentra/internal/model"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/seat_gate.lua
var seatGateLuaSource string

var seatGateScript = redis.NewScript(seatGateLuaSource)

// luaGate runs the atomic seat write-through:
//  1. compare the canonical hash of the new data against the stored hash
//  2. if different, update seat:map and seat:hash and XADD to the dirty stream
//  3. always update seat:ts
//
// Returns 1 when the data changed and the stream was appended, 0 when it was
// identical. Doing this in Lua makes the compare-and-write a single atomic
// server-side operation: two workers polling the same trip cannot interleave
// and publish two "changed" events for one change, or lose one.
//
// rdb is passed explicitly rather than captured in a package-level variable so
// the caller owns the client lifetime and tests can supply their own.
func luaGate(ctx context.Context, rdb *redis.Client, tripID, date string, seats map[string]int) (int64, error) {
	seatJSON, err := json.Marshal(seats)
	if err != nil {
		return -1, fmt.Errorf("marshal seat data: %w", err)
	}

	hash := canonicalHash(seats)
	ts := fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1e9)
	tripDate := model.SeatTripDate(tripID, date)

	keys := []string{
		model.SeatHashKey(tripID, date), // KEYS[1] — stored canonical hash
		model.SeatMapKey(tripID, date),  // KEYS[2] — seat availability JSON
		model.SeatTSKey(tripID, date),   // KEYS[3] — last-updated timestamp
		model.DirtyStreamKey,            // KEYS[4] — change notification stream
	}

	result, err := seatGateScript.Run(ctx, rdb, keys,
		hash,             // ARGV[1]
		string(seatJSON), // ARGV[2]
		ts,               // ARGV[3]
		tripDate,         // ARGV[4]
	).Int64()
	if err != nil {
		return -1, fmt.Errorf("run seat gate for %s: %w", tripDate, err)
	}
	return result, nil
}

// canonicalHash produces a deterministic SHA-256 over a seat map.
//
// Why not hash json.Marshal output directly? Go randomises map iteration
// order, so marshalling the same map twice can produce different byte strings
// and therefore different hashes. Every poll would then look like a change,
// firing a dirty-stream write and a full refresh for data that never moved.
// Sorting the keys first removes the nondeterminism at the source.
func canonicalHash(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s:%d,", k, m[k])
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:])
}
