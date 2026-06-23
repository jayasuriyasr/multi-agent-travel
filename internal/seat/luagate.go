package seat

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/seat_gate.lua
var seatGateLuaSource string

var seatGateScript = redis.NewScript(seatGateLuaSource)

// luaGate executes the atomic Lua script that:
//  1. Compares the canonical hash of new seat data against the stored hash.
//  2. If DIFFERENT: updates seat:map, seat:hash, and XADDs to seat:dirty_stream.
//  3. ALWAYS updates seat:ts (timestamp).
//
// Returns (1, nil) when data changed and the stream was updated.
// Returns (0, nil) when data is identical and no stream write occurred.
// Returns (-1, err) on any Redis or marshal error.
//
// rdb is passed explicitly (not a package-level var) so the caller controls
// the Redis client lifetime — important for testability and the closure pattern.
func luaGate(ctx context.Context, rdb *redis.Client, tripID, date string, resp map[string]int) (int64, error) {
	seatJSON, err := json.Marshal(resp)
	if err != nil {
		return -1, fmt.Errorf("marshal seat data: %w", err)
	}

	// G6: Use canonical hash — Go map iteration order is non-deterministic.
	// Hashing raw json.Marshal output risks false-positive dirty writes because
	// map serialization order is not guaranteed across Go versions or runs.
	hash := canonicalHash(resp)
	ts := fmt.Sprintf("%.6f", float64(time.Now().UnixNano())/1e9)
	tripDate := fmt.Sprintf("%s:%s", tripID, date)

	keys := []string{
		"seat:hash:" + tripDate, // KEYS[1] — stored canonical hash
		"seat:map:" + tripDate,  // KEYS[2] — seat availability JSON
		"seat:ts:" + tripDate,   // KEYS[3] — last-updated timestamp
		"seat:dirty_stream",     // KEYS[4] — change notification stream
	}

	result, err := seatGateScript.Run(ctx, rdb, keys,
		hash,             // ARGV[1] — new canonical hash
		string(seatJSON), // ARGV[2] — new seat JSON
		ts,               // ARGV[3] — current timestamp
		tripDate,         // ARGV[4] — trip:date for stream entry
	).Int64()
	if err != nil {
		return -1, fmt.Errorf("lua gate script for %s: %w", tripDate, err)
	}

	return result, nil
}

// canonicalHash produces a deterministic SHA-256 hash of a map[string]int.
//
// G6 — WHY NOT json.Marshal DIRECTLY?
// Go's map iteration order is randomised at runtime. json.Marshal on a
// map[string]int will produce different key orderings across calls, making
// the hash non-deterministic. Two identical seat maps would hash to different
// values, causing a false-positive dirty-stream write on every single poll.
//
// The fix: extract keys, sort them with sort.Strings(), then build the
// canonical string "key1:val1,key2:val2,..." before hashing.
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
	h := sha256.Sum256(buf.Bytes())
	return fmt.Sprintf("%x", h)
}
