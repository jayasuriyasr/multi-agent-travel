package raptor

import (
	"context"
	"encoding/json"
	"fmt"

	"axentra/internal/model"

	"github.com/redis/go-redis/v9"
)

// ValidateAndTruncate performs post-search MGET validation on all candidate
// paths, filtering out any path where a leg no longer has sufficient seats,
// then truncates to maxResults.
//
// Fix A: Collect ALL unique seat keys across ALL paths into a single MGET.
// Fix B: Filter survivors, truncate — ZERO recursion.
func ValidateAndTruncate(ctx context.Context, rdb *redis.Client,
	candidates []model.Path, class string, count int, maxResults int) []model.Path {

	if len(candidates) == 0 {
		return nil
	}

	// Fix A: Deduplicate seat keys across all paths
	keySet := make(map[string]struct{})
	for _, p := range candidates {
		for _, leg := range p.Legs {
			keySet[fmt.Sprintf("seat:map:%s:%s", leg.TripID, leg.Date)] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	// Single MGET — 1 network round trip for all seat data
	vals, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		// On Redis error, return candidates unvalidated rather than failing
		if len(candidates) > maxResults {
			return candidates[:maxResults]
		}
		return candidates
	}

	// Build lookup cache: redis key → seat map
	cache := make(map[string]map[string]int, len(keys))
	for i, k := range keys {
		var m map[string]int
		if vals[i] != nil {
			if str, ok := vals[i].(string); ok {
				json.Unmarshal([]byte(str), &m)
			}
		}
		cache[k] = m
	}

	// Fix B: Filter survivors, truncate — ZERO recursion
	valid := make([]model.Path, 0, maxResults)
	for _, p := range candidates {
		allLegsOK := true
		for _, leg := range p.Legs {
			k := fmt.Sprintf("seat:map:%s:%s", leg.TripID, leg.Date)
			seatMap := cache[k]
			if seatMap == nil || seatMap[class] < count {
				allLegsOK = false
				break
			}
		}
		if allLegsOK {
			valid = append(valid, p)
			if len(valid) >= maxResults {
				break
			}
		}
	}

	return valid
}
