package raptor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"axentra/internal/breaker"
	"axentra/internal/model"

	"github.com/redis/go-redis/v9"
)

// DefaultMGetChunk bounds how many keys go into a single MGET when the caller
// does not say. Redis handles large multi-key commands, but an unbounded one
// blocks the server for the whole batch and risks the protocol's argument
// limits on big result sets.
const DefaultMGetChunk = 512

// ValidatePath checks that a journey is internally consistent before it is
// shown to anyone: legs must join up end to end, times must move forwards, and
// the journey must actually start where the passenger asked to start.
//
// This is a cheap assertion over a structure that several subtle bugs (round
// mixing, journal cycles, backtracking off by one round) corrupt in ways that
// are otherwise invisible in the JSON response.
func ValidatePath(p model.Path, params model.SearchParams) error {
	if len(p.Legs) == 0 {
		return fmt.Errorf("path has no legs")
	}
	if p.Legs[0].BoardStation != params.Origin {
		return fmt.Errorf("path starts at %q, expected origin %q", p.Legs[0].BoardStation, params.Origin)
	}
	last := p.Legs[len(p.Legs)-1]
	if last.AlightStation != params.Destination {
		return fmt.Errorf("path ends at %q, expected destination %q", last.AlightStation, params.Destination)
	}
	if p.Legs[0].DepartureUnix < params.DepTime {
		return fmt.Errorf("path departs at %d, before requested %d", p.Legs[0].DepartureUnix, params.DepTime)
	}
	for i, leg := range p.Legs {
		if leg.ArrivalUnix < leg.DepartureUnix {
			return fmt.Errorf("leg %d arrives (%d) before it departs (%d)", i, leg.ArrivalUnix, leg.DepartureUnix)
		}
		if leg.Kind == model.LegTransit && leg.TripID == "" {
			return fmt.Errorf("leg %d is transit but has no trip id", i)
		}
		if i == 0 {
			continue
		}
		prev := p.Legs[i-1]
		if prev.AlightStation != leg.BoardStation {
			return fmt.Errorf("gap between leg %d (ends %s) and leg %d (starts %s)",
				i-1, prev.AlightStation, i, leg.BoardStation)
		}
		if leg.DepartureUnix < prev.ArrivalUnix {
			return fmt.Errorf("leg %d departs (%d) before leg %d arrives (%d)",
				i, leg.DepartureUnix, i-1, prev.ArrivalUnix)
		}
	}
	return nil
}

// ValidatorConfig tunes seat validation.
type ValidatorConfig struct {
	// Strict decides what a Redis failure means:
	//   true  — reject every candidate. Correct for booking, where offering a
	//           route implies the seat exists.
	//   false — return candidates unvalidated. Correct for read-only search,
	//           where a transient hiccup degrading to "possibly stale" beats
	//           degrading to "no service".
	Strict bool
	// MGetChunk bounds keys per MGET. Zero means DefaultMGetChunk.
	MGetChunk int
	// MaxResults truncates the survivors. Zero means no truncation.
	MaxResults int
	// BreakerThreshold is the consecutive-failure count that trips the breaker.
	BreakerThreshold int
	// BreakerCooldown is how long the breaker stays open before probing.
	BreakerCooldown time.Duration
}

// Validator is the authoritative seat gate — layer 2 of the two-layer design
// described on canBoard in engine.go.
//
// It owns a circuit breaker because it is the only component in a search that
// touches Redis. Without one, a degraded Redis receives every search's
// validation MGET, each waiting out its timeout before failing, which turns a
// slow dependency into an unavailable one and holds it there. The breaker
// makes that failure fast and cheap, and — because the answer under failure is
// already defined by Strict — it changes nothing about what callers get back,
// only how long they wait and how hard Redis gets hit while recovering.
type Validator struct {
	rdb *redis.Client
	cfg ValidatorConfig
	cb  *breaker.Breaker
}

// NewValidator builds a Validator. A nil client is permitted: validation is
// then impossible, and Strict decides the outcome.
func NewValidator(rdb *redis.Client, cfg ValidatorConfig) *Validator {
	if cfg.MGetChunk <= 0 {
		cfg.MGetChunk = DefaultMGetChunk
	}
	return &Validator{
		rdb: rdb,
		cfg: cfg,
		cb:  breaker.New(cfg.BreakerThreshold, cfg.BreakerCooldown),
	}
}

// BreakerState reports the circuit breaker's disposition.
func (v *Validator) BreakerState() breaker.State { return v.cb.State() }

// Status is the single string to show a human: the breaker state, or
// "unconfigured" when there is no client to break in the first place.
// Reporting a healthy-looking "closed" for a validator that can never validate
// would be worse than saying nothing.
func (v *Validator) Status() string {
	if v.rdb == nil {
		return "unconfigured"
	}
	return string(v.cb.State())
}

// Healthy reports whether seat validation can currently be performed at all —
// both that a client is configured and that the breaker is letting calls
// through. Health output needs the combined answer, because from a caller's
// point of view "no Redis configured" and "Redis unreachable" fail the same
// way.
func (v *Validator) Healthy() bool { return v.rdb != nil && v.cb.Healthy() }

// Validate filters candidate journeys against live seat availability and
// truncates the survivors to MaxResults.
//
// All seat keys across all candidates are collected once and fetched in
// chunked MGETs — one round trip per chunk, not one per path or per leg.
func (v *Validator) Validate(ctx context.Context, candidates []model.Path, class string, count int) []model.Path {
	if len(candidates) == 0 {
		return nil
	}

	// Collect the distinct seat keys referenced by any leg of any candidate.
	keySet := make(map[string]struct{})
	for _, p := range candidates {
		for _, leg := range p.Legs {
			if leg.Kind == model.LegWalk || leg.RouteID == model.WalkRouteID {
				continue
			}
			keySet[model.SeatMapKey(leg.TripID, leg.Date)] = struct{}{}
		}
	}
	if len(keySet) == 0 {
		// Every candidate is walk-only; there is nothing to reserve.
		return v.truncate(candidates)
	}

	if v.rdb == nil {
		return v.unavailable("no redis client configured", candidates)
	}
	if !v.cb.Allow() {
		// Fail fast rather than queueing behind a dependency we already know
		// is unhealthy.
		return v.unavailable("seat validation circuit breaker is open", candidates)
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	cache := make(map[string]map[string]int, len(keys))
	for start := 0; start < len(keys); start += v.cfg.MGetChunk {
		end := start + v.cfg.MGetChunk
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]

		vals, err := v.rdb.MGet(ctx, chunk...).Result()
		if err != nil {
			v.cb.Failure()
			return v.unavailable(err.Error(), candidates)
		}
		for i, k := range chunk {
			var m map[string]int
			if i < len(vals) && vals[i] != nil {
				if str, ok := vals[i].(string); ok {
					if err := json.Unmarshal([]byte(str), &m); err != nil {
						slog.Warn("unparseable seat map in redis", "key", k, "error", err)
						m = nil
					}
				}
			}
			cache[k] = m
		}
	}
	v.cb.Success()

	valid := make([]model.Path, 0, len(candidates))
	for _, p := range candidates {
		ok := true
		for _, leg := range p.Legs {
			if leg.Kind == model.LegWalk || leg.RouteID == model.WalkRouteID {
				continue // walking needs no seat
			}
			// A nil map means Redis has no record (key absent or evicted).
			// Seats are unconfirmed, so the path does not survive.
			seatMap := cache[model.SeatMapKey(leg.TripID, leg.Date)]
			if seatMap == nil || seatMap[class] < count {
				ok = false
				break
			}
		}
		if ok {
			valid = append(valid, p)
			if v.cfg.MaxResults > 0 && len(valid) >= v.cfg.MaxResults {
				break
			}
		}
	}
	return valid
}

// unavailable applies the Strict policy when validation cannot be performed.
func (v *Validator) unavailable(reason string, candidates []model.Path) []model.Path {
	if v.cfg.Strict {
		slog.Error("seat validation unavailable, rejecting all candidates",
			"strict", true, "candidates", len(candidates), "reason", reason,
			"breaker", v.cb.State())
		return nil
	}
	slog.Warn("seat validation unavailable, returning unvalidated candidates",
		"strict", false, "candidates", len(candidates), "reason", reason,
		"breaker", v.cb.State())
	return v.truncate(candidates)
}

func (v *Validator) truncate(paths []model.Path) []model.Path {
	if v.cfg.MaxResults > 0 && len(paths) > v.cfg.MaxResults {
		return paths[:v.cfg.MaxResults]
	}
	return paths
}
