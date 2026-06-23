# Axentra — Go Implementation Roadmap

**Locked Stack:** Go 1.22 · Fiber v2 · hibiken/asynq · pgx/v5 · go-redis/v9

---

## Phase 1 — Infrastructure & Data Models

### Imports
```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/redis/go-redis/v9"
)
```

### 1.1 PostgreSQL Schema

```sql
CREATE TABLE stations (
    id   TEXT PRIMARY KEY, name TEXT NOT NULL, city TEXT NOT NULL,
    lat  DOUBLE PRECISION, lon  DOUBLE PRECISION
);
CREATE TABLE routes (
    route_id TEXT PRIMARY KEY, name TEXT, mode TEXT NOT NULL
);
CREATE TABLE trips (
    trip_id TEXT NOT NULL, date DATE NOT NULL, route_id TEXT REFERENCES routes(route_id),
    departure_unix BIGINT NOT NULL, PRIMARY KEY (trip_id, date)
);
CREATE TABLE stop_times (
    trip_id TEXT NOT NULL, date DATE NOT NULL, stop_seq INTEGER NOT NULL,
    station_id TEXT REFERENCES stations(id), departure_unix BIGINT NOT NULL,
    PRIMARY KEY (trip_id, date, stop_seq),
    FOREIGN KEY (trip_id, date) REFERENCES trips(trip_id, date)
);
CREATE TABLE schema_version (
    id INTEGER PRIMARY KEY DEFAULT 1, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO schema_version DEFAULT VALUES;
CREATE TABLE trip_poll_schedule (
    trip_id TEXT NOT NULL, date DATE NOT NULL, zone TEXT NOT NULL,
    poll_interval_sec INTEGER NOT NULL, PRIMARY KEY (trip_id, date)
);
```

### 1.2 Go Data Models

```go
// internal/model/types.go
package model

type TripKey struct {
    TripID string
    Date   string // YYYY-MM-DD
}

type StopTime struct {
    TripID       string
    Date         string
    StopSeq      int
    StationID    string
    DepartureUnix int64
}

type SeatSignal struct {
    ByClass    map[string]int // {"lower":3, "upper":6}
    Total      int
    Stale      bool
    SnapshotTs float64
}

type RouteEntry struct {
    RouteID  string
    StopIDs  []string
    TripKeys []TripKey
}

// Per-trip departure times for RAPTOR scan
type TripStopTimes struct {
    Key        TripKey
    Departures []int64   // indexed by stop position within route
    StationIDs []string
}
```

### 1.3 Redis Key Contract

| Key | Type | TTL |
|---|---|---|
| `seat:map:{tripID}:{date}` | String (JSON) | None |
| `seat:hash:{tripID}:{date}` | String | None |
| `seat:ts:{tripID}:{date}` | String (float) | None |
| `seat:dirty_stream` | Stream | MAXLEN ~200000 |
| `poll_lock:{tripID}:{date}` | String | = poll interval |

> [!CAUTION]
> **G1 (Redis RDB):** Never persist `poll_lock:*` keys. Configure RDB to snapshot selectively or accept that lock keys restore — add a startup flush for `poll_lock:*` on boot.

---

## Phase 2 — The Schedule Track

### Imports
```go
import (
    "context"
    "crypto/sha256"
    "encoding/json"
    "fmt"
    "sync/atomic"
    "time"
    "github.com/jackc/pgx/v5/pgxpool"
)
```

### 2.1 Ingestion Validator

```go
// internal/ingestion/validator.go
func ValidateBatch(trips []IngestTrip) error {
    seen := make(map[model.TripKey]bool, len(trips))
    for _, t := range trips {
        key := model.TripKey{TripID: t.TripID, Date: t.Date}
        if seen[key] {
            return fmt.Errorf("duplicate (trip_id,date): %v", key)
        }
        seen[key] = true
        for i := 1; i < len(t.StopTimes); i++ {
            if t.StopTimes[i].DepartureUnix <= t.StopTimes[i-1].DepartureUnix {
                return fmt.Errorf("non-monotone stops in trip %v at seq %d", key, i)
            }
        }
    }
    return nil
}
```

> [!WARNING]
> **G2:** Validate the entire batch BEFORE opening a Postgres transaction. On any error, reject the whole payload — no partial inserts.

### 2.2 Double-Buffered Route Arrays + Watermark Watcher

```go
// internal/schedule/buffer.go
type RouteBuffer struct {
    Routes       []RouteEntry
    StopTimes    [][]TripStopTimes   // indexed by route position
    TripIndex    map[TripKey]int     // route-internal index only
}

var (
    routeBuffers [2]*RouteBuffer
    routeLivePtr int32  // atomic: 0 or 1
    lastManifest string
)

func init() {
    routeBuffers[0] = &RouteBuffer{TripIndex: make(map[TripKey]int)}
    routeBuffers[1] = &RouteBuffer{TripIndex: make(map[TripKey]int)}
}

func LiveRoutes() *RouteBuffer {
    return routeBuffers[atomic.LoadInt32(&routeLivePtr)]
}
```

```go
// internal/schedule/watcher.go
func WatcherLoop(ctx context.Context, pool *pgxpool.Pool) {
    var lastTS time.Time
    for {
        select {
        case <-ctx.Done(): return
        case <-time.After(2 * time.Minute):
        }
        var ts time.Time
        pool.QueryRow(ctx, "SELECT updated_at FROM schema_version").Scan(&ts)
        if ts.After(lastTS) {
            lastTS = ts
            reloadRouteArrays(ctx, pool)
        }
    }
}

func reloadRouteArrays(ctx context.Context, pool *pgxpool.Pool) {
    rows, _ := pool.Query(ctx,
        `SELECT t.trip_id, t.date, s.stop_seq, s.station_id, s.departure_unix
         FROM trips t JOIN stop_times s USING (trip_id, date)
         ORDER BY t.trip_id ASC, t.date ASC, s.stop_seq ASC`)
    defer rows.Close()

    staging := new(RouteBuffer)
    staging.TripIndex = make(map[TripKey]int)
    // ... populate staging from rows ...

    hash := manifestHash(staging)
    if hash == lastManifest {
        return // no-op ingestion — skip swap
    }
    lastManifest = hash

    idx := 1 - atomic.LoadInt32(&routeLivePtr)
    routeBuffers[idx] = staging
    atomic.StoreInt32(&routeLivePtr, idx) // atomic swap
}
```

> [!CAUTION]
> **G3 (Go map panic):** `RouteBuffer.TripIndex` is a Go map. Reading it concurrently from RAPTOR goroutines while writing from the watcher causes a **fatal panic** (not a data race — a hard crash). The double-buffer pattern prevents this: the live buffer is read-only; the staging buffer is written to but never read by RAPTOR. **Never** write to the buffer that `LiveRoutes()` currently returns.

> [!WARNING]
> **G4 (ORDER BY):** `ORDER BY trip_id ASC, date ASC` — both columns. Missing `date ASC` causes non-deterministic row order for recurring trip_ids. This silently corrupts `TripIndex` across restarts.

---

## Phase 3 — The Live Seat Track

### Imports
```go
import (
    "context"
    "crypto/sha256"
    "encoding/json"
    "fmt"
    "time"
    "github.com/hibiken/asynq"
    "github.com/redis/go-redis/v9"
)
```

### 3.1 Zone Classifier (self-rescheduling goroutine)

```go
// internal/seat/zone.go
type Zone struct {
    Name     string
    MaxHours float64
    Interval time.Duration
}

var Zones = []Zone{
    {"RED", 12, 5 * time.Minute},
    {"YELLOW", 48, 30 * time.Minute},
    {"GREEN", 168, 4 * time.Hour},
    {"COLD", 1<<31, 24 * time.Hour},
}

func ClassifyZone(departureUnix int64) Zone {
    hours := float64(departureUnix-time.Now().Unix()) / 3600
    for _, z := range Zones {
        if hours < z.MaxHours { return z }
    }
    return Zones[len(Zones)-1]
}

func ZoneClassifyLoop(ctx context.Context, client *asynq.Client, pool *pgxpool.Pool) {
    for {
        select {
        case <-ctx.Done(): return
        case <-time.After(5 * time.Minute):
        }
        rows, _ := pool.Query(ctx,
            "SELECT trip_id, date, departure_unix FROM trips")
        for rows.Next() {
            var tripID, date string; var depUnix int64
            rows.Scan(&tripID, &date, &depUnix)
            zone := ClassifyZone(depUnix)
            payload, _ := json.Marshal(map[string]string{
                "trip_id": tripID, "date": date,
            })
            task := asynq.NewTask("seat:poll", payload)
            client.Enqueue(task,
                asynq.ProcessIn(zone.Interval),
                asynq.TaskID(fmt.Sprintf("poll:%s:%s", tripID, date)),
                asynq.Retention(zone.Interval),
            )
        }
        rows.Close()
    }
}
```

### 3.2 Poll Worker with Distributed Lock

```go
// internal/seat/poller.go
func HandlePollTask(ctx context.Context, t *asynq.Task) error {
    var p struct{ TripID, Date string }
    json.Unmarshal(t.Payload(), &p)

    zone := ClassifyZone(getTripDeparture(p.TripID, p.Date))
    lockKey := fmt.Sprintf("poll_lock:%s:%s", p.TripID, p.Date)

    ok, _ := rdb.SetNX(ctx, lockKey, "1", zone.Interval).Result()
    if !ok { return nil } // another worker holds lock — skip silently

    response, err := provider.FetchSeats(ctx, p.TripID, p.Date)
    if err != nil { return fmt.Errorf("provider error: %w", err) }

    if err := validateSchema(response); err != nil {
        return fmt.Errorf("schema mismatch: %w", err)
    }
    return luaGate(ctx, p.TripID, p.Date, response)
}
```

> [!CAUTION]
> **G5:** Never `DEL` the lock key in a defer. Let TTL expire naturally. Explicit delete reopens the concurrent-poll race window.

### 3.3 Atomic Lua Gate → Redis Stream

```go
// internal/seat/luagate.go
var seatGateScript = redis.NewScript(`
local cur_hash = redis.call('GET', KEYS[1])
if cur_hash ~= ARGV[1] then
  redis.call('SET', KEYS[2], ARGV[2])
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', '200000', '*',
             'trip', ARGV[4], 'changed_at', ARGV[3])
end
redis.call('SET', KEYS[3], ARGV[3])
return cur_hash ~= ARGV[1] and 1 or 0
`)

func luaGate(ctx context.Context, tripID, date string, resp map[string]int) error {
    seatJSON, _ := json.Marshal(resp)
    canonical, _ := json.Marshal(resp) // keys sorted by Go map iteration — see gotcha
    hash := fmt.Sprintf("%x", sha256.Sum256(canonical))
    ts := fmt.Sprintf("%f", float64(time.Now().UnixNano())/1e9)
    tripDate := fmt.Sprintf("%s:%s", tripID, date)

    keys := []string{
        "seat:hash:" + tripDate, "seat:map:" + tripDate,
        "seat:ts:" + tripDate,   "seat:dirty_stream",
    }
    return seatGateScript.Run(ctx, rdb, keys,
        hash, string(seatJSON), ts, tripDate).Err()
}
```

> [!CAUTION]
> **G6 (Canonical hash — Go-specific):** Go's `json.Marshal` does NOT sort map keys by default for `map[string]int`. It does for `map[string]any` but behavior is version-dependent. **Always** use a struct with defined field order, or sort keys explicitly before hashing. A non-deterministic hash causes false-positive dirty stream entries on every poll.

```go
// Safe canonical hash
func canonicalHash(m map[string]int) string {
    keys := make([]string, 0, len(m))
    for k := range m { keys = append(keys, k) }
    sort.Strings(keys)
    var buf bytes.Buffer
    for _, k := range keys { fmt.Fprintf(&buf, "%s:%d,", k, m[k]) }
    h := sha256.Sum256(buf.Bytes())
    return fmt.Sprintf("%x", h)
}
```

---

## Phase 4 — The In-Memory Bridge

### Imports
```go
import (
    "context"
    "encoding/json"
    "sync/atomic"
    "time"
    "github.com/redis/go-redis/v9"
)
```

### 4.1 SEAT_SIGNAL — Dict-Based (V2 Fix 3)

```go
// internal/state/signal.go
type SignalBuffer = map[model.TripKey]model.SeatSignal

var (
    signalBuffers [2]atomic.Pointer[SignalBuffer]
    READY         int32 // atomic bool: 0=false, 1=true
)

func init() {
    a := make(SignalBuffer)
    b := make(SignalBuffer)
    signalBuffers[0].Store(&a)
    signalBuffers[1].Store(&b)
}

// signalLivePtr: 0 or 1
var signalLivePtr int32

func LiveSignal() *SignalBuffer {
    return signalBuffers[atomic.LoadInt32(&signalLivePtr)].Load()
}
```

> [!CAUTION]
> **G7 (THE MOST DANGEROUS Go GOTCHA):** Go maps are NOT safe for concurrent read+write. If the refresh loop writes to the staging map while a RAPTOR goroutine somehow reads it, **Go will panic and crash the process** — not corrupt data silently, but hard-crash. The double-buffer pattern prevents this: RAPTOR only reads from `LiveSignal()`, the refresher only writes to the other buffer. But you must **never** expose the staging pointer. The `atomic.Pointer[SignalBuffer]` pattern ensures the swap is a single pointer store — goroutines reading the old pointer continue safely because the old map is never mutated after swap.

### 4.2 Cold Start

```go
// internal/seat/refresher.go
func ColdStart(ctx context.Context, rdb *redis.Client) error {
    // 1. Capture stream cursor BEFORE snapshot
    entries, _ := rdb.XRevRangeN(ctx, "seat:dirty_stream", "+", "-", 1).Result()
    lastStreamID := "0"
    if len(entries) > 0 { lastStreamID = entries[0].ID }

    // 2. Full snapshot for every known trip
    routes := schedule.LiveRoutes()
    staging := make(SignalBuffer, len(routes.TripIndex))
    for key := range routes.TripIndex {
        sig, err := fetchSignalFromRedis(ctx, rdb, key)
        if err != nil { continue }
        staging[key] = sig
    }

    // 3. Swap + READY
    idx := 1 - atomic.LoadInt32(&signalLivePtr)
    signalBuffers[idx].Store(&staging)
    atomic.StoreInt32(&signalLivePtr, idx)
    atomic.StoreInt32(&READY, 1)
    return nil
}
```

> [!WARNING]
> **G8 (Boot order):** `ColdStart` MUST run after `reloadRouteArrays` completes. `routes.TripIndex` is empty until the first route load finishes. Starting cold-start first produces an empty SEAT_SIGNAL and sets READY=true — the exact bug the Ready Gate was designed to prevent.

### 4.3 Stream Reader Loop

```go
func RefreshLoop(ctx context.Context, rdb *redis.Client) {
    for {
        result, err := rdb.XRead(ctx, &redis.XReadArgs{
            Streams: []string{"seat:dirty_stream", lastStreamID},
            Count:   500,
            Block:   2 * time.Second,
        }).Result()
        if err == redis.Nil || err != nil { continue }

        // Clone the current live buffer into staging
        live := *LiveSignal()
        staging := make(SignalBuffer, len(live))
        for k, v := range live { staging[k] = v }

        for _, msg := range result[0].Messages {
            tripDate := msg.Values["trip"].(string)
            parts := strings.SplitN(tripDate, ":", 2)
            key := model.TripKey{TripID: parts[0], Date: parts[1]}
            sig, err := fetchSignalFromRedis(ctx, rdb, key)
            if err != nil { continue }
            staging[key] = sig
            lastStreamID = msg.ID
        }

        // Atomic swap — new map, never mutate the live one
        idx := 1 - atomic.LoadInt32(&signalLivePtr)
        signalBuffers[idx].Store(&staging)
        atomic.StoreInt32(&signalLivePtr, idx)
    }
}

func fetchSignalFromRedis(ctx context.Context, rdb *redis.Client, key model.TripKey) (model.SeatSignal, error) {
    pipe := rdb.Pipeline()
    mapCmd := pipe.Get(ctx, fmt.Sprintf("seat:map:%s:%s", key.TripID, key.Date))
    tsCmd := pipe.Get(ctx, fmt.Sprintf("seat:ts:%s:%s", key.TripID, key.Date))
    pipe.Exec(ctx)

    var byClass map[string]int
    json.Unmarshal([]byte(mapCmd.Val()), &byClass)
    ts, _ := strconv.ParseFloat(tsCmd.Val(), 64)

    zone := ClassifyZone(getTripDeparture(key.TripID, key.Date))
    stale := (float64(time.Now().Unix()) - ts) > 2*zone.Interval.Seconds()

    total := 0
    for _, v := range byClass { total += v }
    return model.SeatSignal{ByClass: byClass, Total: total, Stale: stale, SnapshotTs: float64(time.Now().UnixNano()) / 1e9}, nil
}
```

> [!IMPORTANT]
> **G9 (Clone, don't mutate):** The refresh loop creates a **new** map by copying the live map, then applies deltas to the copy, then swaps. Never write into the live map — concurrent RAPTOR goroutines are reading it. Go will panic.

> [!WARNING]
> **G10 (Stream lag):** If `lastStreamID` predates the oldest entry in the stream (pod was down longer than the MAXLEN window), `XREAD` silently skips trimmed entries. Add a lag check: compare `lastStreamID` against `XRANGE seat:dirty_stream - + COUNT 1`. If lagging, re-run `ColdStart`.

### 4.4 Ready Gate (Fiber middleware)

```go
// internal/api/middleware.go
func ReadyGate(c *fiber.Ctx) error {
    if atomic.LoadInt32(&state.READY) == 0 {
        return c.Status(503).JSON(fiber.Map{
            "error": "warming_up", "retry_after_seconds": 5,
        })
    }
    return c.Next()
}

// Health endpoint
app.Get("/healthz/ready", func(c *fiber.Ctx) error {
    if atomic.LoadInt32(&state.READY) == 1 {
        return c.SendStatus(200)
    }
    return c.SendStatus(503)
})
```

---

## Phase 5 — The Search Layer

### Imports
```go
import (
    "encoding/json"
    "sort"
    "sync/atomic"
    "time"
    "github.com/gofiber/fiber/v2"
    "github.com/redis/go-redis/v9"
)
```

### 5.1 Seat-Aware RAPTOR — `canBoard`

```go
// internal/raptor/engine.go
func canBoard(buf *SignalBuffer, key model.TripKey, class string, count int) bool {
    sig, ok := (*buf)[key]
    if !ok { return false }
    return !sig.Stale && sig.ByClass[class] >= count
}

func RaptorSearch(params SearchParams, topK int) []Path {
    // CAPTURE ONCE — never re-read live pointers inside the loop
    buf := state.LiveSignal()
    routes := schedule.LiveRoutes()

    // ... RAPTOR round-based traversal using routes.StopTimes ...
    // For each boarding decision:
    //   if canBoard(buf, tripKey, params.SeatClass, params.Passengers) { ... }

    // Return up to topK Pareto-optimal paths
    return paretoFrontier[:min(topK, len(paretoFrontier))]
}
```

> [!CAUTION]
> **G11 (THE snapshot rule):** `buf` and `routes` are captured on line 1-2 of `RaptorSearch`. Every `canBoard` call and every route traversal uses these captured references. If you read `state.LiveSignal()` inside the inner loop, a concurrent swap mid-search mixes two snapshot generations — some boarding decisions use old data, others use new. This is an intermittent, impossible-to-reproduce bug.

### 5.2 MGET Validation + Truncation (Fix A + Fix B)

```go
// internal/raptor/validator.go
func ValidateAndTruncate(ctx context.Context, rdb *redis.Client,
    candidates []Path, class string, count int, maxResults int) []Path {

    // Fix A: collect ALL unique seat keys across ALL paths
    keySet := make(map[string]struct{})
    for _, p := range candidates {
        for _, leg := range p.Legs {
            keySet[fmt.Sprintf("seat:map:%s:%s", leg.TripID, leg.Date)] = struct{}{}
        }
    }
    keys := make([]string, 0, len(keySet))
    for k := range keySet { keys = append(keys, k) }

    // Single MGET — 1 network round trip
    vals, _ := rdb.MGet(ctx, keys...).Result()
    cache := make(map[string]map[string]int, len(keys))
    for i, k := range keys {
        var m map[string]int
        if vals[i] != nil {
            json.Unmarshal([]byte(vals[i].(string)), &m)
        }
        cache[k] = m
    }

    // Fix B: filter survivors, truncate — ZERO recursion
    valid := make([]Path, 0, maxResults)
    for _, p := range candidates {
        ok := true
        for _, leg := range p.Legs {
            k := fmt.Sprintf("seat:map:%s:%s", leg.TripID, leg.Date)
            if cache[k][class] < count {
                ok = false; break
            }
        }
        if ok {
            valid = append(valid, p)
            if len(valid) >= maxResults { break }
        }
    }
    return valid
}
```

### 5.3 HTTP Search Endpoint

```go
// internal/api/search.go
app.Get("/search", ReadyGate, func(c *fiber.Ctx) error {
    params := SearchParams{
        Origin:     c.Query("origin"),
        Dest:       c.Query("destination"),
        Date:       c.Query("date"),
        SeatClass:  c.Query("seat_class", "lower"),
        Passengers: c.QueryInt("passengers", 1),
    }

    t0 := time.Now()

    // Over-fetch: RAPTOR computes top-100 Pareto paths in RAM
    candidates := raptor.RaptorSearch(params, 100)

    // Single MGET validates all legs, truncate to top-5
    final := raptor.ValidateAndTruncate(c.Context(), rdb,
        candidates, params.SeatClass, params.Passengers, 5)

    durationMs := float64(time.Since(t0).Microseconds()) / 1000.0

    return c.JSON(fiber.Map{
        "paths":            final,
        "query_duration_ms": durationMs,
        "validated_at":     time.Now().UTC().Format(time.RFC3339Nano),
    })
})
```

---

## Startup Sequence (main.go)

```go
func main() {
    pool := connectPostgres()
    rdb := connectRedis()

    // Phase 2: Load route arrays FIRST
    schedule.ReloadRouteArrays(ctx, pool)
    go schedule.WatcherLoop(ctx, pool)

    // Phase 4: Cold-start SEAT_SIGNAL AFTER routes are loaded
    state.ColdStart(ctx, rdb)
    go state.RefreshLoop(ctx, rdb)
    // READY is now true

    // Phase 3: Start background workers
    asynqSrv := asynq.NewServer(redisOpt, asynq.Config{Concurrency: 20})
    mux := asynq.NewServeMux()
    mux.HandleFunc("seat:poll", seat.HandlePollTask)
    go asynqSrv.Run(mux)
    go seat.ZoneClassifyLoop(ctx, asynqClient, pool)

    // Phase 5: HTTP API
    app := fiber.New()
    app.Use(middleware.ReadyGate)
    api.RegisterRoutes(app, rdb)
    app.Listen(":8080")
}
```

---

## Go-Specific Gotcha Summary

| # | Gotcha | Severity | Phase |
|---|---|---|---|
| G3 | Go map concurrent read+write = **fatal panic** (not data race) | 🔴 Critical | 2,4 |
| G7 | Same as G3 but for SEAT_SIGNAL — clone-then-swap, never mutate live | 🔴 Critical | 4 |
| G9 | RefreshLoop must create new map, copy, apply deltas, then swap | 🔴 Critical | 4 |
| G11 | Capture `LiveSignal()` + `LiveRoutes()` once at search start | 🔴 Critical | 5 |
| G6 | Go map iteration order is random — sort keys before hashing | 🟠 High | 3 |
| G4 | `ORDER BY trip_id ASC, date ASC` — both columns always | 🟠 High | 2 |
| G5 | Never DEL the poll lock — let TTL expire | 🟠 High | 3 |
| G8 | ColdStart must run AFTER route arrays are loaded | 🟠 High | 4 |
| G10 | Stream MAXLEN trim can silently skip entries for lagging pods | 🟡 Medium | 4 |
| G1 | Don't persist poll_lock keys in RDB snapshots | 🟡 Medium | 1 |

> [!TIP]
> **Run `go test -race ./...` on every CI build.** Go's race detector catches G3/G7/G9/G11 automatically during tests. It is your single most powerful safety net as a solo developer.
