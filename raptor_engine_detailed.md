# Complete Line-by-Line Explanation of `engine.go`

This document provides an exhaustive, line-by-line explanation of the entire `engine.go` file, followed by a step-by-step dry run of the algorithm.

---

## Part 1: Package, Imports, and Constants (Lines 1-20)

**Lines 1-4:** `package raptor`
Defines this file as part of the `raptor` package. The comments explain that RAPTOR stands for Round-bAsed Public Transit Optimized Router, which finds optimal paths balancing arrival time and the number of transfers.

**Lines 6-14:** `import (...)`
Imports standard Go libraries (`log` for logging, `math` for integer constants, `sort` for sorting the final paths) and internal project modules (`model` for data structures, `schedule` for the routing data, `state` for live seat availability).

**Lines 16-20:** `const (...)`
- `MaxRounds = 4`: RAPTOR works in "rounds" (where a round equals a transfer). Setting this to 4 means the engine will calculate paths with up to 3 transfers.
- `infinity = int64(math.MaxInt64)`: Represents an unreachable time (the highest possible 64-bit integer).

---

## Part 2: Seat Availability Logic (Lines 22-54)

**Lines 22-40:** `// canBoard checks seat availability...`
A detailed comment explaining the "Two-Layer Design" for seat checking. Layer 1 (this file) is optimistic in RAM. Layer 2 (Validator) is pessimistic in Redis.

**Line 41:** `func canBoard(buf *state.SignalBuffer, key model.TripKey, class string, count int) bool {`
Function signature. It takes the snapshot of seat data (`buf`), the specific trip ID/Date (`key`), the requested seat `class`, and the passenger `count`.

**Line 42:** `sig, ok := (*buf)[key]`
Looks up the seat availability for this specific trip in the live memory snapshot.

**Lines 43-47:** `if !ok { return true }`
If the trip isn't in memory at all, we *optimistically* return `true` (allow boarding). This prevents the engine from returning 0 results just because the background data loader hasn't finished booting up yet.

**Lines 48-52:** `if sig.Stale { return true }`
If the data is marked as stale (old), we again return `true`. The final Redis MGET step will strictly verify this later.

**Line 53:** `return sig.ByClass[class] >= count`
If we have fresh data, we check if the available seats in the requested class are greater than or equal to the number of passengers requested. Returns `true` if seats are available, `false` if the train is full.

---

## Part 3: Journal Data Structure (Lines 56-65)

**Lines 56-65:** `type journalEntry struct { ... }`
The journal is how RAPTOR remembers how it reached a station so it can build the itinerary at the end.
- `round`: The transfer number (1, 2, 3, etc.).
- `station`: The station we arrived at.
- `tripKey`: The specific train (ID and Date) we rode.
- `routeID`: The route line.
- `boardStop`: Where we got on the train.
- `boardDepUnix`: What time we got on the train.
- `arrivalUnix`: What time we got off the train.

---

## Part 4: The Main Algorithm — `RaptorSearch` (Lines 67-277)

**Line 73:** `func RaptorSearch(params model.SearchParams, topK int) []model.Path {`
The main function. Takes search parameters (Origin, Dest, Date, Time, Passengers) and returns a list of paths.

### 4A. Initialization
**Lines 74-76:** `buf := state.LiveSignal(); routes := schedule.LiveRoutes()`
**CRITICAL:** This captures pointers to the active memory data exactly ONCE. This guarantees that if the background updaters change the timetable mid-search, this specific search won't crash or get corrupted data.

**Lines 78-81:** `if routes == nil ... return nil`
Safety check. If the timetable hasn't loaded yet, abort the search.

**Lines 83-85:** `log.Printf(...)`
Logs the details of the incoming search request for debugging.

**Lines 87-91:** `tau := make([]map[string]int64, MaxRounds+1)`
Allocates the main time table. `tau[round][station]` stores the earliest known arrival time at a station for a specific round. Loops from 0 to 4 to initialize the inner maps.

**Line 93-94:** `bestArrival := make(map[string]int64)`
Tracks the absolute earliest arrival time at any station across *all* rounds. This allows the engine to instantly skip checking trains that arrive later than a previously found path.

**Lines 96-100:** `journal := make([]map[string]journalEntry, MaxRounds+1)`
Allocates the memory for the journal breadcrumbs, one map per round.

**Lines 102-104:** `tau[0][params.Origin] = params.DepTime; bestArrival[params.Origin] = params.DepTime`
Initializes round 0 (zero trips taken). You are at the `Origin` station at the `DepTime` you requested.

**Line 106-107:** `marked := map[string]bool{params.Origin: true}`
RAPTOR is optimized by only scanning routes from stations that were "improved" in the last round. We mark the Origin to kick off Round 1.

### 4B. The Round Loop and Route Accumulation
**Line 109:** `for round := 1; round <= MaxRounds; round++ {`
Begins the main search loop. Round 1 checks direct trains. Round 2 checks 1-transfer trains, etc.

**Lines 110-115:** `type routeQueue struct... queue := make(map[int]int)`
Creates a queue to store which routes we need to scan in this round, and what stop position to start scanning them from.

**Lines 117-123:** `for station := range marked { ... }`
Loops over every station that was marked. Looks up all routes that pass through that station. If a route passes through multiple marked stations, it only keeps the *earliest* boarding position (`rs.StopPos < existing`) to avoid doing redundant work.

**Line 125-126:** `newMarked := make(map[string]bool)`
Prepares an empty list to record the stations we improve *during* this round.

### 4C. The Ride-Forward Scan
**Line 133:** `for routeIdx, boardPos := range queue {`
Starts scanning the routes we collected.

**Lines 134-142:** Fetches the actual route and timetable data. If the data is empty, it skips it.

**Lines 144-148:** `var currentTrip *model.TripStopTimes...`
These variables represent the passenger. `currentTrip` is the train the passenger is currently sitting on. It starts as `nil` because they are standing on the platform.

**Line 150:** `for pos := boardPos; pos < len(route.StopIDs); pos++ {`
Travels down the stops of the route, starting from where we got on (`boardPos`).

**Lines 153-172: Step A (Alighting)**
If we are on a train (`currentTrip != nil`), we check our arrival time at this stop (`arrivalAtStop`). 
We compare it to `bestArrival[station]`. If it is strictly earlier (`arrivalAtStop < currentBest`), we have found a better way to reach this station!
- We update `bestArrival` and `tau`.
- We add this station to `newMarked`.
- We write a `journal` entry recording exactly how we got here.

**Lines 174-186: Step B (Boarding/Switching Preparation)**
We check what the absolute best known arrival time at this station is (`arrivalHere, arrived := bestArrival[station]`). If we haven't reached this station yet, we `continue` to the next stop. 
If we *are* on a train, we record its departure time at this stop as `currentDep`. Any new train we consider boarding must leave *earlier* than `currentDep` to be considered an improvement.

**Line 196:** `for ti := range routeTrips {`
Scans all scheduled trips (train runs) for this route.

**Lines 201-204:** `if dep < arrivalHere { continue }`
If the train departs *before* we arrive at the station, we physically cannot catch it. Skip.

**Lines 205-207:** `if dep >= currentDep { continue }`
If the train departs *after* the train we are already sitting on, it is slower. Skip.

**Lines 208-210:** `if !canBoard(...) { continue }`
Calls the seat availability checker. If the train is full, we cannot board it. Skip.

**Lines 211-215:** `currentTrip = tst; boardStation = station; ...`
If the trip passes all checks, we board it! The passenger is now on this train. We tighten `currentDep` so we only switch again if we find an even faster train down the line.

### 4D. Footpath Relaxation (Walking Transfers)
**Lines 220-230:** `transitImproved := make([]string, 0, len(newMarked))`
After all trains are scanned for the round, we must process walking transfers. We take a snapshot of `newMarked` (stations improved by trains).

**Lines 231-249:** `for _, station := range transitImproved { ... }`
For every station we arrived at via train, we look at its walkable neighbors (`routes.Footpaths`). 
We add the walking time (`WalkSeconds`). If walking to the neighbor gets us there faster than any previously known method, we:
- Update `bestArrival` and `tau`.
- Mark the neighbor in `newMarked` (so the *next* round will check trains departing from there).
- Record a `"WALK"` entry in the journal.

**Lines 251-254:** `marked = newMarked; if len(marked) == 0 { break }`
We set the `marked` list for the next round. If we didn't improve any stations, we stop searching early to save CPU.

### 4E. Final Sorting
**Lines 257-260:** `if _, ok := bestArrival[params.Destination]; !ok { return nil }`
If the destination's arrival time is still empty, no path exists. Return `nil`.

**Line 263:** `paths := reconstructPaths(journal, params)`
Calls the helper function to build the actual itineraries.

**Lines 266-271:** `sort.Slice(...)`
Sorts all found paths. First by arrival time (fastest first), then by the number of transfers (fewest transfers first).

**Lines 273-276:** `if len(paths) > topK...`
Truncates the results to the requested limit (e.g., top 5) and returns them.

---

## Part 5: Path Reconstruction (Lines 279-365)

**Line 281:** `func reconstructPaths(...) []model.Path {`
Takes the raw journal and turns it into human-readable legs.

**Line 284:** `for r := 1; r <= MaxRounds; r++ {`
Loops through the rounds. This ensures we return Pareto-optimal paths (e.g., a fast path with 2 transfers, and a slightly slower path with 0 transfers).

**Lines 285-288:** `entry, ok := journal[r][params.Destination]`
Did we reach the destination in exactly `r` rounds? If not, skip.

**Lines 290-297:** Setup for backtracking. We start at the `Destination`. We create a `visited` map to prevent infinite loops if the data is corrupted.

**Line 298:** `for currRound > 0 && current != params.Origin {`
We loop backward until we hit the `Origin` or run out of rounds.

**Lines 299-302:** `if visited[current] { break }; visited[current] = true`
If we loop back to a station we already processed, the data has a circle. We break and discard the path to prevent the server from freezing.

**Lines 303-318:** The Fallback Scan. 
If we don't find a journal entry for the station in the exact `currRound`, it means we waited at the station across round boundaries. We loop backward (`pr--`) through older rounds to find the exact round we arrived at this station, and update `currRound`.

**Lines 320-328:** `leg := model.Leg{ ... }`
We extract the data from the journal entry and create a formal `Leg` object.

**Lines 330-339:** `if len(legs) > 0 && legs[0].BoardStation != leg.AlightStation {`
Leg-continuity guard. Ensures the station you get off at matches the station you board the next train at. If not, the path is broken and is discarded.

**Line 341-342:** `legs = append([]model.Leg{leg}, legs...); current = e.boardStop`
Prepends the leg to our itinerary (since we are building it backwards). Moves our current pointer to where we boarded the train.

**Lines 343-350:** `if _, foundHere := journal[currRound][e.boardStop]; !foundHere { currRound-- }`
**CRITICAL BACKTRACK LOGIC:** In RAPTOR, a single round can contain a Transit leg AND a Walk leg. If the station we boarded at has an entry in the *current* round's journal, it means we took two legs in the same round. We do *not* decrement the round counter so the loop processes the second leg. If it's not found, we decrement the round counter to step back in time.

**Lines 353-360:** `if len(legs) > 0 && current == params.Origin {`
If we successfully backtracked all the way to the origin, we package the `legs` into a `Path` object and add it to our results.

---

## Part 6: Dry Run Example

**Scenario:**
- Origin: **A**, Destination: **C**, Time: **09:00**.
- **Train 1 (T1):** Departs **A** at 09:30, Arrives **B** at 10:30.
- **Walk:** From **B** to **B-North** takes 10 minutes (600 seconds).
- **Train 2 (T2):** Departs **B-North** at 11:00, Arrives **C** at 12:00.

### Round 0 (Init)
- `tau[0][A]` = 09:00.
- `marked` = {A}.

### Round 1
1. **Queue Setup:** `marked` has A. T1 stops at A. `queue` = {T1}.
2. **Ride-Forward T1:**
   - At **A**: `arrivalHere` = 09:00. T1 departs at 09:30. We board T1. `currentTrip` = T1.
   - At **B**: We are on T1. Arrival is 10:30.
     - `tau[1][B]` = 10:30, `bestArrival[B]` = 10:30.
     - `journal[1][B]` = {board: A, trip: T1}.
     - `newMarked` = {B}.
3. **Footpaths:**
   - Loop over `newMarked` {B}.
   - Walk from **B** to **B-North** takes 10 mins. Arrival = 10:40.
   - `tau[1][B-North]` = 10:40, `bestArrival[B-North]` = 10:40.
   - `journal[1][B-North]` = {board: B, route: WALK}.
   - `newMarked` adds {B-North}.
4. `marked` becomes {B, B-North}.

### Round 2
1. **Queue Setup:** `marked` has B-North. T2 stops at B-North. `queue` = {T2}.
2. **Ride-Forward T2:**
   - At **B-North**: `arrivalHere` = 10:40 (from walk). T2 departs at 11:00. Board T2.
   - At **C**: We are on T2. Arrival is 12:00.
     - `tau[2][C]` = 12:00.
     - `journal[2][C]` = {board: B-North, trip: T2}.
     - `newMarked` = {C}.
3. **Footpaths:** None.
4. `marked` becomes {C}. (Algorithm finishes soon after).

### Path Reconstruction
1. Start at Dest **C**, Round **2**.
2. `journal[2][C]` shows we boarded at **B-North**. 
   - Add Leg: `B-North -> C (T2)`.
   - Is **B-North** in `journal[2]`? No.
   - Decrement Round to **1**. Current = **B-North**.
3. `journal[1][B-North]` shows a WALK from **B**.
   - Add Leg: `B -> B-North (WALK)`.
   - Is **B** in `journal[1]`? YES! (Because T1 arrived in Round 1).
   - DO NOT decrement round! Current = **B**, Round remains **1**.
4. `journal[1][B]` shows we boarded at **A**.
   - Add Leg: `A -> B (T1)`.
   - Is **A** in `journal[1]`? No. 
   - Decrement Round to **0**. Current = **A**.
5. Current is **A** (Origin) and Round is **0**. Loop ends.
6. Final Output: `[A -> B (T1), B -> B-North (Walk), B-North -> C (T2)]`.
