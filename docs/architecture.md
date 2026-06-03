Matchmaker — System Architecture

## Overview

Internal architecture of the matchmaking engine — how players are stored,
how workers discover and claim them, how matches are formed, and how the
system recovers from failures.

---

## The Core Problem

Matchmaking is a concurrent set-selection problem with one invariant that
cannot be violated: **no player may appear in two matches simultaneously.**

The design centres on solving this one problem correctly. Everything else
(throughput, match quality, fairness) is built on top of that foundation.

---

## High-Level Component Map

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP API Layer                          │
│         POST /enqueue    DELETE /enqueue/:id    GET /health     │
│                   GET /metrics    GET /matches                  │
└────────────────────────────┬────────────────────────────────────┘
                             │ Arc<MatchmakerCore>
┌────────────────────────────▼────────────────────────────────────┐
│                        MatchmakerCore                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                       PlayerPool                        │   │
│  │                                                         │   │
│  │   DashMap<Uuid, Arc<Player>>                            │   │
│  │   (16 internal shards — primary store)                  │   │
│  │                                                         │   │
│  │   RwLock<BTreeMap<(u32, Uuid), Weak<Player>>>           │   │
│  │   (shared read lock — rating index for range scans)     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Arc<Metrics>  — AtomicU64 counters, zero locks                 │
│  Arc<Notify>   — fires notify_one() on every enqueue            │
│  Arc<RwLock<VecDeque<Match>>> — bounded match history           │
│  Arc<Config>   — immutable after startup                        │
└────────────────────────────┬────────────────────────────────────┘
                             │ Arc clones distributed at spawn
          ┌──────────────────┼──────────────────────┐
          ▼                  ▼                       ▼
┌──────────────────┐ ┌──────────────────┐ ┌───────────────────┐
│    Worker 1      │ │    Worker N      │ │   Reaper Task     │
│  (Tokio task)    │ │  (Tokio task)    │ │  (1 Tokio task)   │
│                  │ │                  │ │                   │
│  attempt_match   │ │  attempt_match   │ │ scans every 1000ms│
│  on Notify or    │ │  on Notify or    │ │ resets stale      │
│  50ms tick       │ │  50ms tick       │ │ Claimed players   │
└──────────────────┘ └──────────────────┘ └───────────────────┘
```
---

## Player Storage

### Why Two Structures

The hot path needs two operations with incompatible optimal data structures:

- **Insert/remove by player ID** (enqueue, evict): O(1) — hash map
- **Range scan by skill rating** (candidate discovery): O(log N + k) — sorted tree

No single structure satisfies both. The solution is two complementary
structures maintained in sync behind the `PlayerPool` abstraction.

### Primary Store — DashMap
DashMap<Uuid, Arc<Player>>

16 internal shards. UUID v4 keys distribute uniformly — enqueue and
eviction for different players almost never touch the same shard.
**Source of truth** — a player exists if and only if they are in the DashMap.

### Rating Index — BTreeMap
RwLock<BTreeMap<(u32, Uuid), Weak<Player>>>

Composite key `(skill_rating, uuid)` makes range scans directly
expressible. Stores `Weak<Player>` — when a player is removed from the
primary store, the Weak pointer goes dead and is skipped lazily during scans.

### The Sync Invariant

Both structures are always updated together inside `PlayerPool::insert()`
and `PlayerPool::remove()`. No code outside `engine/bucket.rs` accesses
either structure directly.

---

## Player State Machine
```
                     ┌─────────────────────────────────────┐
                     │                                     │
                     ▼                                     │
Enqueued ──► Waiting(0) ──CAS──► Claimed(1) ──CAS──► Matched(2) ──► removed
▲                    │
│                    └──CAS──► Evicted(3)
│
└──── CAS (rollback / Reaper) ────┘
```

### Valid Transitions

| From        | To          | Actor            | Mechanism          |
|-------------|-------------|------------------|--------------------|
| Waiting (0) | Claimed (1) | Worker           | CAS AcqRel/Acquire |
| Waiting (0) | Evicted (3) | Cancel handler   | CAS AcqRel/Acquire |
| Claimed (1) | Waiting (0) | Worker rollback  | CAS Release/Relaxed|
| Claimed (1) | Waiting (0) | Reaper recovery  | CAS AcqRel/Acquire |
| Claimed (1) | Matched (2) | Owning worker    | CAS Release/Relaxed|

### Why CAS and Not a Mutex

`AtomicU8` costs 1 byte. `compare_exchange` with `AcqRel` ordering costs
one locked bus instruction (~5ns). No allocation. No queue. No scheduler
involvement. The hardware guarantees exactly one winner across all cores
simultaneously.

---

## Matchmaking Pipeline
Worker wakes (Notify or 50ms tick)
│
├─ 1. SEED SELECTION
│      oldest_waiting() → minimum join_timestamp among Waiting players
│      Tie-break: join_timestamp ASC, skill_rating ASC, id ASC
│
├─ 2. WINDOW COMPUTATION
│      Stage 1  (0–5s):    ± 50 MMR
│      Stage 2  (5–15s):   ± 100 MMR
│      Stage 3  (15–30s):  ± 200 MMR
│      Stage 4  (30–60s):  ± 400 MMR
│      Stage 5  (60s+):    ± 9999 MMR  ← starvation floor
│
├─ 3. CANDIDATE DISCOVERY
│      BTreeMap read lock (shared — all workers hold simultaneously)
│      range_scan(seed.mmr - window, seed.mmr + window)
│      Filter: state == Waiting only, skip dead Weak pointers
│      Sort: join_timestamp ASC, skill_rating ASC, id ASC
│      Cap: MAX_CANDIDATES_PER_SCAN = 200
│      → < 10 candidates → InsufficientCandidates
│
├─ 4. ATOMIC CLAIMING
│      compare_exchange(Waiting→Claimed) per candidate
│      success → store worker_id, claim_timestamp, add to claimed[]
│      failure → skip, continue
│      if claimed.len() < 10:
│        rollback all → CAS(Claimed→Waiting) → ClaimFailed
│
├─ 5. TEAM BALANCING
│      exhaustive_balance(claimed[10])
│      252 bitmasks with popcount == 5
│      minimise |2 × sum_a − total_sum|
│      → always succeeds, always optimal
│
└─ 6. MATCH CREATION
mark_matched() × 10: CAS(Claimed→Matched)
pool.remove() × 10: DashMap + BTreeMap remove
push MatchRecord to history
update all atomic metrics counters
→ MatchAttemptResult::Success

---

## Worker and Reaper
┌─────────────────────────────────────┐  ┌─────────────────────────────────────┐
│   Workers (WORKER_COUNT Tokio tasks)│  │      Reaper (1 Tokio task)          │
└──────────────────┬──────────────────┘  └──────────────────┬──────────────────┘
│                                         │
▼                                         ▼
┌──────────────────────┐               ┌────────────────────────────┐
│  select! {           │               │  every REAPER_INTERVAL_MS: │
│                      │               │                            │
│  notify.notified()   │               │  scan all_players()        │
│  → attempt_match()   │               │                            │
│                      │               │  if state == Claimed AND   │
│  tick()              │               │  age > STALE_CLAIM_        │
│  → attempt_match()   │               │       TIMEOUT_MS:          │
│                      │               │                            │
│  shutdown()          │               │  CAS(Claimed → Waiting)    │
│  → break             │               │                            │
│  }                   │               │  log recovery event        │
└──────────────────────┘               │                            │
│                           │  metrics                   │
▼                           │  .stale_claims_recovered++ │
┌──────────────────────┐               └────────────────────────────┘
│  WorkerState tracks  │
│  consecutive failures│
│  per seed for        │
│  throughput guard    │
└──────────────────────┘

### Why CAS and Not store() in Reaper

If the Reaper used `store(Waiting)` instead of `compare_exchange`:
t=0  Reaper: age check passes (player is Claimed)
t=1  Worker: CAS(Claimed→Matched) succeeds — match formed
t=2  Reaper: store(Waiting) — overwrites Matched(2) ← BUG
t=3  Player re-enters pool, matched a second time ← correctness violation

With `compare_exchange(Claimed, Waiting)`, the CAS at t=2 fails because
the state is now `Matched(2)`, not `Claimed(1)`. Safe in all interleavings.

**Recovery guarantee:**
STALE_CLAIM_TIMEOUT_MS + REAPER_INTERVAL_MS = 500ms + 1000ms = 1500ms

---

## Graceful Shutdown
SIGTERM / SIGINT received
│
▼
CancellationToken::cancel()
│
├── HTTP server stops accepting connections (5s drain timeout)
├── Workers observe shutdown.cancelled() → exit loop
├── Reaper observes same token → exits
├── JoinSet::join_next() with 10s timeout
├── Final metrics snapshot logged
└── Process exits 0

---

## Metrics Architecture

All counters are `AtomicU64`/`AtomicI64` with `Relaxed` ordering.
`fetch_add` costs ~1ns and never blocks. Metrics never contend with
matchmaking workers. Rolling averages computed at read time:
avg_wait_ms = total_wait_time_ms / total_players_matched

---

## API Layer

Handlers do three things only: extract request data, call into
`MatchmakerCore`, serialize the response.
POST /enqueue      → validate → core.enqueue()  → 200 / 409 / 422
DELETE /enqueue/:id → core.cancel()             → 200 / 404 / 409
GET /health        → reads current_queue_size   → 200
GET /metrics       → core.metrics_snapshot()    → 200
GET /matches       → core.recent_matches(limit) → 200

---

## Horizontal Scaling Path

Replace `PlayerPool` internals with Redis — the matching algorithm is
unchanged:

| Current                  | Redis equivalent                          |
|--------------------------|-------------------------------------------|
| DashMap insert           | `HSET player:{id}` + `ZADD queue {mmr}`   |
| BTreeMap range scan      | `ZRANGEBYSCORE queue {min} {max}`         |
| CAS(Waiting→Claimed)     | Lua script: atomic HGET + HSET            |
| pool.remove()            | `ZREM queue {id}` + `DEL player:{id}`     |

Only `engine/bucket.rs` needs a new backend. Everything above it unchanged.

---

## Known Limitations

| Limitation                        | Impact                          | Mitigation                          |
|-----------------------------------|---------------------------------|-------------------------------------|
| `oldest_waiting()` is O(N)        | ~100µs at 100K players          | Replace with `BinaryHeap`           |
| In-memory only                    | State lost on restart           | Redis migration path above          |
| Cancel while Claimed returns 409  | Player mid-match cannot cancel  | Client should retry after ~50ms     |
| No auth on `/metrics`             | Operational data is public      | Add middleware in production        |
| Stale claim recovery window 1500ms| Players stuck up to 1.5s        | Reduce `REAPER_INTERVAL_MS`         |