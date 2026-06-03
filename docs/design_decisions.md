# Matchmaker — Design Decisions

Each decision below records the problem, the alternatives considered,
the choice made, and the reasoning. This document exists so future
engineers understand *why* the code is the way it is.

---

## 1. Dual-Structure Player Pool (DashMap + BTreeMap)

**Problem**: The matchmaking hot path needs two operations with conflicting
optimal data structures:
- O(1) insert/remove by player ID (enqueue, evict)
- O(log N + k) range scan by skill rating (candidate discovery)

No single standard data structure satisfies both.

**Alternatives considered**:

| Option | Insert/Remove | Range Scan | Problem |
|---|---|---|---|
| `HashMap` only | O(1) | O(N) linear scan | Unacceptable at 10K+ players |
| `BTreeMap` only | O(log N) | O(log N + k) | Single global write lock on insert |
| Sharded skill buckets | O(1) | O(buckets scanned) | Fixed granularity breaks dynamic windows |
| Lock-free skip list | O(log N) | O(log N + k) | External crate, complex memory model |

**Decision**: DashMap as primary store (O(1), 16-shard concurrent writes) +
`RwLock<BTreeMap>` as secondary index (O(log N + k) range scan, shared reads).

**Reasoning**: The write path (enqueue) is dominated by DashMap — no global
lock. The read path (scan) is dominated by BTreeMap under a shared read lock —
multiple workers scan simultaneously without blocking each other. Write lock
on BTreeMap is held only for a single insert/remove (~1µs). This is a standard
database pattern: primary store + secondary index.

**Tradeoff accepted**: Two structures must stay in sync. The `PlayerPool`
abstraction enforces this invariant — no code outside `bucket.rs` touches
either structure directly.

---

## 2. AtomicU8 CAS State Machine for Player Ownership

**Problem**: Multiple workers scan overlapping candidate sets simultaneously.
Without a coordination mechanism, two workers could claim the same player
and place them in two different matches — a critical correctness violation.

**Alternatives considered**:

| Option | Correctness | Performance | Complexity |
|---|---|---|---|
| Single global Mutex around find+claim | Correct | Poor — serialises all workers | Low |
| Per-player Mutex | Correct | Better — per-player contention | Medium |
| AtomicU8 CAS | Correct | Best — hardware atomic, no locks | Medium |

**Decision**: `AtomicU8` with `compare_exchange(WAITING, CLAIMED, AcqRel, Acquire)`.

**Reasoning**: CAS is the canonical lock-free ownership transfer primitive.
The hardware guarantees that exactly one thread wins per CAS on a given
memory location. No mutex is needed. The `AcqRel`/`Acquire` ordering
establishes the necessary happens-before relationship: the claiming worker
observes all writes made before the player entered Waiting state.

**Tradeoff accepted**: Failed CAS attempts require rollback — a worker that
claims 7 players but loses CAS on the 8th must release all 7 and retry.
Under heavy contention this increases retry rate. In practice, the retry
rate is low because the pool is large enough that workers find non-overlapping
candidate sets on most attempts.

---

## 3. Exhaustive C(10,5) Team Balance

**Problem**: 10 players must be split into two teams of 5 that are as
skill-balanced as possible.

**Alternatives considered**:

| Algorithm | Quality | Runtime | Notes |
|---|---|---|---|
| Random split | Poor | O(1) | Unacceptable variance |
| Sort + alternate | Good | O(N log N) | Not optimal |
| Snake draft | Good | O(N log N) | Not optimal |
| Exhaustive search | Optimal | O(C(10,5)) = O(252) | Correct choice |
| Dynamic programming | Near-optimal | O(N·S) | Overkill for N=10 |

**Decision**: Exhaustive enumeration of all 252 bitmasks with `popcount == 5`.

**Reasoning**: N=10 is fixed by the problem specification. C(10,5) = 252
iterations is O(1) — it cannot grow. Each iteration is a tight inner loop
computing a sum from a pre-cached ratings array (~10 additions). Total
runtime is ~500ns on modern hardware. This gives the provably optimal
split with zero approximation error. Any other algorithm trades quality
for performance savings that are meaningless at this scale.

**Tradeoff accepted**: Only valid for exactly N=10. If the match size ever
changes, this algorithm must be reconsidered. Documented in code.

---

## 4. Tokio Async Tasks for Workers (not std::thread)

**Problem**: The matchmaking computation (~10–50µs per attempt) is CPU-bound,
not I/O-bound. The conventional wisdom is to use `std::thread` for CPU-bound
work and `tokio::spawn` for I/O-bound work.

**Why async tasks are correct here**:

Worker time is split approximately:
- ~0.05ms: actual CPU work (scan + CAS + balance)
- ~50ms: idle, waiting for Notify signal or tick timer

CPU utilisation per worker: ~0.1%. This is overwhelmingly wait-bound.
The Notify signal and interval timer are async primitives — they integrate
naturally with Tokio. Using `std::thread` would require a custom channel
or condvar to replace `Notify`, adding complexity for no benefit.

If benchmarking ever shows that the 50µs CPU burst causes Tokio runtime
starvation, the fix is `tokio::task::spawn_blocking` for the CPU work —
a one-line change.

**Decision**: `tokio::spawn` async tasks.

**Tradeoff accepted**: Under pathological load (100% match success rate,
zero idle time), workers could monopolise Tokio threads. Mitigated by
`WORKER_COUNT ≤ available_parallelism` and the natural idle periods between
match attempts.

---

## 5. Notify::notify_one() vs notify_waiters()

**Problem**: When a player enqueues, workers should be woken to attempt
a match. Two options:

- `notify_one()`: wake exactly one worker
- `notify_waiters()`: wake all waiting workers

**Decision**: `notify_one()`.

**Reasoning**: A single new player enqueue rarely makes a match possible
by itself (9 more players must already be waiting). Waking all N workers
causes them all to scan, fail with InsufficientCandidates, and go back
to sleep — wasted work. `notify_one()` wakes a single worker to check
conditions. If a match is possible, it forms it. If not, it sleeps.
The periodic tick ensures no player waits beyond `WORKER_TICK_MS` even
if `notify_one()` misses a formation opportunity.

**Tradeoff accepted**: Under heavy load where 10 players enqueue rapidly,
only one worker wakes per enqueue. The tick fallback (default 50ms) catches
any missed opportunities. In practice, the tick fires frequently enough
that this is not a throughput bottleneck.

---

## 6. Oldest-First Seed Selection

**Problem**: Which player should anchor each match attempt?

**Decision**: Globally oldest Waiting player (minimum `join_timestamp`),
with a throughput guard: skip the seed after `SEED_RETRY_LIMIT` consecutive
failures and use the oldest player outside their MMR range instead.

**Reasoning**: Oldest-first is the most defensible fairness model — it
directly honours queue order. No player who joined before you can be
matched after you (in the absence of the throughput guard).

The throughput guard prevents one unsatisfiable player (e.g. MMR 2950
in a pool of 1000–1500 players) from blocking all workers. Without it,
all workers would repeatedly attempt and fail to match the same player.

**Tradeoff accepted**: A player who triggers the throughput guard may be
temporarily skipped, allowing players who joined later to match first.
This is bounded: after `SEED_RETRY_LIMIT` skips, the player's constraint
relaxation has advanced, making them easier to match.

---

## 7. Reaper Task for Worker Crash Recovery

**Problem**: A worker that claims players and then panics leaves those
players permanently in `Claimed` state — they can never be matched.

**Decision**: A background Reaper task scans all players every
`REAPER_INTERVAL_MS`. Any player in `Claimed` state with
`now - claim_timestamp > STALE_CLAIM_TIMEOUT_MS` is reset to `Waiting`
via CAS.

**Why CAS and not store()**:

If the Reaper used `state.store(WAITING)` directly, a healthy worker that
is mid-formation could have its claim silently stolen. The worker would
then call `mark_matched()` on a Waiting player, corrupting state.

Using `compare_exchange(CLAIMED, WAITING)` is safe in all interleavings:
- Healthy worker completes first: state becomes MATCHED, Reaper CAS fails → correct
- Reaper resets first: state becomes WAITING, worker's next CAS fails → worker rolls back → correct

**Tradeoff accepted**: Players stuck in `Claimed` state are not re-matched
for up to `STALE_CLAIM_TIMEOUT_MS + REAPER_INTERVAL_MS` (default 1.5s).
This is acceptable — the window is small and the condition is rare.

---

## 8. Weak<Player> in BTreeMap Index

**Problem**: The BTreeMap rating index needs to reference players without
preventing their deallocation after eviction from the primary store.

**Decision**: `Weak<Player>` in the BTreeMap. `Weak::upgrade()` is called
during range_scan — dead entries (players already evicted) are skipped.

**Reasoning**: If the BTreeMap held `Arc<Player>`, evicted players would
remain alive (non-zero refcount) even after removal from the DashMap.
This would cause memory to grow unboundedly under high churn and would
require an explicit cleanup pass to drop the Arcs.

With `Weak<Player>`, the player is deallocated as soon as the DashMap
removes it (refcount drops to zero). The BTreeMap entry becomes a dead
Weak — a harmless tombstone that is skipped lazily during scans.

**Tradeoff accepted**: `Weak::upgrade()` costs one atomic increment + one
branch per candidate during scan. At 200 candidates per scan this is
~200 atomic ops — negligible.

---

## 9. Bounded Match History (VecDeque)

**Problem**: Match results must be accessible via GET /matches without
growing memory unboundedly.

**Decision**: `RwLock<VecDeque<Match>>` with capacity `MATCH_HISTORY_LIMIT`
(default 10,000). Oldest matches are evicted when the limit is exceeded.

**Reasoning**: A VecDeque with a fixed capacity bound provides O(1) push/pop
and bounded memory. The RwLock is held only for the brief duration of
`push_back` + optional `pop_front` — never during the matchmaking scan.
At ~500 bytes per match × 10,000 matches = ~5MB memory footprint.

**Tradeoff accepted**: Matches older than the history limit are permanently
lost. For this assignment, 10,000 matches is sufficient. In production,
a persistent store (PostgreSQL) would replace this structure.

---

## 10. All Configuration from Environment Variables

**Problem**: Hardcoded constants cannot be tuned in production without
recompilation. A config file requires a deployment artifact beyond the binary.

**Decision**: All tunable parameters sourced from environment variables
with sensible defaults. `.env` file supported for development via `dotenvy`.

**Reasoning**: Environment variables are the 12-factor app standard for
configuration. They work uniformly across bare metal, Docker, Kubernetes,
and AWS ECS. No config file format to parse, no schema to maintain.
Defaults allow the service to run correctly out of the box with zero
configuration — important for the assignment's simulation deliverable.

**Tradeoff accepted**: Large number of environment variables (15+) can be
unwieldy. Mitigated by `.env.example` with full documentation and sensible
defaults for every variable.