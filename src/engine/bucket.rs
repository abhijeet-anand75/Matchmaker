//! Player pool storage — the dual-structure player registry.
//!
//! [`PlayerPool`] is the single source of truth for all players currently
//! waiting in the matchmaking queue. It maintains two complementary structures:
//!
//! - **Primary store**: `DashMap<Uuid, Arc<Player>>` — O(1) insert/remove/lookup
//!   by player ID. 16 internal shards eliminate global lock contention on the
//!   write path.
//!
//! - **Rating index**: `RwLock<BTreeMap<(u32, Uuid), Weak<Player>>>` — O(log N + k)
//!   range scan by skill rating. The composite key `(skill_rating, id)` guarantees
//!   uniqueness when two players share identical ratings. Stores `Weak<Player>`
//!   to avoid keeping players alive after eviction from the primary store.
//!
//! # Invariant
//!
//! Both structures are always updated together. No code outside this module
//! accesses either structure directly. The `PlayerPool` API is the only legal
//! way to mutate pool state.
//!
//! # Dead Entry Handling
//!
//! When a player is removed from the primary store, their `Weak<Player>` in the
//! rating index becomes dead. Dead entries are:
//! - Skipped lazily during `range_scan` (upgrade fails → skip)
//! - Cleaned eagerly during `remove` (both structures updated together)
//! - The Reaper task calls `remove` for matched/evicted players, ensuring
//!   the index does not grow unboundedly under high churn.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, Weak};

use dashmap::DashMap;
use uuid::Uuid;

use crate::models::{Player, player_state};

//  Constants 

/// Maximum number of candidates returned by a single `range_scan` call.
///
/// Caps the cost of the claiming loop to a bounded number of CAS attempts.
/// At 200 candidates, a worker will almost always find 10 claimable players
/// even under heavy contention. Excess candidates beyond 10 claimable ones
/// are wasted CAS cycles — this bound keeps the work finite.
pub const MAX_CANDIDATES_PER_SCAN: usize = 200;

/// Maximum skill rating accepted during enqueue validation.
pub const MAX_SKILL_RATING: u32 = 3000;

//  PlayerPool 

/// Thread-safe dual-structure player registry.
///
/// See module-level documentation for the full design rationale.
pub struct PlayerPool {
    /// Primary store: source of truth for player existence and state.
    /// DashMap provides concurrent insert/remove via 16 internal shards.
    primary: DashMap<Uuid, Arc<Player>>,

    /// Rating index: enables O(log N + k) range scans by skill rating.
    /// `RwLock` allows multiple workers to scan simultaneously (shared read),
    /// while enqueue/evict hold a brief exclusive write.
    /// Stores `Weak<Player>` — does not extend player lifetime after eviction.
    index: RwLock<BTreeMap<(u32, Uuid), Weak<Player>>>,
}

impl PlayerPool {
    /// Construct an empty player pool.
    pub fn new() -> Self {
        Self {
            // 16 shards is the DashMap default — distributes UUID-keyed
            // inserts uniformly across shards, minimising per-shard lock waits.
            primary: DashMap::new(),
            index: RwLock::new(BTreeMap::new()),
        }
    }

    //  Write operations 

    /// Insert a player into both the primary store and the rating index.
    ///
    /// Called exclusively from the enqueue path. The player must be in
    /// `Waiting` state before insertion.
    ///
    /// # Complexity
    /// - Primary store insert: O(1) amortised
    /// - Rating index insert: O(log N) — BTreeMap write lock held briefly
    pub fn insert(&self, player: Arc<Player>) {
        let key = (player.skill_rating, player.id);
        let weak = Arc::downgrade(&player);

        // Insert into primary store first.
        self.primary.insert(player.id, Arc::clone(&player));

        // Then insert into rating index under a brief write lock.
        // The write lock is held only for the BTreeMap insert — not for
        // the primary store insert — minimising contention duration.
        self.index
            .write()
            .expect("rating index RwLock is never poisoned")
            .insert(key, weak);
    }

    /// Remove a player from both the primary store and the rating index.
    ///
    /// Called after match formation (player is Matched) or after cancellation
    /// (player is Evicted). Safe to call if the player is not present —
    /// returns `None` in that case.
    ///
    /// # Complexity
    /// - Primary store remove: O(1) amortised
    /// - Rating index remove: O(log N) — BTreeMap write lock held briefly
    pub fn remove(&self, id: &Uuid) -> Option<Arc<Player>> {
        // Remove from primary store first — get the Arc to retrieve skill_rating
        // for the index key.
        let player = self.primary.remove(id).map(|(_, v)| v)?;

        // Remove from rating index under a brief write lock.
        let key = (player.skill_rating, player.id);
        self.index
            .write()
            .expect("rating index RwLock is never poisoned")
            .remove(&key);

        Some(player)
    }

    //  Read operations 

    /// Scan for players within a skill rating range.
    ///
    /// Returns up to [`MAX_CANDIDATES_PER_SCAN`] players whose skill rating
    /// falls in `[min_rating, max_rating]` and whose state is `Waiting`.
    ///
    /// Results are sorted by `(join_timestamp ASC, skill_rating ASC, id ASC)`
    /// to prefer older players as match partners — honouring FIFO fairness
    /// within the candidate set.
    ///
    /// Dead `Weak<Player>` entries (players evicted after the index entry was
    /// created) are silently skipped. They do not affect correctness.
    ///
    /// # Complexity
    /// - BTreeMap range scan: O(log N + k) where k = entries in range
    /// - Per-entry work: one `Weak::upgrade` + one `AtomicU8` load
    /// - Total: O(log N + k), capped at O(log N + MAX_CANDIDATES_PER_SCAN)
    pub fn range_scan(&self, min_rating: u32, max_rating: u32) -> Vec<Arc<Player>> {
        // Clamp bounds to avoid u32 underflow if min_rating would wrap.
        let min_rating = min_rating.min(MAX_SKILL_RATING);
        let max_rating = max_rating.min(MAX_SKILL_RATING);

        // Build range bounds using the composite key structure.
        // (rating, MIN_UUID) to (rating, MAX_UUID) covers all players at a
        // given rating. Using Uuid::nil() and a "max" UUID as sentinels.
        let lower = (min_rating, Uuid::nil());
        // Uuid::max() is all-0xFF bytes — sorts after all valid UUIDs.
        let upper = (max_rating, uuid_max());

        let mut candidates = Vec::with_capacity(32);

        // Acquire shared read lock — multiple workers hold this simultaneously.
        // The lock is held for the entire iteration. This is acceptable because:
        // 1. Read lock is non-exclusive — all workers scan concurrently.
        // 2. The iteration is fast — only pointer upgrades and state loads.
        // 3. Write lock (enqueue/evict) waits for current readers to finish —
        //    but write operations are brief (single BTreeMap insert/remove).
        let guard = self
            .index
            .read()
            .expect("rating index RwLock is never poisoned");

        for (_, weak) in guard.range(lower..=upper) {
            // Attempt to promote Weak → Arc.
            // Fails if the player was removed from the primary store after
            // this index entry was created (dead entry — skip silently).
            let Some(player) = weak.upgrade() else {
                continue;
            };

            // Only include players in Waiting state.
            // Players in Claimed, Matched, or Evicted state are mid-operation
            // or already done — do not include them as candidates.
            if player.state.load(std::sync::atomic::Ordering::Acquire)
                != player_state::WAITING
            {
                continue;
            }

            candidates.push(player);

            // Hard cap — stop iterating once we have enough candidates.
            // Excess candidates beyond what we need for one match only
            // increase CAS contention without improving match quality.
            if candidates.len() >= MAX_CANDIDATES_PER_SCAN {
                break;
            }
        }

        // Drop the read lock before sorting — sort is CPU work that does not
        // need the lock. This minimises read lock hold time.
        drop(guard);

        // Sort by (join_timestamp ASC, skill_rating ASC, id ASC).
        // Older players appear first — FIFO fairness within the candidate set.
        // Skill rating and UUID are deterministic tie-breakers.
        candidates.sort_unstable_by(|a, b| {
            a.join_timestamp
                .cmp(&b.join_timestamp)
                .then_with(|| a.skill_rating.cmp(&b.skill_rating))
                .then_with(|| a.id.cmp(&b.id))
        });

        candidates
    }

    /// Find the globally oldest `Waiting` player in the pool.
    ///
    /// This is the seed selection strategy: the longest-waiting player anchors
    /// each match attempt, ensuring FIFO fairness at the seed level.
    ///
    /// # Complexity
    /// O(N) — iterates the primary store to find the minimum `join_timestamp`.
    /// Acceptable at N ≤ 100K; a min-heap optimisation is documented in the
    /// README as the first production scaling improvement.
    ///
    /// Returns `None` if the pool is empty or contains no Waiting players.
    pub fn oldest_waiting(&self) -> Option<Arc<Player>> {
        self.primary
            .iter()
            .filter_map(|entry| {
                let player = entry.value();
                if player.is_waiting() {
                    Some(Arc::clone(player))
                } else {
                    None
                }
            })
            .min_by(|a, b| {
                // Primary: oldest join time first
                a.join_timestamp
                    .cmp(&b.join_timestamp)
                    // Secondary: lower skill rating first (arbitrary but consistent)
                    .then_with(|| a.skill_rating.cmp(&b.skill_rating))
                    // Tertiary: UUID lexicographic (fully deterministic tie-break)
                    .then_with(|| a.id.cmp(&b.id))
            })
    }

    /// Find the oldest `Waiting` player whose skill rating is outside
    /// the given range `[exclude_min, exclude_max]`.
    ///
    /// Used by the seed throughput guard: when the primary seed has failed
    /// `SEED_RETRY_LIMIT` consecutive times, skip to the oldest player in
    /// a different MMR range to prevent all workers from spinning on one
    /// unsatisfiable player.
    pub fn oldest_waiting_excluding_range(
        &self,
        exclude_min: u32,
        exclude_max: u32,
    ) -> Option<Arc<Player>> {
        self.primary
            .iter()
            .filter_map(|entry| {
                let player = entry.value();
                let mmr = player.skill_rating;
                if player.is_waiting() && !(mmr >= exclude_min && mmr <= exclude_max) {
                    Some(Arc::clone(player))
                } else {
                    None
                }
            })
            .min_by(|a, b| {
                a.join_timestamp
                    .cmp(&b.join_timestamp)
                    .then_with(|| a.skill_rating.cmp(&b.skill_rating))
                    .then_with(|| a.id.cmp(&b.id))
            })
    }

    /// Iterate all players in the primary store.
    ///
    /// Used exclusively by the Reaper task to scan for stale claims.
    /// Returns cloned `Arc<Player>` references — safe to hold after the
    /// iterator is dropped.
    pub fn all_players(&self) -> Vec<Arc<Player>> {
        self.primary
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Returns the number of players currently in the pool (all states).
    ///
    /// O(1) — DashMap maintains an internal counter.
    pub fn len(&self) -> usize {
        self.primary.len()
    }

    /// Returns `true` if the pool contains no players.
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty()
    }

    /// Returns `true` if a player with the given ID exists in the pool.
    ///
    /// Used by the enqueue handler to detect duplicate registrations.
    pub fn contains(&self, id: &Uuid) -> bool {
        self.primary.contains_key(id)
    }

    /// Look up a player by ID.
    ///
    /// Returns `None` if the player is not in the pool.
    pub fn get(&self, id: &Uuid) -> Option<Arc<Player>> {
        self.primary.get(id).map(|entry| Arc::clone(entry.value()))
    }
}

impl Default for PlayerPool {
    fn default() -> Self {
        Self::new()
    }
}

//  UUID sentinel 

/// Returns a UUID with all bytes set to `0xFF`.
///
/// Used as an upper bound sentinel in BTreeMap range scans. All valid UUID v4
/// values sort before this sentinel, ensuring the range `(rating, nil)..(rating,
/// max)` covers all players at a given rating.
#[inline]
fn uuid_max() -> Uuid {
    Uuid::from_bytes([0xFF; 16])
}

//  Tests 

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uuid::Uuid;

    fn make_player(skill_rating: u32) -> Arc<Player> {
        Arc::new(Player::new(Uuid::new_v4(), skill_rating))
    }

    #[test]
    fn test_insert_and_contains() {
        let pool = PlayerPool::new();
        let p = make_player(1000);
        let id = p.id;
        pool.insert(Arc::clone(&p));
        assert!(pool.contains(&id));
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_remove_clears_both_structures() {
        let pool = PlayerPool::new();
        let p = make_player(1000);
        let id = p.id;
        pool.insert(Arc::clone(&p));
        let removed = pool.remove(&id);
        assert!(removed.is_some());
        assert!(!pool.contains(&id));
        assert_eq!(pool.len(), 0);

        
        let results = pool.range_scan(900, 1100);
        assert!(results.is_empty());
    }

    #[test]
    fn test_range_scan_returns_waiting_players_only() {
        let pool = PlayerPool::new();

        let p1 = make_player(1000);
        let p2 = make_player(1050);
        let p3 = make_player(2000); // outside range

        pool.insert(Arc::clone(&p1));
        pool.insert(Arc::clone(&p2));
        pool.insert(Arc::clone(&p3));

        let results = pool.range_scan(900, 1100);
        assert_eq!(results.len(), 2);

        // p3 must not be in results
        assert!(!results.iter().any(|p| p.id == p3.id));
    }

    #[test]
    fn test_range_scan_excludes_claimed_players() {
        let pool = PlayerPool::new();
        let p = make_player(1000);
        pool.insert(Arc::clone(&p));

        // Claim the player
        p.try_claim(1, 0);

        let results = pool.range_scan(900, 1100);
        assert!(
            results.is_empty(),
            "Claimed player must not appear in scan results"
        );
    }

    #[test]
    fn test_range_scan_skips_dead_weak_pointers() {
        let pool = PlayerPool::new();
        let p = make_player(1000);
        let id = p.id;
        pool.insert(Arc::clone(&p));

        // Remove from primary — Weak in index becomes dead
        pool.remove(&id);

        // Manually verify: scan should return nothing (dead Weak skipped)
        let results = pool.range_scan(900, 1100);
        assert!(results.is_empty());
    }

    #[test]
    fn test_oldest_waiting_returns_earliest_joiner() {
        let pool = PlayerPool::new();

        let p1 = make_player(1000);
        // Small sleep to ensure distinct Instant values
        std::thread::sleep(std::time::Duration::from_millis(5));
        let p2 = make_player(1200);

        let id1 = p1.id;
        pool.insert(Arc::clone(&p1));
        pool.insert(Arc::clone(&p2));

        let oldest = pool.oldest_waiting().expect("pool is not empty");
        assert_eq!(oldest.id, id1, "p1 joined first and must be returned");
    }

    #[test]
    fn test_oldest_waiting_skips_claimed() {
        let pool = PlayerPool::new();
        let p1 = make_player(1000);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let p2 = make_player(1200);
        let id2 = p2.id;

        pool.insert(Arc::clone(&p1));
        pool.insert(Arc::clone(&p2));

        // Claim p1 — it should be excluded from seed selection
        p1.try_claim(1, 0);

        let oldest = pool.oldest_waiting().expect("p2 is still waiting");
        assert_eq!(oldest.id, id2);
    }

    #[test]
    fn test_range_scan_sorted_by_join_timestamp() {
        let pool = PlayerPool::new();
        let mut ids_in_order = Vec::new();

        for _ in 0..5 {
            let p = make_player(1000);
            ids_in_order.push(p.id);
            pool.insert(Arc::clone(&p));
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        let results = pool.range_scan(900, 1100);
        assert_eq!(results.len(), 5);

        for (i, p) in results.iter().enumerate() {
            assert_eq!(
                p.id, ids_in_order[i],
                "Candidate at position {i} must be the {i}th player to join"
            );
        }
    }

    #[test]
    fn test_all_players_returns_every_entry() {
        let pool = PlayerPool::new();
        for rating in [800, 1000, 1200, 1400, 1600] {
            pool.insert(make_player(rating));
        }
        assert_eq!(pool.all_players().len(), 5);
    }

    #[test]
    fn test_concurrent_inserts_are_safe() {
        use std::sync::Arc as StdArc;

        let pool = StdArc::new(PlayerPool::new());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let pool = StdArc::clone(&pool);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    pool.insert(make_player(1000));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.len(), 800);
    }

    #[test]
    fn test_get_returns_correct_player() {
        let pool = PlayerPool::new();
        let p = make_player(1337);
        let id = p.id;
        pool.insert(Arc::clone(&p));

        let found = pool.get(&id).expect("player must be found");
        assert_eq!(found.id, id);
        assert_eq!(found.skill_rating, 1337);
    }

    #[test]
    fn test_oldest_waiting_excluding_range() {
        let pool = PlayerPool::new();

        let p_low = make_player(500);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let p_high = make_player(2900);
        let id_low = p_low.id;

        pool.insert(Arc::clone(&p_low));
        pool.insert(Arc::clone(&p_high));

        
        let result = pool.oldest_waiting_excluding_range(2800, 3000);
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, id_low);
    }
}