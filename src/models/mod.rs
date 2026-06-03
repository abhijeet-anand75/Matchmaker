//! Core domain models for the matchmaking engine.
//!
//! All types that cross module boundaries are defined here.
//! This module has zero internal dependencies — it depends only on
//! the standard library and external crates. Everything else depends on it.
//!
//! # Thread Safety
//!
//! [`Player`] is `Send + Sync` because all mutable fields use atomics or
//! `Mutex`. It is always heap-allocated behind `Arc<Player>` — never cloned
//! or moved after construction.
//!
//! [`Match`] and [`PlayerSnapshot`] are plain value types — `Clone + Send + Sync`.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

//  Player state constants

/// Typed constants for the `Player.state` atomic field.
///
/// Using a module of constants rather than an enum avoids the overhead of
/// converting between enum variants and `u8` on every atomic operation.
pub mod player_state {
    /// Player is in the queue and available for matching.
    pub const WAITING: u8 = 0;
    /// Player has been claimed by a worker for an in-progress match attempt.
    pub const CLAIMED: u8 = 1;
    /// Player has been successfully matched and removed from the active pool.
    pub const MATCHED: u8 = 2;
    /// Player has been removed from the queue by cancellation.
    pub const EVICTED: u8 = 3;
}

//  Player

/// A player waiting in the matchmaking queue.
///
/// Always heap-allocated behind `Arc<Player>`. Never cloned — shared ownership
/// via `Arc::clone`. The `Arc` reference count is the only ownership mechanism.
///
/// # Field Invariants
///
/// - `state` transitions are always via `compare_exchange` — never `store`
///   (except the initial construction where `store` is safe as no other
///   thread has access yet).
/// - `claimed_by` and `claim_timestamp` are always set/cleared atomically
///   together with the `state` CAS that claims/releases the player.
/// - `matched_at` is set exactly once, under `Mutex`, immediately before
///   the `state` transitions to `MATCHED`.
pub struct Player {
    /// Globally unique player identifier.
    pub id: Uuid,

    /// Player's current skill rating (MMR). Immutable after construction.
    /// Valid range: 0–10_000. Capped at enqueue time.
    pub skill_rating: u32,

    /// Monotonic timestamp of when the player joined the queue.
    /// Used for seed selection (oldest-first) and constraint relaxation
    /// (elapsed time drives window widening). Immutable after construction.
    pub join_timestamp: Instant,

    /// Current lifecycle state of this player.
    ///
    /// - `0` = Waiting  (available for matching)
    /// - `1` = Claimed  (owned by a worker mid-match-attempt)
    /// - `2` = Matched  (successfully placed in a match)
    /// - `3` = Evicted  (cancelled by player request)
    ///
    /// All transitions use `compare_exchange` with `AcqRel`/`Acquire` ordering
    /// to establish happens-before relationships between the claiming worker
    /// and any subsequent observers.
    pub state: AtomicU8,

    /// ID of the worker that currently holds this player in `Claimed` state.
    /// `0` means unclaimed. Set atomically alongside the `state` CAS.
    /// Used by the Reaper to attribute stuck claims in log output.
    pub claimed_by: AtomicU64,

    /// Unix timestamp (milliseconds) when the current claim was established.
    /// `0` means unclaimed. Used by the Reaper to detect stale claims:
    /// if `now_ms - claim_timestamp > STALE_CLAIM_TIMEOUT_MS`, the claim
    /// is considered abandoned and reset to `Waiting`.
    pub claim_timestamp: AtomicU64,

    /// Wall-clock time at which this player was matched.
    /// Set exactly once, immediately before transitioning to `MATCHED`.
    /// Used to compute per-player wait time in match result metrics.
    /// `None` until the player is matched.
    ///
    /// Uses `Mutex<Option<Instant>>` because `Instant` is not `Copy`-atomic
    /// and is written exactly once — lock contention is negligible.
    pub matched_at: Mutex<Option<Instant>>,
}

impl Player {
    /// Construct a new player entering the queue.
    ///
    /// State is initialized to `WAITING`. All atomic claim fields are `0`.
    pub fn new(id: Uuid, skill_rating: u32) -> Self {
        Self {
            id,
            skill_rating,
            join_timestamp: Instant::now(),
            state: AtomicU8::new(player_state::WAITING),
            claimed_by: AtomicU64::new(0),
            claim_timestamp: AtomicU64::new(0),
            matched_at: Mutex::new(None),
        }
    }

    /// Returns the player's current state as a `u8`.
    ///
    /// Uses `Acquire` ordering to ensure that any writes made by the thread
    /// that last set this state are visible to the caller.
    #[inline]
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// Returns `true` if this player is currently in `Waiting` state.
    #[inline]
    pub fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == player_state::WAITING
    }

    /// Attempt to atomically transition from `Waiting` → `Claimed`.
    ///
    /// Returns `true` if this worker successfully claimed the player.
    /// Returns `false` if another worker or a cancellation beat us to it.
    ///
    /// On success, records `worker_id` and `now_ms` into the claim fields.
    /// These are set with `Relaxed` ordering after the successful CAS because
    /// the `AcqRel` CAS already establishes the necessary happens-before.
    #[inline]
    pub fn try_claim(&self, worker_id: u64, now_ms: u64) -> bool {
        match self.state.compare_exchange(
            player_state::WAITING,
            player_state::CLAIMED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.claimed_by.store(worker_id, Ordering::Relaxed);
                self.claim_timestamp.store(now_ms, Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    /// Atomically release a claim, returning the player to `Waiting` state.
    ///
    /// Called during rollback when a worker cannot claim enough players
    /// to form a full match. Clears `claimed_by` and `claim_timestamp`.
    ///
    /// This always succeeds because only the owning worker calls this method,
    /// and the owning worker is the only entity that can modify a `Claimed`
    /// player's state.
    #[inline]
    pub fn release_claim(&self) {
        self.claimed_by.store(0, Ordering::Relaxed);
        self.claim_timestamp.store(0, Ordering::Relaxed);
        // Release ordering: ensure the cleared claim fields are visible
        // before the state transition is observed by other threads.
        self.state
            .compare_exchange(
                player_state::CLAIMED,
                player_state::WAITING,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .ok(); // Ignore failure — Reaper may have already reset this player
    }

    /// Transition from `Claimed` → `Matched` and record the match timestamp.
    ///
    /// Called by the owning worker after successful team balancing.
    /// Sets `matched_at` under the Mutex, then transitions state.
    /// This always succeeds — only the owning worker calls it.
    pub fn mark_matched(&self) {
        let now = Instant::now();
        {
            let mut guard = self
                .matched_at
                .lock()
                .expect("matched_at mutex is never poisoned");
            *guard = Some(now);
        }

        self.state
            .compare_exchange(
                player_state::CLAIMED,
                player_state::MATCHED,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .ok();
    }

    /// Attempt to evict (cancel) this player from the queue.
    ///
    /// Only succeeds if the player is currently in `Waiting` state.
    /// Returns `true` if successfully evicted, `false` if the player is
    /// currently `Claimed` (mid-match) or already `Matched`/`Evicted`.
    #[inline]
    pub fn try_evict(&self) -> bool {
        self.state
            .compare_exchange(
                player_state::WAITING,
                player_state::EVICTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Returns the elapsed wait time in milliseconds since the player joined.
    #[inline]
    pub fn wait_ms(&self) -> u64 {
        self.join_timestamp.elapsed().as_millis() as u64
    }

    /// Returns the elapsed wait time in milliseconds at the moment of matching.
    ///
    /// Returns `None` if the player has not been matched yet.
    pub fn matched_wait_ms(&self) -> Option<u64> {
        let guard = self
            .matched_at
            .lock()
            .expect("matched_at mutex is never poisoned");
        guard.map(|matched| matched.duration_since(self.join_timestamp).as_millis() as u64)
    }
}

impl std::fmt::Debug for Player {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Player")
            .field("id", &self.id)
            .field("skill_rating", &self.skill_rating)
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("claimed_by", &self.claimed_by.load(Ordering::Relaxed))
            .field("wait_ms", &self.join_timestamp.elapsed().as_millis())
            .finish()
    }
}

unsafe impl Send for Player {}
unsafe impl Sync for Player {}

//  PlayerSnapshot

/// An immutable snapshot of a player's data at the moment of match formation.
///
/// Stored in [`Match`] records. Using a snapshot rather than `Arc<Player>`
/// in match history prevents the `Arc` reference from keeping player memory
/// alive after the player has been evicted from the pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSnapshot {
    /// Player's unique identifier.
    pub id: Uuid,
    /// Player's skill rating at time of match.
    pub skill_rating: u32,
    /// How long the player waited before being matched (milliseconds).
    pub wait_ms: u64,
}

impl PlayerSnapshot {
    /// Capture a snapshot of a player at the moment of match formation.
    pub fn capture(player: &Player) -> Self {
        Self {
            id: player.id,
            skill_rating: player.skill_rating,
            wait_ms: player.matched_wait_ms().unwrap_or_else(|| player.wait_ms()),
        }
    }
}

//  Team

/// One side of a formed match — five players.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    /// The five players on this team.
    pub players: Vec<PlayerSnapshot>,
    /// Sum of all players' skill ratings.
    pub total_rating: u32,
    /// Average skill rating across all five players.
    pub avg_rating: f32,
}

impl Team {
    /// Construct a team from five player snapshots.
    pub fn new(players: Vec<PlayerSnapshot>) -> Self {
        debug_assert_eq!(players.len(), 5, "A team must have exactly 5 players");
        let total_rating: u32 = players.iter().map(|p| p.skill_rating).sum();
        let avg_rating = total_rating as f32 / players.len() as f32;
        Self {
            players,
            total_rating,
            avg_rating,
        }
    }
}

//  Match

/// A completed match record — two teams of five players.
///
/// Immutable after construction. Stored in the bounded match history ring
/// inside [`MatchmakerCore`] and returned by `GET /matches`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Match {
    /// Unique identifier for this match.
    pub match_id: Uuid,

    /// Team A — five players.
    pub team_a: Team,

    /// Team B — five players.
    pub team_b: Team,

    /// Difference in total skill rating between the two teams.
    /// Lower is better — `0` means perfectly balanced.
    pub team_delta: u32,

    /// Skill spread across all ten players: `max(mmr) - min(mmr)`.
    pub skill_spread: u32,

    /// Average wait time across all ten players (milliseconds).
    pub avg_wait_ms: u64,

    /// Maximum wait time across all ten players (milliseconds).
    pub max_wait_ms: u64,

    /// Unix timestamp (seconds) when the match was formed.
    /// Serialized as an integer for simplicity.
    pub formed_at_unix: u64,
}

impl Match {
    /// Construct a match record from two teams and computed quality metrics.
    pub fn new(
        match_id: Uuid,
        team_a: Team,
        team_b: Team,
        skill_spread: u32,
        avg_wait_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        let team_delta = team_a.total_rating.abs_diff(team_b.total_rating);
        let formed_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            match_id,
            team_a,
            team_b,
            team_delta,
            skill_spread,
            avg_wait_ms,
            max_wait_ms,
            formed_at_unix,
        }
    }
}

//  API request / response DTOs

/// Request body for `POST /enqueue`.
#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    /// The player's unique identifier (UUID v4).
    pub id: Uuid,
    /// The player's current skill rating. Valid range: 0–3000.
    pub skill_rating: u32,
}

/// Response body for `POST /enqueue`.
#[derive(Debug, Serialize)]
pub struct EnqueueResponse {
    pub player_id: Uuid,
    pub status: &'static str,
    /// Current queue depth (advisory — not a position guarantee).
    pub queue_depth: usize,
}

/// Response body for `GET /health`.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub players_waiting: i64,
    pub uptime_secs: u64,
}

/// Response body for `GET /matches`.
#[derive(Debug, Serialize)]
pub struct MatchesResponse {
    pub matches: Vec<Match>,
    pub total_matches_formed: u64,
}

/// Unified error response body for all `4xx`/`5xx` responses.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: code.into(),
        }
    }
}

//  Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_player(skill_rating: u32) -> Arc<Player> {
        Arc::new(Player::new(Uuid::new_v4(), skill_rating))
    }

    #[test]
    fn test_player_initial_state_is_waiting() {
        let p = make_player(1000);
        assert_eq!(p.state(), player_state::WAITING);
        assert!(p.is_waiting());
    }

    #[test]
    fn test_try_claim_succeeds_from_waiting() {
        let p = make_player(1000);
        assert!(p.try_claim(1, 12345));
        assert_eq!(p.state(), player_state::CLAIMED);
        assert_eq!(p.claimed_by.load(Ordering::Relaxed), 1);
        assert_eq!(p.claim_timestamp.load(Ordering::Relaxed), 12345);
    }

    #[test]
    fn test_try_claim_fails_if_already_claimed() {
        let p = make_player(1000);
        assert!(p.try_claim(1, 100));
        // Second claim attempt must fail
        assert!(!p.try_claim(2, 200));
        // Original claimer's ID is preserved
        assert_eq!(p.claimed_by.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_release_claim_returns_to_waiting() {
        let p = make_player(1000);
        p.try_claim(1, 100);
        p.release_claim();
        assert_eq!(p.state(), player_state::WAITING);
        assert_eq!(p.claimed_by.load(Ordering::Relaxed), 0);
        assert_eq!(p.claim_timestamp.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_mark_matched_sets_state_and_timestamp() {
        let p = make_player(1000);
        p.try_claim(1, 100);
        p.mark_matched();
        assert_eq!(p.state(), player_state::MATCHED);
        assert!(p.matched_at.lock().unwrap().is_some());
    }

    #[test]
    fn test_try_evict_from_waiting() {
        let p = make_player(1000);
        assert!(p.try_evict());
        assert_eq!(p.state(), player_state::EVICTED);
    }

    #[test]
    fn test_try_evict_fails_when_claimed() {
        let p = make_player(1000);
        p.try_claim(1, 100);
        // Cannot evict a Claimed player
        assert!(!p.try_evict());
        assert_eq!(p.state(), player_state::CLAIMED);
    }

    #[test]
    fn test_player_snapshot_captures_data() {
        let p = make_player(1500);
        p.try_claim(1, 100);
        p.mark_matched();
        let snap = PlayerSnapshot::capture(&p);
        assert_eq!(snap.skill_rating, 1500);
    }

    #[test]
    fn test_team_computes_totals() {
        let players: Vec<PlayerSnapshot> = (0..5)
            .map(|i| PlayerSnapshot {
                id: Uuid::new_v4(),
                skill_rating: 1000 + i * 10,
                wait_ms: 1000,
            })
            .collect();
        let team = Team::new(players);
        assert_eq!(team.total_rating, 5100); // 1000+1010+1020+1030+1040
        assert!((team.avg_rating - 1020.0).abs() < 0.01);
    }

    #[test]
    fn test_concurrent_claim_only_one_winner() {
        use std::sync::atomic::AtomicUsize;

        let p = Arc::new(Player::new(Uuid::new_v4(), 1000));
        let wins = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for worker_id in 1u64..=16 {
            let p = Arc::clone(&p);
            let wins = Arc::clone(&wins);
            handles.push(std::thread::spawn(move || {
                if p.try_claim(worker_id, 0) {
                    wins.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Exactly one worker must have won the claim
        assert_eq!(wins.load(Ordering::Relaxed), 1);
        assert_eq!(p.state(), player_state::CLAIMED);
    }
}
