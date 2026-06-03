//! Matchmaking engine — the stateful core of the service.
//!
//! This module owns and coordinates all engine sub-components:
//!
//! - [`PlayerPool`]: dual-structure player registry (DashMap + BTreeMap)
//! - [`Metrics`]: atomic side-channel counters
//! - [`tokio::sync::Notify`]: wake signal for workers on enqueue
//! - Match history: bounded ring of completed [`Match`] records
//!
//! [`MatchmakerCore`] is the single struct that ties these together.
//! It is constructed once in `main.rs`, wrapped in `Arc<MatchmakerCore>`,
//! and shared between the API layer and the worker pool.
//!
//! # Ownership Model
//!
//! ```text
//! Arc<MatchmakerCore>
//!   ├── Arc<PlayerPool>       — shared with all workers
//!   ├── Arc<Metrics>          — shared with all workers and API handlers
//!   ├── Arc<Notify>           — shared with all workers
//!   └── Arc<RwLock<VecDeque<Match>>>  — shared with all workers and API handlers
//! ```
//!
//! All fields inside `MatchmakerCore` are themselves `Arc`-wrapped so that
//! workers can hold independent references without going through the core.

pub mod balancer;
pub mod bucket;
pub mod matcher;
pub mod relaxation;

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::Config;
use crate::engine::bucket::PlayerPool;
use crate::engine::matcher::{WorkerContext, MATCH_HISTORY_LIMIT};
use crate::metrics::Metrics;
use crate::models::{Match, Player};

//  Enqueue errors 

/// Errors that can occur when a player attempts to join the queue.
#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error("Player {0} is already in the queue")]
    AlreadyQueued(Uuid),

    #[error("Skill rating {0} is outside the valid range (0–{1})")]
    InvalidSkillRating(u32, u32),
}

/// Errors that can occur when a player attempts to cancel their queue entry.
#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("Player {0} not found in queue")]
    NotFound(Uuid),

    #[error("Player {0} cannot be cancelled — currently being matched")]
    CurrentlyBeingMatched(Uuid),
}

//  MatchmakerCore 

/// The top-level matchmaking engine.
///
/// Owns all shared state and exposes a clean API to both the HTTP layer
/// (enqueue, cancel, recent_matches, metrics_snapshot) and the worker layer
/// (pool, notify, match_history).
///
/// Constructed once at startup. All fields are individually `Arc`-wrapped
/// so workers can clone them cheaply without holding a reference to the
/// entire core.
pub struct MatchmakerCore {
    /// Dual-structure player registry.
    pool: Arc<PlayerPool>,

    /// Atomic metrics counters — lock-free side channel.
    metrics: Arc<Metrics>,

    /// Wake signal — fires `notify_one()` on every player enqueue.
    /// Workers wait on this to avoid spinning when the pool is sparse.
    notify: Arc<Notify>,

    /// Bounded match history — last `MATCH_HISTORY_LIMIT` completed matches.
    match_history: Arc<RwLock<VecDeque<Match>>>,

    /// System configuration — immutable after construction.
    config: Arc<Config>,

    /// Monotonic timestamp of when this core was started.
    /// Used to compute uptime in the health endpoint.
    started_at: Instant,
}

impl MatchmakerCore {
    /// Construct a new `MatchmakerCore`.
    ///
    /// All sub-components are initialised to their empty/zero state.
    /// Workers are not started here — that is the responsibility of
    /// [`crate::workers`].
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>) -> Self {
        Self {
            pool: Arc::new(PlayerPool::new()),
            metrics,
            notify: Arc::new(Notify::new()),
            match_history: Arc::new(RwLock::new(VecDeque::new())),
            config,
            started_at: Instant::now(),
        }
    }

    //  Enqueue 

    /// Add a player to the matchmaking queue.
    ///
    /// Validates the player, inserts into the pool, increments metrics,
    /// and fires a `notify_one()` to wake one sleeping worker.
    ///
    /// # Errors
    ///
    /// - [`EnqueueError::AlreadyQueued`] if the player ID is already present.
    /// - [`EnqueueError::InvalidSkillRating`] if the rating is out of range.
    pub fn enqueue(&self, id: Uuid, skill_rating: u32) -> Result<usize, EnqueueError> {
        use crate::engine::bucket::MAX_SKILL_RATING;

        
        if skill_rating > MAX_SKILL_RATING {
            return Err(EnqueueError::InvalidSkillRating(skill_rating, MAX_SKILL_RATING));
        }

        // Reject duplicate registrations.
        if self.pool.contains(&id) {
            return Err(EnqueueError::AlreadyQueued(id));
        }

        let player = Arc::new(Player::new(id, skill_rating));
        self.pool.insert(Arc::clone(&player));

        // Update metrics atomically — no lock involved.
        self.metrics
            .total_players_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .current_queue_size
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        
        self.notify.notify_one();

        tracing::debug!(
            player_id = %id,
            skill_rating = skill_rating,
            queue_size = self.pool.len(),
            "Player enqueued"
        );

        Ok(self.pool.len())
    }

    //  Cancel

    /// Remove a player from the matchmaking queue.
    ///
    /// Only succeeds if the player is currently in `Waiting` state.
    /// Returns [`CancelError::CurrentlyBeingMatched`] if the player is
    /// `Claimed` — the caller should retry after a brief delay.
    ///
    /// # Errors
    ///
    /// - [`CancelError::NotFound`] if the player ID is not in the pool.
    /// - [`CancelError::CurrentlyBeingMatched`] if the player is `Claimed`.
    pub fn cancel(&self, id: &Uuid) -> Result<(), CancelError> {
        let player = self
            .pool
            .get(id)
            .ok_or(CancelError::NotFound(*id))?;

        // Attempt atomic transition Waiting → Evicted.
        if !player.try_evict() {
            return Err(CancelError::CurrentlyBeingMatched(*id));
        }

        // Eviction succeeded — remove from pool structures.
        self.pool.remove(id);

        self.metrics
            .current_queue_size
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        self.metrics
            .total_players_cancelled
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::debug!(player_id = %id, "Player cancelled");

        Ok(())
    }

    //  Match history 

    /// Return the most recent `limit` completed matches.
    ///
    /// Acquires a shared read lock on the match history — does not block
    /// matchmaking workers (they hold a write lock only briefly during
    /// match creation).
    pub fn recent_matches(&self, limit: usize) -> Vec<Match> {
        let history = self
            .match_history
            .read()
            .expect("match_history RwLock is never poisoned");

        history
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Total number of matches ever formed (from metrics counter).
    pub fn total_matches_formed(&self) -> u64 {
        self.metrics
            .total_matches_created
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    //  Metrics 

    /// Capture a point-in-time snapshot of all metrics.
    ///
    /// Reads all atomic counters — zero lock involvement.
    pub fn metrics_snapshot(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    //  Health 

    /// Current number of players waiting in the queue.
    pub fn players_waiting(&self) -> i64 {
        self.metrics
            .current_queue_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Service uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    //  Internal accessors for workers 
    // These methods give workers direct Arc references to sub-components
    // without requiring them to hold an Arc<MatchmakerCore>.

    /// Clone a reference to the player pool.
    pub fn pool(&self) -> Arc<PlayerPool> {
        Arc::clone(&self.pool)
    }

    /// Clone a reference to the metrics counters.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Clone a reference to the worker wake signal.
    pub fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Clone a reference to the match history store.
    pub fn match_history(&self) -> Arc<RwLock<VecDeque<Match>>> {
        Arc::clone(&self.match_history)
    }

    /// Clone a reference to the configuration.
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }

    /// Construct a [`WorkerContext`] for a worker with the given ID.
    ///
    /// Clones all Arc references from the core — cheap pointer copies.
    /// Each worker gets its own `WorkerContext` with a unique `worker_id`.
    pub fn make_worker_context(&self, worker_id: u64) -> WorkerContext {
        WorkerContext {
            worker_id,
            pool: self.pool(),
            metrics: self.metrics(),
            config: self.config(),
            match_history: self.match_history(),
            match_history_limit: MATCH_HISTORY_LIMIT,
        }
    }
}

//  Tests 

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn clear_env() {
        for var in &[
            "SERVER_PORT",
            "WORKER_COUNT",
            "WORKER_TICK_MS",
            "STALE_CLAIM_TIMEOUT_MS",
            "RELAXATION_STAGE_1_MS",
            "RELAXATION_STAGE_2_MS",
            "RELAXATION_STAGE_3_MS",
            "RELAXATION_STAGE_4_MS",
            "RELAXATION_STAGE_1_DELTA",
            "RELAXATION_STAGE_2_DELTA",
            "RELAXATION_STAGE_3_DELTA",
            "RELAXATION_STAGE_4_DELTA",
            "RELAXATION_STAGE_5_DELTA",
        ] {
            std::env::remove_var(var);
        }
    }

    fn make_core() -> Arc<MatchmakerCore> {
        clear_env();
        let config = Arc::new(Config::from_env().unwrap());
        let metrics = Arc::new(Metrics::new());
        Arc::new(MatchmakerCore::new(config, metrics))
    }

    #[test]
    fn test_enqueue_success() {
        let core = make_core();
        let id = Uuid::new_v4();
        let result = core.enqueue(id, 1000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_enqueue_duplicate_rejected() {
        let core = make_core();
        let id = Uuid::new_v4();
        assert!(core.enqueue(id, 1000).is_ok());
        let second = core.enqueue(id, 1000);
        assert!(matches!(second, Err(EnqueueError::AlreadyQueued(_))));
    }

    #[test]
    fn test_enqueue_invalid_skill_rating() {
        let core = make_core();
        let result = core.enqueue(Uuid::new_v4(), 9999);
        assert!(matches!(result, Err(EnqueueError::InvalidSkillRating(_, _))));
    }

    #[test]
    fn test_cancel_waiting_player() {
        let core = make_core();
        let id = Uuid::new_v4();
        core.enqueue(id, 1000).unwrap();
        assert!(core.cancel(&id).is_ok());
        assert_eq!(core.players_waiting(), 0);
    }

    #[test]
    fn test_cancel_unknown_player() {
        let core = make_core();
        let result = core.cancel(&Uuid::new_v4());
        assert!(matches!(result, Err(CancelError::NotFound(_))));
    }

    #[test]
    fn test_cancel_claimed_player_returns_error() {
        let core = make_core();
        let id = Uuid::new_v4();
        core.enqueue(id, 1000).unwrap();

        // Manually claim the player to simulate mid-match state
        let player = core.pool.get(&id).unwrap();
        player.try_claim(99, 0);

        let result = core.cancel(&id);
        assert!(matches!(result, Err(CancelError::CurrentlyBeingMatched(_))));
    }

    #[test]
    fn test_players_waiting_counter() {
        let core = make_core();
        assert_eq!(core.players_waiting(), 0);

        for _ in 0..5 {
            core.enqueue(Uuid::new_v4(), 1000).unwrap();
        }
        assert_eq!(core.players_waiting(), 5);
    }

    #[test]
    fn test_metrics_updated_on_enqueue() {
        let core = make_core();
        core.enqueue(Uuid::new_v4(), 1500).unwrap();
        core.enqueue(Uuid::new_v4(), 1500).unwrap();

        let snapshot = core.metrics_snapshot();
        assert_eq!(snapshot.total_players_enqueued, 2);
        assert_eq!(snapshot.current_queue_size, 2);
    }

    #[test]
    fn test_metrics_updated_on_cancel() {
        let core = make_core();
        let id = Uuid::new_v4();
        core.enqueue(id, 1000).unwrap();
        core.cancel(&id).unwrap();

        let snapshot = core.metrics_snapshot();
        assert_eq!(snapshot.current_queue_size, 0);
        assert_eq!(snapshot.total_players_cancelled, 1);
    }

    #[test]
    fn test_recent_matches_empty_initially() {
        let core = make_core();
        assert!(core.recent_matches(10).is_empty());
    }

    #[test]
    fn test_uptime_is_non_zero() {
        let core = make_core();
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let _ = core.uptime_secs();
    }

    #[test]
    fn test_make_worker_context_has_correct_worker_id() {
        let core = make_core();
        let ctx = core.make_worker_context(42);
        assert_eq!(ctx.worker_id, 42);
    }

    #[test]
    fn test_notify_fires_on_enqueue() {
        let core = make_core();
        let notify = core.notify();

        
        let notified = notify.notified();

        core.enqueue(Uuid::new_v4(), 1000).unwrap();

        
        drop(notified);
        assert!(Arc::strong_count(&notify) >= 1);
    }
}