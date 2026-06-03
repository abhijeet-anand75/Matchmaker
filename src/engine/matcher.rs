//! Core match formation logic — one complete attempt cycle.
//!
//! [`attempt_match`] is the function called by every worker on each wake.
//! It implements the full matchmaking pipeline:
//!
//! ```text
//! Seed Selection
//!   → Candidate Discovery
//!     → Atomic Claiming
//!       → Team Balancing
//!         → Match Creation
//!           → Metrics Update
//! ```
//!
//! # Correctness Guarantee
//!
//! The atomic CAS claiming protocol ensures that no player can appear in
//! two simultaneous match attempts. Every claim is exclusive — exactly one
//! worker wins per player. If a worker cannot claim 10 players, it rolls
//! back all partial claims before returning.
//!
//! # Failure Modes
//!
//! - [`MatchAttemptResult::PoolEmpty`]: No Waiting players exist. Worker sleeps.
//! - [`MatchAttemptResult::InsufficientCandidates`]: Fewer than 10 compatible
//!   players found. Seed's failure count incremented. Worker sleeps.
//! - [`MatchAttemptResult::ClaimFailed`]: Enough candidates found but workers
//!   raced and this worker couldn't secure 10. Claims rolled back. Worker sleeps.
//! - [`MatchAttemptResult::Success`]: Match formed. Metrics updated. Result stored.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::SystemTime;

use uuid::Uuid;

use crate::config::Config;
use crate::engine::balancer::{exhaustive_balance, MATCH_SIZE};
use crate::engine::bucket::PlayerPool;
use crate::engine::relaxation::scan_bounds;
use crate::metrics::Metrics;
use crate::models::{Match, Player, PlayerSnapshot, Team};

//  Worker context

/// All shared state a worker needs to perform a match attempt.
///
/// Constructed once per worker at spawn time. All fields are `Arc` references —
/// cloned cheaply from [`MatchmakerCore`]. The worker holds this for its
/// entire lifetime.
pub struct WorkerContext {
    /// Unique identifier for this worker (1-indexed).
    /// Stored in `Player.claimed_by` during active claims for Reaper attribution.
    pub worker_id: u64,

    /// Shared player pool — primary store and rating index.
    pub pool: Arc<PlayerPool>,

    /// Shared metrics counters — updated after every match attempt.
    pub metrics: Arc<Metrics>,

    /// System configuration — relaxation thresholds, timeouts, etc.
    pub config: Arc<Config>,

    /// Bounded match history — workers push completed matches here.
    /// The API layer reads this for `GET /matches`.
    pub match_history: Arc<RwLock<VecDeque<Match>>>,

    /// Maximum number of matches to retain in history.
    pub match_history_limit: usize,
}

//  Result types

/// The outcome of a single match attempt by one worker.
#[derive(Debug)]
pub enum MatchAttemptResult {
    /// A match was successfully formed.
    Success(Match),

    /// The pool contains no Waiting players. Worker should sleep.
    PoolEmpty,

    /// Fewer than 10 compatible players found within the current relaxation
    /// window. Seed's failure count has been incremented.
    InsufficientCandidates {
        found: usize,
        window: u32,
        seed_mmr: u32,
        stage: u8,
    },

    /// Enough candidates were found but CAS contention prevented this worker
    /// from claiming 10 players. All partial claims have been rolled back.
    ClaimFailed { claimed: usize, needed: usize },
}

//  Seed retry tracking

/// Per-worker mutable state for seed retry tracking.
///
/// Not shared across workers — each worker maintains its own failure counter
/// for the current seed. Reset on successful match formation.
pub struct WorkerState {
    /// Number of consecutive failed attempts for the current seed player.
    pub consecutive_failures: u32,
    /// ID of the player that has been failing repeatedly (for logging).
    pub current_seed_id: Option<Uuid>,
}

impl WorkerState {
    pub fn new() -> Self {
        Self {
            consecutive_failures: 0,
            current_seed_id: None,
        }
    }

    /// Reset failure tracking after a successful match.
    pub fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.current_seed_id = None;
    }

    /// Record a failure for a seed player.
    pub fn record_failure(&mut self, seed_id: Uuid) {
        if self.current_seed_id == Some(seed_id) {
            self.consecutive_failures += 1;
        } else {
            // New seed — reset counter
            self.consecutive_failures = 1;
            self.current_seed_id = Some(seed_id);
        }
    }
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::new()
    }
}

//  Constants

/// Number of consecutive failures before skipping to an alternate seed.
/// Prevents a single unsatisfiable player from monopolising all workers.
const SEED_RETRY_LIMIT: u32 = 3;

/// Match history capacity — maximum records retained in memory.
pub const MATCH_HISTORY_LIMIT: usize = 10_000;

//  Core function
/// Perform one complete match attempt.
///
/// This is the hot path — called by every worker on every wake cycle.
/// It must be fast on the failure paths (PoolEmpty, InsufficientCandidates)
/// because those are the common case in a sparse pool.
///
/// # Phases
///
/// 1. **Seed selection**: Find the oldest Waiting player. Apply throughput
///    guard if the seed has failed too many times.
/// 2. **Candidate discovery**: Scan the rating index for compatible players.
/// 3. **Atomic claiming**: CAS-claim up to 10 players. Roll back on failure.
/// 4. **Team balancing**: Exhaustive optimal split of the 10 claimed players.
/// 5. **Match creation**: Transition states, remove from pool, build record.
/// 6. **Metrics update**: Atomic counter updates — no locking.
pub fn attempt_match(ctx: &WorkerContext, state: &mut WorkerState) -> MatchAttemptResult {
    //  Phase 1: Seed Selection

    let seed = match select_seed(ctx, state) {
        Some(s) => s,
        None => return MatchAttemptResult::PoolEmpty,
    };

    //  Phase 2: Candidate Discovery

    let (min_rating, max_rating) = scan_bounds(seed.skill_rating, seed.join_timestamp, &ctx.config);

    let stage = crate::engine::relaxation::relaxation_stage(seed.join_timestamp, &ctx.config);
    let window = max_rating - min_rating;

    ctx.metrics
        .worker_cycles_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let candidates = ctx.pool.range_scan(min_rating, max_rating);

    if candidates.len() < MATCH_SIZE {
        state.record_failure(seed.id);
        ctx.metrics
            .match_attempts_insufficient
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::debug!(
            worker_id = ctx.worker_id,
            seed_id = %seed.id,
            seed_mmr = seed.skill_rating,
            stage = stage,
            window = window,
            found = candidates.len(),
            needed = MATCH_SIZE,
            "Insufficient candidates"
        );

        return MatchAttemptResult::InsufficientCandidates {
            found: candidates.len(),
            window,
            seed_mmr: seed.skill_rating,
            stage,
        };
    }

    //  Phase 3: Atomic Claiming

    let now_ms = unix_ms();
    let mut claimed: Vec<Arc<Player>> = Vec::with_capacity(MATCH_SIZE);

    for candidate in &candidates {
        if claimed.len() == MATCH_SIZE {
            break;
        }

        if candidate.try_claim(ctx.worker_id, now_ms) {
            claimed.push(Arc::clone(candidate));
        }
    }

    if claimed.len() < MATCH_SIZE {
        let claimed_count = claimed.len();
        for player in &claimed {
            player.release_claim();
        }

        state.record_failure(seed.id);
        ctx.metrics
            .match_attempts_claim_failed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tracing::debug!(
            worker_id = ctx.worker_id,
            seed_id = %seed.id,
            claimed = claimed_count,
            needed = MATCH_SIZE,
            "Claim failed — rolled back"
        );

        return MatchAttemptResult::ClaimFailed {
            claimed: claimed_count,
            needed: MATCH_SIZE,
        };
    }

    //  Phase 4: Team Balancing

    let balance = exhaustive_balance(&claimed);

    //  Phase 5: Match Creation

    for player in &claimed {
        player.mark_matched();
    }

    let wait_times_ms: Vec<u64> = claimed
        .iter()
        .map(|p| p.matched_wait_ms().unwrap_or_else(|| p.wait_ms()))
        .collect();

    let avg_wait_ms = wait_times_ms.iter().sum::<u64>() / wait_times_ms.len() as u64;
    let max_wait_ms = wait_times_ms.iter().copied().max().unwrap_or(0);

    let all_ratings: Vec<u32> = claimed.iter().map(|p| p.skill_rating).collect();
    let skill_spread =
        all_ratings.iter().max().unwrap_or(&0) - all_ratings.iter().min().unwrap_or(&0);

    // Build team snapshots.
    let team_a_snapshots: Vec<PlayerSnapshot> = balance
        .team_a
        .iter()
        .map(|p| PlayerSnapshot::capture(p))
        .collect();

    let team_b_snapshots: Vec<PlayerSnapshot> = balance
        .team_b
        .iter()
        .map(|p| PlayerSnapshot::capture(p))
        .collect();

    let team_a = Team::new(team_a_snapshots);
    let team_b = Team::new(team_b_snapshots);

    let match_id = Uuid::new_v4();
    let completed_match = Match::new(
        match_id,
        team_a,
        team_b,
        skill_spread,
        avg_wait_ms,
        max_wait_ms,
    );

    // Remove all 10 players from the pool.
    // This updates both DashMap and BTreeMap under brief write locks.
    for player in &claimed {
        ctx.pool.remove(&player.id);
    }

    // Push to bounded match history.
    {
        let mut history = ctx
            .match_history
            .write()
            .expect("match_history RwLock is never poisoned");

        history.push_back(completed_match.clone());

        // Evict oldest record if over capacity.
        while history.len() > ctx.match_history_limit {
            history.pop_front();
        }
    }

    //  Phase 6: Metrics Update

    update_metrics_on_success(&ctx.metrics, &completed_match, &wait_times_ms, skill_spread);

    state.reset();

    tracing::info!(
        worker_id = ctx.worker_id,
        match_id = %match_id,
        team_delta = completed_match.team_delta,
        skill_spread = skill_spread,
        avg_wait_ms = avg_wait_ms,
        max_wait_ms = max_wait_ms,
        stage = stage,
        "Match formed"
    );

    MatchAttemptResult::Success(completed_match)
}

//  Seed selection

/// Select the seed player for this match attempt.
///
/// Primary: oldest Waiting player globally (FIFO fairness).
/// Guard: if the primary seed has failed `SEED_RETRY_LIMIT` consecutive times,
/// skip to the oldest player outside their MMR range to prevent workers from
/// spinning indefinitely on one unsatisfiable player.
fn select_seed(ctx: &WorkerContext, state: &WorkerState) -> Option<Arc<Player>> {
    let primary_seed = ctx.pool.oldest_waiting()?;

    // Check if the primary seed has been failing repeatedly.
    if state.current_seed_id == Some(primary_seed.id)
        && state.consecutive_failures >= SEED_RETRY_LIMIT
    {
        let (exclude_min, exclude_max) = scan_bounds(
            primary_seed.skill_rating,
            primary_seed.join_timestamp,
            &ctx.config,
        );

        tracing::debug!(
            worker_id = ctx.worker_id,
            seed_id = %primary_seed.id,
            seed_mmr = primary_seed.skill_rating,
            failures = state.consecutive_failures,
            "Seed retry limit reached — skipping to alternate seed"
        );

        // Fall back to primary seed if no alternate exists.
        ctx.pool
            .oldest_waiting_excluding_range(exclude_min, exclude_max)
            .or(Some(primary_seed))
    } else {
        Some(primary_seed)
    }
}

//  Metrics helpers

/// Update all atomic metrics counters after a successful match.
///
/// All updates use `Relaxed` ordering — metrics are advisory and do not
/// require happens-before relationships with matchmaking operations.
fn update_metrics_on_success(
    metrics: &Metrics,
    completed_match: &Match,
    wait_times_ms: &[u64],
    skill_spread: u32,
) {
    use std::sync::atomic::Ordering::Relaxed;

    metrics.total_matches_created.fetch_add(1, Relaxed);
    metrics
        .total_players_matched
        .fetch_add(MATCH_SIZE as u64, Relaxed);

    // Decrement queue size by the number of players removed.
    metrics
        .current_queue_size
        .fetch_sub(MATCH_SIZE as i64, Relaxed);

    // Accumulate total wait time for rolling average computation.
    let total_wait: u64 = wait_times_ms.iter().sum();
    metrics.total_wait_time_ms.fetch_add(total_wait, Relaxed);

    // Accumulate skill spread and team delta for averages.
    metrics
        .skill_spread_sum
        .fetch_add(skill_spread as u64, Relaxed);
    metrics.skill_spread_count.fetch_add(1, Relaxed);

    metrics
        .team_delta_sum
        .fetch_add(completed_match.team_delta as u64, Relaxed);
    metrics.team_delta_count.fetch_add(1, Relaxed);
}

//  Utility

/// Returns the current Unix timestamp in milliseconds.
///
/// Used for `claim_timestamp` on player claiming.
/// Falls back to `0` if the system clock is before the Unix epoch (impossible
/// on any real system but required for correctness of the fallback path).
#[inline]
pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

//  Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, RwLock};
    use uuid::Uuid;

    use crate::engine::balancer::TEAM_SIZE;

    use crate::config::Config;
    use crate::engine::bucket::PlayerPool;
    use crate::metrics::Metrics;
    use crate::models::Player;

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

    fn make_ctx() -> (WorkerContext, Arc<PlayerPool>) {
        clear_env();
        let config = Arc::new(Config::from_env().unwrap());
        let pool = Arc::new(PlayerPool::new());
        let metrics = Arc::new(Metrics::new());
        let history = Arc::new(RwLock::new(VecDeque::new()));

        let ctx = WorkerContext {
            worker_id: 1,
            pool: Arc::clone(&pool),
            metrics,
            config,
            match_history: history,
            match_history_limit: MATCH_HISTORY_LIMIT,
        };

        (ctx, pool)
    }

    fn enqueue_players(pool: &PlayerPool, count: usize, base_rating: u32) {
        for i in 0..count {
            let player = Arc::new(Player::new(Uuid::new_v4(), base_rating + i as u32));
            pool.insert(player);
        }
    }

    #[test]
    fn test_pool_empty_returns_correct_result() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();
        let result = attempt_match(&ctx, &mut state);
        assert!(matches!(result, MatchAttemptResult::PoolEmpty));
    }

    #[test]
    fn test_insufficient_candidates_with_nine_players() {
        let (ctx, pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&pool, 9, 1000);

        let result = attempt_match(&ctx, &mut state);
        assert!(
            matches!(
                result,
                MatchAttemptResult::InsufficientCandidates { found: 9, .. }
            ),
            "Expected InsufficientCandidates, got {:?}",
            result
        );
    }

    #[test]
    fn test_successful_match_with_ten_players() {
        let (ctx, pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&pool, 10, 1000);

        let result = attempt_match(&ctx, &mut state);
        assert!(
            matches!(result, MatchAttemptResult::Success(_)),
            "Expected Success, got {:?}",
            result
        );

        // Pool must be empty after match
        assert_eq!(pool.len(), 0, "All players must be removed after match");
    }

    #[test]
    fn test_match_teams_have_correct_sizes() {
        let (ctx, pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&pool, 10, 1000);

        if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
            assert_eq!(m.team_a.players.len(), TEAM_SIZE);
            assert_eq!(m.team_b.players.len(), TEAM_SIZE);
        } else {
            panic!("Expected successful match");
        }
    }

    #[test]
    fn test_match_stored_in_history() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&ctx.pool, 10, 1000);
        let _ = attempt_match(&ctx, &mut state);

        let history = ctx.match_history.read().unwrap();
        assert_eq!(history.len(), 1, "Match must be stored in history");
    }

    #[test]
    fn test_metrics_updated_on_success() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&ctx.pool, 10, 1000);
        let _ = attempt_match(&ctx, &mut state);

        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(ctx.metrics.total_matches_created.load(Relaxed), 1);
        assert_eq!(ctx.metrics.total_players_matched.load(Relaxed), 10);
    }

    #[test]
    fn test_no_duplicate_players_across_teams() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();

        enqueue_players(&ctx.pool, 10, 1000);

        if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
            let a_ids: std::collections::HashSet<Uuid> =
                m.team_a.players.iter().map(|p| p.id).collect();
            let b_ids: std::collections::HashSet<Uuid> =
                m.team_b.players.iter().map(|p| p.id).collect();

            assert!(
                a_ids.is_disjoint(&b_ids),
                "No player may appear in both teams"
            );
        } else {
            panic!("Expected successful match");
        }
    }

    #[test]
    fn test_state_reset_after_success() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();
        state.consecutive_failures = 5;

        enqueue_players(&ctx.pool, 10, 1000);
        let _ = attempt_match(&ctx, &mut state);

        assert_eq!(
            state.consecutive_failures, 0,
            "Failure counter must reset after successful match"
        );
        assert!(state.current_seed_id.is_none());
    }

    #[test]
    fn test_multiple_sequential_matches() {
        let (ctx, _pool) = make_ctx();
        let mut state = WorkerState::new();

        // 30 players → should form 3 sequential matches
        enqueue_players(&ctx.pool, 30, 1000);

        let mut match_count = 0;
        for _ in 0..3 {
            if matches!(
                attempt_match(&ctx, &mut state),
                MatchAttemptResult::Success(_)
            ) {
                match_count += 1;
            }
        }

        assert_eq!(match_count, 3);
        assert_eq!(ctx.pool.len(), 0);
    }

    #[test]
    fn test_history_bounded_by_limit() {
        clear_env();
        let config = Arc::new(Config::from_env().unwrap());
        let pool = Arc::new(PlayerPool::new());
        let metrics = Arc::new(Metrics::new());
        let history = Arc::new(RwLock::new(VecDeque::new()));

        // Set a very small history limit
        let ctx = WorkerContext {
            worker_id: 1,
            pool: Arc::clone(&pool),
            metrics,
            config,
            match_history: Arc::clone(&history),
            match_history_limit: 2, // only keep 2 matches
        };

        let mut state = WorkerState::new();

        // Form 3 matches
        for _ in 0..3 {
            enqueue_players(&pool, 10, 1000);
            let _ = attempt_match(&ctx, &mut state);
        }

        let history_len = history.read().unwrap().len();
        assert_eq!(
            history_len, 2,
            "History must be bounded to match_history_limit"
        );
    }

    #[test]
    fn test_worker_state_failure_tracking() {
        let mut state = WorkerState::new();
        let id = Uuid::new_v4();

        state.record_failure(id);
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.current_seed_id, Some(id));

        state.record_failure(id);
        assert_eq!(state.consecutive_failures, 2);

        // Different seed resets counter
        let id2 = Uuid::new_v4();
        state.record_failure(id2);
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.current_seed_id, Some(id2));
    }

    #[test]
    fn test_unix_ms_is_reasonable() {
        let ts = unix_ms();

        assert!(
            ts > 1_704_067_200_000,
            "unix_ms must return a plausible timestamp"
        );
    }
}
