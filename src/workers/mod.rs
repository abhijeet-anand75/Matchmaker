//! Worker pool and Reaper task management.
//!
//! This module is responsible for spawning and managing the two categories
//! of background tasks that drive the matchmaking engine:
//!
//! ## Matchmaking Workers
//!
//! A fixed pool of `config.worker_count` Tokio tasks. Each worker runs an
//! event loop that wakes on:
//! - A [`tokio::sync::Notify`] signal (fired on every player enqueue)
//! - A periodic [`tokio::time::interval`] fallback (for sparse pools)
//! - A [`tokio_util::sync::CancellationToken`] shutdown signal
//!
//! On each wake, the worker calls [`attempt_match`] and handles the result.
//! Workers maintain their own [`WorkerState`] for seed failure tracking —
//! this state is not shared across workers.
//!
//! ## Reaper Task
//!
//! A single background task that runs every `REAPER_INTERVAL_MS` milliseconds.
//! It scans all players in the pool for those stuck in `Claimed` state for
//! longer than `config.stale_claim_timeout_ms`. Stale claims are reset to
//! `Waiting` via CAS, allowing those players to be re-matched.
//!
//! The Reaper is the recovery mechanism for worker panics or task cancellations
//! that occur after claiming players but before completing match formation.
//!
//! ## Shutdown
//!
//! Both workers and the Reaper observe the same [`CancellationToken`].
//! On cancellation:
//! - Workers finish their current `attempt_match` call (µs-scale) then exit.
//! - The Reaper finishes its current scan then exits.
//! - The [`tokio::task::JoinSet`] returned to `main.rs` allows the caller
//!   to await all tasks with a timeout.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::engine::matcher::{attempt_match, unix_ms, MatchAttemptResult, WorkerContext, WorkerState};
use crate::engine::MatchmakerCore;
use crate::metrics::Metrics;
use crate::models::player_state;

// ── Constants ─────────────────────────────────────────────────────────────────

/// How often the Reaper task scans for stale claims (milliseconds).
const REAPER_INTERVAL_MS: u64 = 1_000;

// ── Worker pool ───────────────────────────────────────────────────────────────

/// Spawn all matchmaking workers and the Reaper task.
///
/// Returns a [`JoinSet`] containing all spawned task handles.
/// The caller (`main.rs`) owns the `JoinSet` and is responsible for
/// joining tasks on shutdown.
///
/// # Arguments
///
/// * `core` — shared engine state, cloned for each worker
/// * `shutdown` — cancellation token; cancel to stop all tasks
///
/// # Worker Numbering
///
/// Workers are numbered 1..=`config.worker_count`. Worker ID `0` is reserved
/// for the Reaper task. Worker IDs appear in logs and in `Player.claimed_by`
/// for debugging stale claims.
pub fn spawn_all(
    core: Arc<MatchmakerCore>,
    shutdown: CancellationToken,
) -> JoinSet<()> {
    let mut set = JoinSet::new();

    let worker_count = core.config().worker_count;
    let tick_ms = core.config().worker_tick_ms;

    // Spawn matchmaking workers
    for worker_id in 1..=(worker_count as u64) {
        let ctx = core.make_worker_context(worker_id);
        let notify = core.notify();
        let token = shutdown.clone();

        set.spawn(async move {
            run_worker(worker_id, ctx, notify, tick_ms, token).await;
        });

        info!(worker_id, "Matchmaking worker spawned");
    }

    // Spawn the single Reaper task (worker_id = 0)
    {
        let pool = core.pool();
        let metrics = core.metrics();
        let stale_timeout_ms = core.config().stale_claim_timeout_ms;
        let token = shutdown.clone();

        set.spawn(async move {
            run_reaper(pool, metrics, stale_timeout_ms, token).await;
        });

        info!("Reaper task spawned");
    }

    set
}

// ── Worker event loop ─────────────────────────────────────────────────────────

/// The event loop for a single matchmaking worker.
///
/// Runs until the `shutdown` token is cancelled.
///
/// # Wake sources
///
/// 1. `notify.notified()` — triggered on every player enqueue
/// 2. `tick` — periodic fallback (every `tick_ms`)
/// 3. `shutdown.cancelled()` — clean exit
///
/// On each wake (sources 1 or 2), the worker calls [`attempt_match`] once.
/// The result is logged and metrics are updated by the matcher itself.
///
/// # Backoff
///
/// Workers do not implement exponential backoff. The `tick_ms` interval
/// provides natural backpressure in sparse pools. In dense pools, the
/// `Notify` signal fires rapidly and workers match players as fast as the
/// pool allows. This is the correct behaviour — no artificial delay is needed.
async fn run_worker(
    worker_id: u64,
    ctx: WorkerContext,
    notify: Arc<tokio::sync::Notify>,
    tick_ms: u64,
    shutdown: CancellationToken,
) {
    let mut state = WorkerState::new();
    let mut tick = interval(Duration::from_millis(tick_ms));

    // Consume the first tick immediately — interval fires on creation.
    tick.tick().await;

    info!(worker_id, "Worker started");

    loop {
        // Wait for the next event: notify signal, tick, or shutdown.
        tokio::select! {
            biased;

            // Shutdown takes highest priority — check it first.
            _ = shutdown.cancelled() => {
                info!(worker_id, "Worker received shutdown signal — exiting");
                break;
            }

            // Notify signal: a new player was enqueued — attempt immediately.
            _ = notify.notified() => {
                debug!(worker_id, "Worker woke on notify");
            }

            // Periodic fallback: attempt even without a notify signal.
            // Catches edge cases where a player is waiting but no new
            // enqueue fires (e.g. pool has enough players but no recent arrivals).
            _ = tick.tick() => {
                debug!(worker_id, "Worker woke on tick");
            }
        }

        // Perform one match attempt. This call is synchronous CPU work
        // (~10–50µs). It does not block the Tokio runtime because:
        // - It holds no async locks
        // - It completes in microseconds
        // - Tokio's work-stealing scheduler handles the brief CPU burst
        match attempt_match(&ctx, &mut state) {
            MatchAttemptResult::Success(m) => {
                // Match was formed and logged inside attempt_match.
                // Nothing additional to do here.
                debug!(
                    worker_id,
                    match_id = %m.match_id,
                    "Worker completed match formation"
                );
            }

            MatchAttemptResult::PoolEmpty => {
                // Normal condition in sparse pools.
                // Worker will sleep until next notify or tick.
                debug!(worker_id, "Worker found pool empty");
            }

            MatchAttemptResult::InsufficientCandidates {
                found,
                window,
                seed_mmr,
                stage,
            } => {
                // Normal condition — not enough compatible players yet.
                debug!(
                    worker_id,
                    found,
                    window,
                    seed_mmr,
                    stage,
                    "Insufficient candidates for match"
                );
            }

            MatchAttemptResult::ClaimFailed { claimed, needed } => {
                // CAS contention — another worker claimed the same players.
                // All claims have been rolled back by attempt_match.
                debug!(
                    worker_id,
                    claimed,
                    needed,
                    "Claim failed — rolled back, will retry"
                );
            }
        }
    }

    info!(worker_id, "Worker exited cleanly");
}

// ── Reaper task ───────────────────────────────────────────────────────────────

/// The Reaper background task — worker crash recovery.
///
/// Runs every [`REAPER_INTERVAL_MS`] milliseconds.
///
/// Scans all players in the pool. For any player in `Claimed` state with a
/// `claim_timestamp` older than `stale_claim_timeout_ms`, attempts a CAS
/// reset to `Waiting`.
///
/// # Why CAS and not Store?
///
/// A healthy worker may complete its match formation just as the Reaper
/// fires. Using `compare_exchange(CLAIMED, WAITING)` ensures that:
/// - If the worker transitions Claimed → Matched first, the Reaper's CAS
///   fails safely (state is no longer CLAIMED).
/// - If the Reaper resets first, the worker's subsequent CAS (Claimed → Matched)
///   also fails safely, causing the worker to roll back.
///
/// This is the correct interleaving — no player ends up in an inconsistent state.
///
/// # Stale Detection
///
/// A claim is considered stale if:
/// `now_ms - player.claim_timestamp > stale_claim_timeout_ms`
///
/// With default settings (`STALE_CLAIM_TIMEOUT_MS=500`, `WORKER_TICK_MS=50`),
/// a healthy worker completes match formation in ~1ms — well within the 500ms
/// window. Only truly stuck claims (from crashed workers) are reset.
async fn run_reaper(
    pool: Arc<crate::engine::bucket::PlayerPool>,
    metrics: Arc<Metrics>,
    stale_claim_timeout_ms: u64,
    shutdown: CancellationToken,
) {
    let mut tick = interval(Duration::from_millis(REAPER_INTERVAL_MS));

    // Consume immediate first tick.
    tick.tick().await;

    info!("Reaper started (interval={}ms, stale_timeout={}ms)",
        REAPER_INTERVAL_MS, stale_claim_timeout_ms);

    loop {
        tokio::select! {
            biased;

            _ = shutdown.cancelled() => {
                info!("Reaper received shutdown signal — exiting");
                break;
            }

            _ = tick.tick() => {
                reaper_tick(&pool, &metrics, stale_claim_timeout_ms).await;
            }
        }
    }

    info!("Reaper exited cleanly");
}

/// Perform one Reaper scan — check all players for stale claims.
///
/// This is `async` to allow yielding between player checks if the pool
/// is very large, preventing the Reaper from monopolising the Tokio thread.
/// In practice, with pools < 100K players, the scan completes fast enough
/// that yielding is not needed. The `async` boundary is kept for correctness.
async fn reaper_tick(
    pool: &Arc<crate::engine::bucket::PlayerPool>,
    metrics: &Arc<Metrics>,
    stale_claim_timeout_ms: u64,
) {
    let now_ms = unix_ms();
    let all_players = pool.all_players();
    let mut recovered = 0u32;

    for player in &all_players {
        // Fast path: skip non-Claimed players without loading claim_timestamp.
        if player.state() != player_state::CLAIMED {
            continue;
        }

        let claim_ts = player
            .claim_timestamp
            .load(std::sync::atomic::Ordering::Acquire);

        // `claim_ts == 0` means the player was just claimed and the timestamp
        // hasn't been written yet (extremely rare race). Skip — the next
        // Reaper tick will catch it if it persists.
        if claim_ts == 0 {
            continue;
        }

        let claim_age_ms = now_ms.saturating_sub(claim_ts);

        if claim_age_ms > stale_claim_timeout_ms {
            let worker_id = player
                .claimed_by
                .load(std::sync::atomic::Ordering::Relaxed);

            // Attempt CAS: Claimed → Waiting.
            // This is safe even if the worker completes normally — see module docs.
            match player.state.compare_exchange(
                player_state::CLAIMED,
                player_state::WAITING,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully reset stale claim.
                    player
                        .claimed_by
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                    player
                        .claim_timestamp
                        .store(0, std::sync::atomic::Ordering::Relaxed);

                    recovered += 1;

                    warn!(
                        player_id = %player.id,
                        skill_rating = player.skill_rating,
                        claim_age_ms = claim_age_ms,
                        stuck_worker_id = worker_id,
                        "Reaper recovered stale claim — player reset to Waiting"
                    );
                }
                Err(actual) => {
                    // State changed between our load and CAS — healthy worker
                    // completed the match. This is the expected race; ignore.
                    debug!(
                        player_id = %player.id,
                        actual_state = actual,
                        "Reaper CAS failed — player state changed (worker completed normally)"
                    );
                }
            }
        }
    }

    if recovered > 0 {
        metrics
            .total_stale_claims_recovered
            .fetch_add(recovered as u64, std::sync::atomic::Ordering::Relaxed);

        warn!(recovered, "Reaper recovered stale claims this tick");
    } else {
        debug!("Reaper tick complete — no stale claims found");
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use uuid::Uuid;

    use tokio::time::sleep;

    use crate::config::Config;
    use crate::engine::bucket::PlayerPool;
    use crate::metrics::Metrics;
    use crate::models::Player;

    fn clear_env() {
        for var in &[
            "SERVER_PORT", "WORKER_COUNT", "WORKER_TICK_MS",
            "STALE_CLAIM_TIMEOUT_MS",
            "RELAXATION_STAGE_1_MS", "RELAXATION_STAGE_2_MS",
            "RELAXATION_STAGE_3_MS", "RELAXATION_STAGE_4_MS",
            "RELAXATION_STAGE_1_DELTA", "RELAXATION_STAGE_2_DELTA",
            "RELAXATION_STAGE_3_DELTA", "RELAXATION_STAGE_4_DELTA",
            "RELAXATION_STAGE_5_DELTA",
        ] {
            std::env::remove_var(var);
        }
    }

    fn make_pool_and_metrics() -> (Arc<PlayerPool>, Arc<Metrics>) {
        (Arc::new(PlayerPool::new()), Arc::new(Metrics::new()))
    }

    fn enqueue_player(pool: &PlayerPool, skill_rating: u32) -> Arc<Player> {
        let player = Arc::new(Player::new(Uuid::new_v4(), skill_rating));
        pool.insert(Arc::clone(&player));
        player
    }

    #[tokio::test]
    async fn test_reaper_recovers_stale_claim() {
        let (pool, metrics) = make_pool_and_metrics();

        // Create a player and claim it with a timestamp in the far past
        let player = enqueue_player(&pool, 1000);
        player.try_claim(1, 1); // claim_timestamp = 1ms (effectively ancient)

        // Run reaper with a 100ms timeout — claim is 1ms old, well past threshold
        reaper_tick(&pool, &metrics, 100).await;

        assert_eq!(
            player.state(),
            player_state::WAITING,
            "Reaper must reset stale claim to Waiting"
        );
        assert_eq!(
            player.claimed_by.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            player.claim_timestamp.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics.total_stale_claims_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn test_reaper_does_not_touch_fresh_claims() {
        let (pool, metrics) = make_pool_and_metrics();

        let player = enqueue_player(&pool, 1000);
        // Claim with current timestamp — not stale
        player.try_claim(1, unix_ms());

        reaper_tick(&pool, &metrics, 500).await;

        // Should still be Claimed — not stale
        assert_eq!(
            player.state(),
            player_state::CLAIMED,
            "Reaper must not touch fresh claims"
        );
        assert_eq!(
            metrics.total_stale_claims_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_reaper_skips_waiting_players() {
        let (pool, metrics) = make_pool_and_metrics();
        let player = enqueue_player(&pool, 1000);

        // Player is Waiting — Reaper should ignore it
        reaper_tick(&pool, &metrics, 100).await;

        assert_eq!(player.state(), player_state::WAITING);
        assert_eq!(
            metrics.total_stale_claims_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_reaper_skips_matched_players() {
        let (pool, metrics) = make_pool_and_metrics();
        let player = enqueue_player(&pool, 1000);

        // Simulate matched state
        player.try_claim(1, 1);
        player.mark_matched();

        reaper_tick(&pool, &metrics, 100).await;

        assert_eq!(player.state(), player_state::MATCHED);
        assert_eq!(
            metrics.total_stale_claims_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_reaper_recovers_multiple_stale_claims() {
        let (pool, metrics) = make_pool_and_metrics();

        // Create 5 players all with stale claims
        for _ in 0..5 {
            let player = enqueue_player(&pool, 1000);
            player.try_claim(1, 1);
        }

        reaper_tick(&pool, &metrics, 100).await;

        assert_eq!(
            metrics.total_stale_claims_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            5,
            "Reaper must recover all stale claims"
        );

        // All players back to Waiting
        for p in pool.all_players() {
            assert_eq!(p.state(), player_state::WAITING);
        }
    }

    #[tokio::test]
    async fn test_spawn_all_returns_non_empty_joinset() {
        clear_env();
        std::env::set_var("WORKER_COUNT", "2");
        std::env::set_var("WORKER_TICK_MS", "100");
        std::env::set_var("STALE_CLAIM_TIMEOUT_MS", "500");

        let config = Arc::new(Config::from_env().unwrap());
        let metrics = Arc::new(Metrics::new());
        let core = Arc::new(crate::engine::MatchmakerCore::new(
            Arc::clone(&config),
            Arc::clone(&metrics),
        ));

        let shutdown = CancellationToken::new();
        let mut set = spawn_all(Arc::clone(&core), shutdown.clone());

        // Give tasks a moment to start
        sleep(Duration::from_millis(50)).await;

        // Cancel and drain
        shutdown.cancel();

        let mut count = 0;
        while set.join_next().await.is_some() {
            count += 1;
        }

        // 2 workers + 1 reaper = 3 tasks
        assert_eq!(count, 3, "Expected 2 workers + 1 reaper = 3 tasks");

        clear_env();
    }

    #[tokio::test]
    async fn test_workers_form_matches_and_stop_cleanly() {
        clear_env();
        std::env::set_var("WORKER_COUNT", "2");
        std::env::set_var("WORKER_TICK_MS", "20");
        std::env::set_var("STALE_CLAIM_TIMEOUT_MS", "500");

        let config = Arc::new(Config::from_env().unwrap());
        let metrics = Arc::new(Metrics::new());
        let core = Arc::new(crate::engine::MatchmakerCore::new(
            Arc::clone(&config),
            Arc::clone(&metrics),
        ));

        // Enqueue exactly 10 players — should form 1 match
        for i in 0..10 {
            core.enqueue(Uuid::new_v4(), 1000 + i).unwrap();
        }

        let shutdown = CancellationToken::new();
        let mut set = spawn_all(Arc::clone(&core), shutdown.clone());

        // Wait for match to form
        sleep(Duration::from_millis(200)).await;

        shutdown.cancel();
        while set.join_next().await.is_some() {}

        let snapshot = core.metrics_snapshot();
        assert_eq!(
            snapshot.total_matches_created, 1,
            "One match must have been formed"
        );
        assert_eq!(
            snapshot.total_players_matched, 10,
            "All 10 players must have been matched"
        );

        clear_env();
    }
}