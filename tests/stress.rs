//! Stress tests — correctness under high concurrent load.
//!
//! These tests exercise the matchmaking engine with thousands of players
//! and multiple concurrent workers. Every test validates correctness first:
//! - No player appears in two matches (duplicate prevention)
//! - Pool drains completely and consistently
//! - Metrics remain accurate under load
//!
//! All tests are wrapped in tokio::time::timeout to prevent CI hangs.
//! A timeout failure produces a descriptive assertion message.
//!
//! Mark individual tests with #[ignore] to exclude from normal `cargo test`.
//! Run stress tests explicitly with:
//!   cargo test --test stress -- --include-ignored

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use matchmaker::engine::MatchmakerCore;
use matchmaker::metrics::Metrics;
use matchmaker::workers::spawn_all;

use common::{assert_no_duplicates, clear_env};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a `MatchmakerCore` configured for stress testing.
/// Uses more workers and a faster tick for throughput.
fn make_stress_core(worker_count: usize) -> Arc<MatchmakerCore> {
    clear_env();
    std::env::set_var("WORKER_COUNT", worker_count.to_string());
    std::env::set_var("WORKER_TICK_MS", "10");
    std::env::set_var("STALE_CLAIM_TIMEOUT_MS", "500");
    std::env::set_var("RELAXATION_STAGE_1_MS", "5000");
    std::env::set_var("RELAXATION_STAGE_2_MS", "15000");
    std::env::set_var("RELAXATION_STAGE_3_MS", "30000");
    std::env::set_var("RELAXATION_STAGE_4_MS", "60000");
    std::env::set_var("RELAXATION_STAGE_1_DELTA", "100");
    std::env::set_var("RELAXATION_STAGE_2_DELTA", "200");
    std::env::set_var("RELAXATION_STAGE_3_DELTA", "400");
    std::env::set_var("RELAXATION_STAGE_4_DELTA", "800");
    std::env::set_var("RELAXATION_STAGE_5_DELTA", "9999");

    let config = Arc::new(
        matchmaker::config::Config::from_env().expect("stress config must be valid"),
    );
    let metrics = Arc::new(Metrics::new());
    Arc::new(MatchmakerCore::new(config, metrics))
}

async fn wait_for_matches(
    core: &Arc<MatchmakerCore>,
    expected: u64,
    deadline: Duration,
) -> bool {
    timeout(deadline, async {
        loop {
            sleep(Duration::from_millis(50)).await;
            if core.metrics_snapshot().total_matches_created >= expected {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false)
}

async fn shutdown_workers(
    shutdown: CancellationToken,
    mut set: tokio::task::JoinSet<()>,
) {
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(5), async {
        while set.join_next().await.is_some() {}
    })
    .await;
}

// ── Stress Test 1: 100 players, 4 workers ─────────────────────────────────────

/// 100 players → 10 matches.
/// Validates: correct match count, zero duplicates, pool fully drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stress_100_players_4_workers() {
    let core = make_stress_core(4);
    let expected_matches = 10u64;

    // Enqueue 100 players concurrently
    let mut enqueue_handles = Vec::new();
    for i in 0..100u32 {
        let core = Arc::clone(&core);
        enqueue_handles.push(tokio::spawn(async move {
            core.enqueue(Uuid::new_v4(), 1000 + (i % 80))
                .expect("Enqueue must succeed");
        }));
    }
    for h in enqueue_handles {
        h.await.expect("Enqueue task must not panic");
    }

    assert_eq!(
        core.players_waiting(),
        100,
        "All 100 players must be in queue before workers start"
    );

    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let formed = wait_for_matches(&core, expected_matches, Duration::from_secs(15)).await;

    assert!(
        formed,
        "TIMEOUT: {expected_matches} matches must form within 15 seconds. \
         Formed: {}. Players waiting: {}",
        core.metrics_snapshot().total_matches_created,
        core.players_waiting()
    );

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_matches_created, expected_matches,
        "Must form exactly {expected_matches} matches"
    );
    assert_eq!(
        snapshot.total_players_matched, expected_matches * 10,
        "Must match exactly {} players",
        expected_matches * 10
    );
    assert_eq!(
        core.players_waiting(),
        0,
        "Pool must be fully drained after all matches"
    );

    // Duplicate check via match history
    let history = core.recent_matches(100);
    assert_eq!(history.len() as u64, expected_matches);
    assert_no_duplicates(&history);

    shutdown_workers(shutdown, worker_set).await;
    clear_env();
}

// ── Stress Test 2: 500 players, 8 workers ─────────────────────────────────────

/// 500 players → 50 matches under 8 workers.
/// Validates correctness at 5× the basic load.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_500_players_8_workers() {
    let core = make_stress_core(8);
    let expected_matches = 50u64;

    // Concurrent enqueue from 10 tasks × 50 players each
    let mut enqueue_handles = Vec::new();
    for batch in 0..10u32 {
        let core = Arc::clone(&core);
        enqueue_handles.push(tokio::spawn(async move {
            for j in 0..50u32 {
                let rating = 1000 + ((batch * 50 + j) % 150);
                core.enqueue(Uuid::new_v4(), rating)
                    .expect("Enqueue must succeed");
            }
        }));
    }
    for h in enqueue_handles {
        h.await.expect("Enqueue task must not panic");
    }

    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let formed = wait_for_matches(&core, expected_matches, Duration::from_secs(30)).await;

    assert!(
        formed,
        "TIMEOUT: {expected_matches} matches must form within 30 seconds. \
         Formed: {}. Players waiting: {}",
        core.metrics_snapshot().total_matches_created,
        core.players_waiting()
    );

    let snapshot = core.metrics_snapshot();
    assert_eq!(snapshot.total_matches_created, expected_matches);
    assert_eq!(snapshot.total_players_matched, expected_matches * 10);
    assert_eq!(core.players_waiting(), 0, "Pool must be fully drained");

    // Correctness: no duplicates
    let history = core.recent_matches(100);
    assert_no_duplicates(&history);

    // Invariant: matched = matches × 10
    assert_eq!(
        snapshot.total_players_matched,
        snapshot.total_matches_created * 10,
        "Invariant violated: total_players_matched != total_matches_created × 10"
    );

    shutdown_workers(shutdown, worker_set).await;
    clear_env();
}

// ── Stress Test 3: Concurrent enqueue during active matching ──────────────────

/// Players arrive continuously while workers are actively forming matches.
/// Simulates real steady-state operation.
/// Validates: no corruption, no duplicates, metrics consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_concurrent_enqueue_during_matching() {
    let core = make_stress_core(4);
    let total_players = 200u32;
    let expected_matches = 20u64;

    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    // Enqueue players in small batches with brief pauses
    // simulating a real arrival stream
    let mut enqueue_handles = Vec::new();
    for batch in 0..20u32 {
        let core = Arc::clone(&core);
        enqueue_handles.push(tokio::spawn(async move {
            // Small stagger between batches
            sleep(Duration::from_millis(batch as u64 * 5)).await;
            for j in 0..10u32 {
                let rating = 1000 + ((batch * 10 + j) % 100);
                core.enqueue(Uuid::new_v4(), rating)
                    .expect("Enqueue must succeed");
            }
        }));
    }

    for h in enqueue_handles {
        h.await.expect("Enqueue task must not panic");
    }

    let formed = wait_for_matches(&core, expected_matches, Duration::from_secs(20)).await;

    assert!(
        formed,
        "TIMEOUT: {expected_matches} matches must form within 20 seconds. \
         Formed: {}",
        core.metrics_snapshot().total_matches_created
    );

    assert_eq!(core.players_waiting(), 0, "Pool must drain completely");

    let snapshot = core.metrics_snapshot();
    assert_eq!(snapshot.total_players_enqueued, total_players as u64);
    assert_eq!(snapshot.total_matches_created, expected_matches);
    assert_eq!(snapshot.total_players_matched, expected_matches * 10);

    let history = core.recent_matches(100);
    assert_no_duplicates(&history);

    shutdown_workers(shutdown, worker_set).await;
    clear_env();
}

// ── Stress Test 4: Queue consistency after full drain ─────────────────────────

/// After the pool fully drains, the system must be in a consistent state:
/// - Queue depth = 0
/// - No stuck players in any non-terminal state
/// - Metrics are self-consistent
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stress_queue_consistency_after_full_drain() {
    let core = make_stress_core(4);

    for i in 0..50u32 {
        core.enqueue(Uuid::new_v4(), 1000 + (i % 60))
            .expect("Enqueue must succeed");
    }

    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let formed = wait_for_matches(&core, 5, Duration::from_secs(15)).await;
    assert!(formed, "TIMEOUT: 5 matches must form within 15 seconds");

    shutdown_workers(shutdown, worker_set).await;

    // Post-drain consistency checks
    let snapshot = core.metrics_snapshot();

    // Queue depth must be zero
    assert_eq!(
        snapshot.current_queue_size, 0,
        "Queue depth must be 0 after full drain"
    );

    // Internal pool must be empty
    assert!(
        core.pool().is_empty(),
        "PlayerPool must be empty after full drain"
    );

    // No player should be in Claimed state (no stuck claims)
    for player in core.pool().all_players() {
        let state = player.state();
        assert_ne!(
            state,
            matchmaker::models::player_state::CLAIMED,
            "No player must be stuck in Claimed state after drain"
        );
    }

    // Metrics invariant
    assert_eq!(
        snapshot.total_players_matched,
        snapshot.total_matches_created * 10,
        "Metrics invariant violated after drain"
    );

    clear_env();
}

// ── Stress Test 5: High worker contention on small pool ───────────────────────

/// 16 workers competing for exactly 10 players.
/// Most workers will fail with ClaimFailed or InsufficientCandidates.
/// Exactly 1 match must form. No panic. No corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_stress_high_contention_small_pool() {
    let core = make_stress_core(16);

    for i in 0..10u32 {
        core.enqueue(Uuid::new_v4(), 1000 + i * 3)
            .expect("Enqueue must succeed");
    }

    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let formed = wait_for_matches(&core, 1, Duration::from_secs(5)).await;

    assert!(
        formed,
        "TIMEOUT: Exactly 1 match must form under high contention"
    );

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_matches_created, 1,
        "Exactly 1 match must form — not 0, not 2"
    );
    assert_eq!(snapshot.total_players_matched, 10);
    assert_eq!(core.players_waiting(), 0);

    shutdown_workers(shutdown, worker_set).await;
    clear_env();
}

// ── Stress Test 6: 1000 players (marked ignore for CI) ───────────────────────

/// 1000 players → 100 matches.
/// This is the full-scale demonstration test for the simulation deliverable.
/// Marked #[ignore] — run explicitly with:
///   cargo test --test stress test_stress_1000_players -- --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "Long-running stress test — run explicitly with --ignored"]
async fn test_stress_1000_players_100_matches() {
    let core = make_stress_core(8);
    let expected_matches = 100u64;

    // Concurrent enqueue from 20 tasks × 50 players
    let mut enqueue_handles = Vec::new();
    for batch in 0..20u32 {
        let core = Arc::clone(&core);
        enqueue_handles.push(tokio::spawn(async move {
            for j in 0..50u32 {
                let rating = 500 + ((batch * 50 + j) % 2000);
                core.enqueue(Uuid::new_v4(), rating)
                    .expect("Enqueue must succeed");
            }
        }));
    }
    for h in enqueue_handles {
        h.await.expect("Enqueue task must not panic");
    }

    assert_eq!(core.players_waiting(), 1000);

    let start = std::time::Instant::now();
    let shutdown = CancellationToken::new();
    let worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let formed = wait_for_matches(&core, expected_matches, Duration::from_secs(30)).await;

    let elapsed = start.elapsed();

    assert!(
        formed,
        "TIMEOUT: {expected_matches} matches must form within 30 seconds. \
         Formed: {}. Elapsed: {:?}",
        core.metrics_snapshot().total_matches_created,
        elapsed
    );

    let snapshot = core.metrics_snapshot();
    assert_eq!(snapshot.total_matches_created, expected_matches);
    assert_eq!(snapshot.total_players_matched, expected_matches * 10);
    assert_eq!(core.players_waiting(), 0);

    // Correctness check
    let history = core.recent_matches(200);
    assert_no_duplicates(&history);

    let throughput = expected_matches as f64 / elapsed.as_secs_f64();

    println!(
        "\n=== Stress Test Results ===\n\
         Players:            1000\n\
         Matches formed:     {}\n\
         Players matched:    {}\n\
         Elapsed:            {:.2?}\n\
         Throughput:         {:.1} matches/sec\n\
         Avg wait ms:        {}\n\
         Avg skill spread:   {}\n\
         Avg team delta:     {}\n\
         Claim failures:     {}\n\
         Worker cycles:      {}\n\
         ===========================",
        snapshot.total_matches_created,
        snapshot.total_players_matched,
        elapsed,
        throughput,
        snapshot.avg_wait_ms,
        snapshot.avg_skill_spread,
        snapshot.avg_team_delta,
        snapshot.match_attempts_claim_failed,
        snapshot.worker_cycles_total,
    );

    shutdown_workers(shutdown, worker_set).await;
    clear_env();
}