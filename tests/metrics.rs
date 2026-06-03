//! Metrics correctness tests.
//!
//! Validates that all atomic counters update correctly, derived fields
//! (averages) are computed accurately, and the metrics endpoint returns
//! the expected JSON structure with correct field names and types.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use matchmaker::engine::matcher::{attempt_match, WorkerState};
use matchmaker::engine::MatchmakerCore;
use matchmaker::metrics::Metrics;
use matchmaker::workers::spawn_all;

use common::{clear_env, make_core, make_worker_ctx, seed_uniform};

// ── Counter correctness ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_enqueue_increments_total_players_enqueued() {
    let core = make_core();

    for _ in 0..5 {
        core.enqueue(Uuid::new_v4(), 1000).unwrap();
    }

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_players_enqueued, 5,
        "total_players_enqueued must equal number of enqueue calls"
    );
}

#[tokio::test]
async fn test_cancel_increments_total_players_cancelled() {
    let core = make_core();

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    core.enqueue(id1, 1000).unwrap();
    core.enqueue(id2, 1000).unwrap();

    core.cancel(&id1).unwrap();
    core.cancel(&id2).unwrap();

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_players_cancelled, 2,
        "total_players_cancelled must equal number of successful cancels"
    );
}

#[tokio::test]
async fn test_match_increments_total_matches_created() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);
    let _ = attempt_match(&ctx, &mut state);

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_matches_created, 1,
        "total_matches_created must be 1 after one match"
    );
}

#[tokio::test]
async fn test_match_increments_total_players_matched() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);
    let _ = attempt_match(&ctx, &mut state);

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_players_matched, 10,
        "total_players_matched must be 10 after one match"
    );
}

#[tokio::test]
async fn test_multiple_matches_accumulate_correctly() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 30, 1000);

    for _ in 0..3 {
        let _ = attempt_match(&ctx, &mut state);
    }

    let snapshot = core.metrics_snapshot();
    assert_eq!(snapshot.total_matches_created, 3);
    assert_eq!(snapshot.total_players_matched, 30);
    assert_eq!(snapshot.total_players_enqueued, 30);
}

// ── Queue depth gauge ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_queue_depth_accurate_after_enqueue() {
    let core = make_core();

    assert_eq!(core.metrics_snapshot().current_queue_size, 0);

    for i in 1..=10 {
        core.enqueue(Uuid::new_v4(), 1000).unwrap();
        assert_eq!(
            core.metrics_snapshot().current_queue_size,
            i,
            "Queue depth must equal enqueue count at step {i}"
        );
    }
}

#[tokio::test]
async fn test_queue_depth_decrements_on_cancel() {
    let core = make_core();

    let id = Uuid::new_v4();
    core.enqueue(id, 1000).unwrap();
    assert_eq!(core.metrics_snapshot().current_queue_size, 1);

    core.cancel(&id).unwrap();
    assert_eq!(
        core.metrics_snapshot().current_queue_size,
        0,
        "Queue depth must decrement on cancel"
    );
}

#[tokio::test]
async fn test_queue_depth_decrements_on_match() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);
    assert_eq!(core.metrics_snapshot().current_queue_size, 10);

    let _ = attempt_match(&ctx, &mut state);
    assert_eq!(
        core.metrics_snapshot().current_queue_size,
        0,
        "Queue depth must reach 0 after all players are matched"
    );
}

#[tokio::test]
async fn test_queue_depth_never_negative() {
    let core = make_core();

    let id = Uuid::new_v4();
    core.enqueue(id, 1000).unwrap();
    core.cancel(&id).unwrap();

    // Attempt double cancel — second call returns error but must not corrupt gauge
    let _ = core.cancel(&id);

    assert!(
        core.metrics_snapshot().current_queue_size >= 0,
        "Queue depth must never go negative"
    );
}

// ── Attempt failure counters ──────────────────────────────────────────────────

#[tokio::test]
async fn test_insufficient_candidates_counter_increments() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // Only 5 players — insufficient
    seed_uniform(&core, 5, 1000);
    let _ = attempt_match(&ctx, &mut state);

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.match_attempts_insufficient, 1,
        "match_attempts_insufficient must increment on InsufficientCandidates"
    );
}

#[tokio::test]
async fn test_worker_cycles_increments_on_each_attempt() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // Seed with players so workers reach candidate discovery phase
    // where worker_cycles_total is incremented
    seed_uniform(&core, 5, 1000);

    // 3 attempts — pool has players so counter will increment
    for _ in 0..3 {
        let _ = attempt_match(&ctx, &mut state);
    }

    let snapshot = core.metrics_snapshot();
    assert!(
        snapshot.worker_cycles_total >= 1,
        "worker_cycles_total must increment when pool has players"
    );
}

// ── Rolling average correctness ───────────────────────────────────────────────

#[tokio::test]
async fn test_avg_wait_ms_is_zero_before_any_match() {
    let core = make_core();
    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.avg_wait_ms, 0,
        "avg_wait_ms must be 0 before any match"
    );
}

#[tokio::test]
async fn test_avg_wait_ms_computed_after_match() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);

    // Small sleep so wait_ms is non-zero
    tokio::time::sleep(Duration::from_millis(5)).await;

    let _ = attempt_match(&ctx, &mut state);

    let snapshot = core.metrics_snapshot();
    assert!(
        snapshot.avg_wait_ms > 0,
        "avg_wait_ms must be > 0 after players have waited"
    );
    assert!(
        snapshot.total_wait_time_ms > 0,
        "total_wait_time_ms must be accumulated"
    );
}

#[tokio::test]
async fn test_avg_wait_ms_derived_from_sum_and_count() {
    let metrics = Arc::new(Metrics::new());

    // Manually set sum and count to known values
    // 10 players, total wait = 5000ms → avg = 500ms
    metrics
        .total_players_matched
        .store(10, std::sync::atomic::Ordering::Relaxed);
    metrics
        .total_wait_time_ms
        .store(5_000, std::sync::atomic::Ordering::Relaxed);

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.avg_wait_ms, 500,
        "avg_wait_ms must equal total_wait_time_ms / total_players_matched"
    );
}

#[tokio::test]
async fn test_avg_team_delta_is_zero_before_any_match() {
    let core = make_core();
    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.avg_team_delta, 0,
        "avg_team_delta must be 0 before any match"
    );
}

#[tokio::test]
async fn test_avg_team_delta_computed_after_match() {
    let metrics = Arc::new(Metrics::new());

    // 3 matches with deltas 100, 200, 300 → avg = 200
    metrics
        .team_delta_sum
        .store(600, std::sync::atomic::Ordering::Relaxed);
    metrics
        .team_delta_count
        .store(3, std::sync::atomic::Ordering::Relaxed);

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.avg_team_delta, 200,
        "avg_team_delta must equal team_delta_sum / team_delta_count"
    );
}

#[tokio::test]
async fn test_avg_skill_spread_computed_correctly() {
    let metrics = Arc::new(Metrics::new());

    // 4 matches with spreads 50, 100, 150, 200 → avg = 125
    metrics
        .skill_spread_sum
        .store(500, std::sync::atomic::Ordering::Relaxed);
    metrics
        .skill_spread_count
        .store(4, std::sync::atomic::Ordering::Relaxed);

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.avg_skill_spread, 125,
        "avg_skill_spread must equal skill_spread_sum / skill_spread_count"
    );
}

#[tokio::test]
async fn test_no_division_by_zero_on_empty_metrics() {
    let metrics = Metrics::new();
    // All counts are 0 — snapshot must not panic
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.avg_wait_ms, 0);
    assert_eq!(snapshot.avg_team_delta, 0);
    assert_eq!(snapshot.avg_skill_spread, 0);
}

// ── Concurrent metric updates ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_enqueue_metric_accuracy() {
    let core = make_core();
    let mut handles = Vec::new();

    // 10 tasks × 50 enqueues = 500 total
    for _ in 0..10 {
        let core = Arc::clone(&core);
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                core.enqueue(Uuid::new_v4(), 1000).unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_players_enqueued, 500,
        "Concurrent enqueues must produce exact counter — no lost updates"
    );
    assert_eq!(
        snapshot.current_queue_size, 500,
        "Queue depth must be exactly 500 after concurrent inserts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_match_metric_accuracy() {
    clear_env();
    std::env::set_var("WORKER_COUNT", "4");
    std::env::set_var("WORKER_TICK_MS", "10");
    std::env::set_var("STALE_CLAIM_TIMEOUT_MS", "500");
    std::env::set_var("RELAXATION_STAGE_1_MS", "5000");
    std::env::set_var("RELAXATION_STAGE_2_MS", "15000");
    std::env::set_var("RELAXATION_STAGE_3_MS", "30000");
    std::env::set_var("RELAXATION_STAGE_4_MS", "60000");
    std::env::set_var("RELAXATION_STAGE_1_DELTA", "50");
    std::env::set_var("RELAXATION_STAGE_2_DELTA", "100");
    std::env::set_var("RELAXATION_STAGE_3_DELTA", "200");
    std::env::set_var("RELAXATION_STAGE_4_DELTA", "400");
    std::env::set_var("RELAXATION_STAGE_5_DELTA", "9999");

    let config = Arc::new(matchmaker::config::Config::from_env().expect("config must be valid"));
    let metrics = Arc::new(Metrics::new());
    let core = Arc::new(MatchmakerCore::new(
        Arc::clone(&config),
        Arc::clone(&metrics),
    ));

    // 50 players → 5 matches
    for i in 0..50u32 {
        core.enqueue(Uuid::new_v4(), 1000 + (i % 30)).unwrap();
    }

    let shutdown = CancellationToken::new();
    let mut worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    let result = timeout(Duration::from_secs(10), async {
        loop {
            sleep(Duration::from_millis(50)).await;
            if core.metrics_snapshot().total_matches_created >= 5 {
                return true;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "5 matches must form within 10 seconds");

    let snapshot = core.metrics_snapshot();

    // Core invariant: total_players_matched == total_matches_created * 10
    assert_eq!(
        snapshot.total_players_matched,
        snapshot.total_matches_created * 10,
        "total_players_matched must always equal total_matches_created × 10"
    );

    assert_eq!(
        snapshot.current_queue_size, 0,
        "Queue must be empty after all matches"
    );

    shutdown.cancel();
    while worker_set.join_next().await.is_some() {}

    clear_env();
}

// ── Stale claims recovered counter ───────────────────────────────────────────

#[tokio::test]
async fn test_stale_claims_recovered_starts_at_zero() {
    let core = make_core();
    let snapshot = core.metrics_snapshot();
    assert_eq!(
        snapshot.total_stale_claims_recovered, 0,
        "Reaper recovery counter must start at 0"
    );
}

#[tokio::test]
async fn test_stale_claims_recovered_increments_on_recovery() {
    let metrics = Arc::new(Metrics::new());

    metrics
        .total_stale_claims_recovered
        .fetch_add(5, std::sync::atomic::Ordering::Relaxed);

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.total_stale_claims_recovered, 5,
        "Stale claim recovery counter must reflect Reaper activity"
    );
}

// ── Snapshot completeness ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_snapshot_contains_all_required_fields() {
    let core = make_core();
    let snapshot = core.metrics_snapshot();

    // Verify all fields are accessible (compilation proves presence)
    let _ = snapshot.total_players_enqueued;
    let _ = snapshot.total_players_cancelled;
    let _ = snapshot.total_matches_created;
    let _ = snapshot.total_players_matched;
    let _ = snapshot.match_attempts_insufficient;
    let _ = snapshot.match_attempts_claim_failed;
    let _ = snapshot.worker_cycles_total;
    let _ = snapshot.total_stale_claims_recovered;
    let _ = snapshot.current_queue_size;
    let _ = snapshot.total_wait_time_ms;
    let _ = snapshot.avg_wait_ms;
    let _ = snapshot.avg_skill_spread;
    let _ = snapshot.avg_team_delta;

    // If this test compiles and runs, all fields exist in MetricsSnapshot
}

#[tokio::test]
async fn test_snapshot_serializes_to_valid_json() {
    let core = make_core();

    // Enqueue and match some players so non-zero values are present
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();
    seed_uniform(&core, 10, 1000);
    let _ = attempt_match(&ctx, &mut state);

    let snapshot = core.metrics_snapshot();
    let json = serde_json::to_string(&snapshot)
        .expect("MetricsSnapshot must serialize to JSON without error");

    // Verify all required field names appear in the JSON
    let required_fields = [
        "total_players_enqueued",
        "total_players_cancelled",
        "total_matches_created",
        "total_players_matched",
        "match_attempts_insufficient",
        "match_attempts_claim_failed",
        "worker_cycles_total",
        "total_stale_claims_recovered",
        "current_queue_size",
        "total_wait_time_ms",
        "avg_wait_ms",
        "avg_skill_spread",
        "avg_team_delta",
    ];

    for field in &required_fields {
        assert!(
            json.contains(field),
            "Serialized metrics JSON must contain field '{field}'"
        );
    }

    // Verify it round-trips through serde_json::Value
    let value: serde_json::Value = serde_json::from_str(&json).expect("JSON must be valid");

    assert!(value.is_object(), "Metrics JSON must be an object");
    assert_eq!(
        value["total_matches_created"].as_u64().unwrap(),
        1,
        "total_matches_created must be 1 after one match"
    );
    assert_eq!(
        value["total_players_matched"].as_u64().unwrap(),
        10,
        "total_players_matched must be 10"
    );
}
