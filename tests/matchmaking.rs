//! Matchmaking correctness tests.
//!
//! Validates the complete match formation pipeline:
//! - Basic match creation with compatible players
//! - Candidate discovery and pool scanning
//! - Constraint relaxation progression
//! - Starvation prevention (Stage 5 floor)
//! - Player removal after matching
//! - Duplicate prevention
//! - Edge cases: empty pool, insufficient players, outlier MMR

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use uuid::Uuid;

use matchmaker::engine::bucket::PlayerPool;
use matchmaker::engine::matcher::{attempt_match, MatchAttemptResult, WorkerState};
use matchmaker::engine::relaxation::{relaxation_stage, relaxation_window, scan_bounds};
use matchmaker::models::{player_state, Player};

use common::{
    assert_match_valid, assert_no_duplicates, assert_team_delta_is_optimal, clear_env,
    default_config, fast_config, make_core, make_match_ready_players, make_player, make_worker_ctx,
    seed_uniform,
};

// ── Basic match formation ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_match_forms_with_exactly_ten_players() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);

    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::Success(_)),
        "Expected Success with 10 compatible players, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_match_result_is_structurally_valid() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // 10 players within Stage 1 window (±50 MMR)
    let players = make_match_ready_players(1000);
    for p in &players {
        core.enqueue(p.id, p.skill_rating).unwrap();
    }

    if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
        assert_match_valid(&m);
    } else {
        panic!("Expected a successful match");
    }
}

#[tokio::test]
async fn test_team_delta_is_optimal_for_formed_match() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    let players = make_match_ready_players(1000);
    for p in &players {
        core.enqueue(p.id, p.skill_rating).unwrap();
    }

    if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
        assert_team_delta_is_optimal(&m);
    } else {
        panic!("Expected a successful match");
    }
}

#[tokio::test]
async fn test_all_players_removed_from_pool_after_match() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);
    assert_eq!(core.players_waiting(), 10);

    let result = attempt_match(&ctx, &mut state);
    assert!(matches!(result, MatchAttemptResult::Success(_)));
    assert_eq!(
        core.players_waiting(),
        0,
        "All 10 players must be removed from pool after match"
    );
}

#[tokio::test]
async fn test_matched_players_state_is_matched() {
    use std::sync::Arc;

    // Build the pool and engine manually so we hold the SAME Arc<Player>
    // instances that the pool holds — not copies created by core.enqueue().
    clear_env();
    let config = default_config();
    let metrics = Arc::new(matchmaker::metrics::Metrics::new());
    let core = Arc::new(matchmaker::engine::MatchmakerCore::new(
        Arc::clone(&config),
        Arc::clone(&metrics),
    ));

    // Insert players directly via the pool so we retain the same Arc.
    let players: Vec<Arc<Player>> = (0..10)
        .map(|i| Arc::new(Player::new(Uuid::new_v4(), 1000 + i * 5)))
        .collect();

    // Use the pool's insert directly — bypasses core.enqueue() constructor.
    for p in &players {
        core.pool().insert(Arc::clone(p));
        // Manually update metrics since we bypassed enqueue()
        core.metrics()
            .total_players_enqueued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        core.metrics()
            .current_queue_size
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::Success(_)),
        "Expected successful match"
    );

    // Now check state on the SAME Arc instances we inserted.
    for p in &players {
        assert_eq!(
            p.state(),
            player_state::MATCHED,
            "Player {} must be in Matched state after match formation",
            p.id
        );
    }
}

#[tokio::test]
async fn test_match_stored_in_history() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 10, 1000);
    let _ = attempt_match(&ctx, &mut state);

    let history = core.recent_matches(10);
    assert_eq!(history.len(), 1, "Formed match must appear in history");
    assert_match_valid(&history[0]);
}

#[tokio::test]
async fn test_multiple_sequential_matches_all_valid() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // 30 players → 3 sequential matches
    seed_uniform(&core, 30, 1000);

    let mut matches = Vec::new();
    for _ in 0..3 {
        if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
            matches.push(m);
        }
    }

    assert_eq!(matches.len(), 3, "Expected 3 sequential matches");
    assert_eq!(
        core.players_waiting(),
        0,
        "Pool must be empty after 3 matches"
    );

    for m in &matches {
        assert_match_valid(m);
    }

    assert_no_duplicates(&matches);
}

// ── Empty pool and edge cases ─────────────────────────────────────────────────

#[tokio::test]
async fn test_empty_pool_returns_pool_empty() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::PoolEmpty),
        "Empty pool must return PoolEmpty, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_nine_players_returns_insufficient_candidates() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 9, 1000);

    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(
            result,
            MatchAttemptResult::InsufficientCandidates { found: 9, .. }
        ),
        "9 players must return InsufficientCandidates, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_single_player_returns_insufficient_candidates() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    core.enqueue(Uuid::new_v4(), 1000).unwrap();

    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(
            result,
            MatchAttemptResult::InsufficientCandidates { found: 1, .. }
        ),
        "Single player must return InsufficientCandidates"
    );
}

#[tokio::test]
async fn test_incompatible_players_outside_window_not_matched() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // 5 players at 1000 MMR, 5 players at 2000 MMR
    // Stage 1 window is ±50 — they are 1000 apart, incompatible
    seed_uniform(&core, 5, 1000);
    seed_uniform(&core, 5, 2000);

    // With 10 players total but incompatible — each group needs 10 within window
    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::InsufficientCandidates { .. }),
        "Players 1000 MMR apart must not match in Stage 1, got {:?}",
        result
    );
    assert_eq!(
        core.players_waiting(),
        10,
        "No players must be removed when match fails"
    );
}

// ── Candidate discovery ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_range_scan_respects_window_boundaries() {
    let config = default_config();
    let pool = Arc::new(PlayerPool::new());

    // Player at 1000 MMR — Stage 1 window is ±50 → [950, 1050]
    let seed = Arc::new(Player::new(Uuid::new_v4(), 1000));
    pool.insert(Arc::clone(&seed));

    // Players within window
    let inside_1 = Arc::new(Player::new(Uuid::new_v4(), 1049));
    let inside_2 = Arc::new(Player::new(Uuid::new_v4(), 951));
    pool.insert(Arc::clone(&inside_1));
    pool.insert(Arc::clone(&inside_2));

    // Players outside window
    let outside_1 = Arc::new(Player::new(Uuid::new_v4(), 1100));
    let outside_2 = Arc::new(Player::new(Uuid::new_v4(), 800));
    pool.insert(Arc::clone(&outside_1));
    pool.insert(Arc::clone(&outside_2));

    let (min, max) = scan_bounds(seed.skill_rating, seed.join_timestamp, &config);
    let candidates = pool.range_scan(min, max);

    let candidate_ids: HashSet<Uuid> = candidates.iter().map(|p| p.id).collect();

    assert!(
        candidate_ids.contains(&seed.id),
        "Seed must be in scan results"
    );
    assert!(
        candidate_ids.contains(&inside_1.id),
        "Player at 1049 must be in Stage 1 window"
    );
    assert!(
        candidate_ids.contains(&inside_2.id),
        "Player at 951 must be in Stage 1 window"
    );
    assert!(
        !candidate_ids.contains(&outside_1.id),
        "Player at 1100 must be outside Stage 1 window"
    );
    assert!(
        !candidate_ids.contains(&outside_2.id),
        "Player at 800 must be outside Stage 1 window"
    );
}

#[tokio::test]
async fn test_candidates_sorted_by_join_timestamp() {
    let config = default_config();
    let pool = Arc::new(PlayerPool::new());

    // Insert players with small delays to ensure distinct Instants
    for _ in 0..5 {
        pool.insert(Arc::new(Player::new(Uuid::new_v4(), 1000)));
        std::thread::sleep(Duration::from_millis(2));
    }

    let (min, max) = scan_bounds(1000, std::time::Instant::now(), &config);
    let candidates = pool.range_scan(min, max);

    // Verify ascending join_timestamp order
    for window in candidates.windows(2) {
        assert!(
            window[0].join_timestamp <= window[1].join_timestamp,
            "Candidates must be sorted by join_timestamp ASC"
        );
    }
}

// ── Constraint relaxation ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_relaxation_window_stage_1_fresh_player() {
    let config = default_config();
    let player = make_player(1000);

    let window = relaxation_window(player.join_timestamp, &config);
    assert_eq!(
        window, config.relaxation_stage_1_delta,
        "Brand new player must get Stage 1 window"
    );
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 1);
}

#[tokio::test]
async fn test_relaxation_window_progresses_through_all_stages() {
    // Use fast config — stages progress in 50/100/200/400ms
    let config = fast_config();

    let player = make_player(1000);

    // Stage 1 immediately
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 1);
    assert_eq!(
        relaxation_window(player.join_timestamp, &config),
        config.relaxation_stage_1_delta
    );

    // Wait past stage 1 threshold (50ms)
    sleep(Duration::from_millis(60)).await;
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 2);
    assert_eq!(
        relaxation_window(player.join_timestamp, &config),
        config.relaxation_stage_2_delta
    );

    // Wait past stage 2 threshold (100ms total)
    sleep(Duration::from_millis(60)).await;
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 3);
    assert_eq!(
        relaxation_window(player.join_timestamp, &config),
        config.relaxation_stage_3_delta
    );

    // Wait past stage 3 threshold (200ms total)
    sleep(Duration::from_millis(120)).await;
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 4);

    // Wait past stage 4 threshold (400ms total)
    sleep(Duration::from_millis(220)).await;
    assert_eq!(relaxation_stage(player.join_timestamp, &config), 5);
    assert_eq!(
        relaxation_window(player.join_timestamp, &config),
        config.relaxation_stage_5_delta
    );
}

#[tokio::test]
async fn test_relaxation_window_is_monotonically_non_decreasing() {
    let config = fast_config();
    let player = make_player(1000);

    let mut prev_window = 0u32;

    // Sample window at 10 points over 500ms
    for _ in 0..10 {
        sleep(Duration::from_millis(50)).await;
        let window = relaxation_window(player.join_timestamp, &config);
        assert!(
            window >= prev_window,
            "Window must be non-decreasing: {} < {}",
            window,
            prev_window
        );
        prev_window = window;
    }
}

/// Core relaxation integration test:
/// An outlier player (MMR 2950) in a pool of 1000–1050 MMR players.
/// They cannot match at Stage 1 (±50). After waiting past Stage 5
/// threshold, their window becomes unconstrained and they match.
#[tokio::test]
async fn test_outlier_player_matched_at_starvation_floor() {
    // Use fast config so Stage 5 is reached in ~400ms
    let config = fast_config();
    let metrics = Arc::new(matchmaker::metrics::Metrics::new());
    let core = Arc::new(MatchmakerCore::new(
        Arc::clone(&config),
        Arc::clone(&metrics),
    ));
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    use matchmaker::engine::MatchmakerCore;

    // Enqueue 9 players at 1000–1045 MMR (Stage 1 compatible with each other)
    for i in 0..9u32 {
        core.enqueue(Uuid::new_v4(), 1000 + i * 5).unwrap();
    }

    // Enqueue the outlier at 2950 MMR
    let outlier_id = Uuid::new_v4();
    core.enqueue(outlier_id, 2950).unwrap();

    // At Stage 1: outlier cannot match (needs players within ±50 of 2950)
    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::InsufficientCandidates { .. }),
        "Outlier must not match at Stage 1"
    );

    // Wait past Stage 4 threshold → enters Stage 5 (unconstrained)
    sleep(Duration::from_millis(450)).await;

    // Stage 5: window is ±9999 — all players are now candidates
    let window = relaxation_window(
        core.pool()
            .get(&outlier_id)
            .expect("outlier must still be in pool")
            .join_timestamp,
        &config,
    );
    assert_eq!(
        window, config.relaxation_stage_5_delta,
        "Outlier must be at Stage 5 window after waiting"
    );

    // Now a match should form — outlier + 9 low-MMR players
    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::Success(_)),
        "Outlier must be matched at Stage 5, got {:?}",
        result
    );

    assert_eq!(core.players_waiting(), 0, "All players must be matched");
}

// ── Duplicate prevention ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_no_duplicate_player_ids_across_teams() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    seed_uniform(&core, 20, 1000);

    let mut all_matches = Vec::new();
    for _ in 0..2 {
        if let MatchAttemptResult::Success(m) = attempt_match(&ctx, &mut state) {
            all_matches.push(m);
        }
    }

    assert_eq!(all_matches.len(), 2);
    assert_no_duplicates(&all_matches);
}

#[tokio::test]
async fn test_player_not_in_pool_after_match() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    let players = make_match_ready_players(1000);
    let ids: Vec<Uuid> = players.iter().map(|p| p.id).collect();

    for p in &players {
        core.enqueue(p.id, p.skill_rating).unwrap();
    }

    let _ = attempt_match(&ctx, &mut state);

    for id in &ids {
        assert!(
            !core.pool().contains(id),
            "Player {} must not be in pool after being matched",
            id
        );
    }
}

// ── Fairness: oldest player matched first ─────────────────────────────────────

#[tokio::test]
async fn test_oldest_player_is_seed() {
    let core = make_core();

    // First player enqueued — should be the seed
    let first_id = Uuid::new_v4();
    core.enqueue(first_id, 1000).unwrap();

    // Small delay to ensure distinct Instant
    sleep(Duration::from_millis(5)).await;

    // 9 more players
    for _ in 0..9 {
        core.enqueue(Uuid::new_v4(), 1010).unwrap();
    }

    let oldest = core
        .pool()
        .oldest_waiting()
        .expect("pool must not be empty");

    assert_eq!(
        oldest.id, first_id,
        "Oldest player must be the first one enqueued"
    );
}

// ── Cancel removes player from consideration ──────────────────────────────────

#[tokio::test]
async fn test_cancelled_player_not_matched() {
    let core = make_core();
    let ctx = make_worker_ctx(&core, 1);
    let mut state = WorkerState::new();

    // Enqueue 10 players but cancel one
    let cancel_id = Uuid::new_v4();
    core.enqueue(cancel_id, 1000).unwrap();

    for _ in 0..9 {
        core.enqueue(Uuid::new_v4(), 1005).unwrap();
    }

    core.cancel(&cancel_id).unwrap();

    // Only 9 players remain — insufficient for a match
    let result = attempt_match(&ctx, &mut state);
    assert!(
        matches!(result, MatchAttemptResult::InsufficientCandidates { .. }),
        "Cancelled player must not be counted as a candidate"
    );
}

// ── scan_bounds boundary tests ────────────────────────────────────────────────

#[test]
fn test_scan_bounds_no_underflow_at_zero_rating() {
    let config = default_config();
    let player = make_player(0);
    let (min, _) = scan_bounds(player.skill_rating, player.join_timestamp, &config);
    assert_eq!(min, 0, "Lower bound must not underflow below 0");
}

#[test]
fn test_scan_bounds_no_overflow_at_max_rating() {
    let config = default_config();
    let player = make_player(matchmaker::engine::bucket::MAX_SKILL_RATING);
    let (_, max) = scan_bounds(player.skill_rating, player.join_timestamp, &config);
    assert_eq!(
        max,
        matchmaker::engine::bucket::MAX_SKILL_RATING,
        "Upper bound must not exceed MAX_SKILL_RATING"
    );
}

// ── Helper import for outlier test ────────────────────────────────────────────
