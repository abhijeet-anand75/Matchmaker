//! Team balance algorithm tests.
//!
//! Validates the exhaustive C(10,5) balance algorithm:
//! - Always produces exactly 5 players per team
//! - Every input player appears in exactly one output team
//! - The result is provably optimal (verified by independent brute-force)
//! - Deterministic: identical inputs → identical delta
//! - Handles uniform, spread, and extreme distributions

mod common;

use std::sync::Arc;
use uuid::Uuid;

use matchmaker::engine::balancer::{exhaustive_balance, MATCH_SIZE, TEAM_SIZE};
use matchmaker::models::Player;

use common::{assert_team_delta_is_optimal, clear_env};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_player(skill_rating: u32) -> Arc<Player> {
    Arc::new(Player::new(Uuid::new_v4(), skill_rating))
}

fn make_players(ratings: &[u32]) -> Vec<Arc<Player>> {
    assert_eq!(ratings.len(), MATCH_SIZE, "Must provide exactly 10 ratings");
    ratings.iter().map(|&r| make_player(r)).collect()
}

/// Independently verify optimality by brute-force.
fn brute_force_min_delta(ratings: &[u32; 10]) -> u32 {
    let total: u32 = ratings.iter().sum();
    (0u16..1024u16)
        .filter(|m| m.count_ones() == 5)
        .map(|mask| {
            let a: u32 = (0..10)
                .filter(|&i| (mask >> i) & 1 == 1)
                .map(|i| ratings[i])
                .sum();
            (2 * a).abs_diff(total)
        })
        .min()
        .unwrap_or(u32::MAX)
}

// ── Team size invariants ──────────────────────────────────────────────────────

#[test]
fn test_team_sizes_are_always_five() {
    clear_env();
    let players = make_players(&[1000; 10]);
    let result = exhaustive_balance(&players);
    assert_eq!(result.team_a.len(), TEAM_SIZE, "team_a must have 5 players");
    assert_eq!(result.team_b.len(), TEAM_SIZE, "team_b must have 5 players");
}

#[test]
fn test_team_sizes_correct_for_varied_ratings() {
    clear_env();
    let inputs: Vec<[u32; 10]> = vec![
        [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        [1, 1, 1, 1, 1, 3000, 3000, 3000, 3000, 3000],
        [1500; 10],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 3000],
    ];

    for ratings in &inputs {
        let players = make_players(ratings);
        let result = exhaustive_balance(&players);
        assert_eq!(
            result.team_a.len(),
            TEAM_SIZE,
            "team_a must have 5 players for ratings {:?}",
            ratings
        );
        assert_eq!(
            result.team_b.len(),
            TEAM_SIZE,
            "team_b must have 5 players for ratings {:?}",
            ratings
        );
    }
}

// ── Player containment ────────────────────────────────────────────────────────

#[test]
fn test_all_input_players_appear_in_output() {
    clear_env();
    let players = make_players(&[800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700]);
    let input_ids: std::collections::HashSet<Uuid> = players.iter().map(|p| p.id).collect();

    let result = exhaustive_balance(&players);
    let output_ids: std::collections::HashSet<Uuid> = result
        .team_a
        .iter()
        .chain(result.team_b.iter())
        .map(|p| p.id)
        .collect();

    assert_eq!(
        input_ids, output_ids,
        "Every input player must appear in exactly one output team"
    );
}

#[test]
fn test_no_player_in_both_teams() {
    clear_env();
    let players = make_players(&[1000; 10]);
    let result = exhaustive_balance(&players);

    let a_ids: std::collections::HashSet<Uuid> = result.team_a.iter().map(|p| p.id).collect();
    let b_ids: std::collections::HashSet<Uuid> = result.team_b.iter().map(|p| p.id).collect();

    assert!(
        a_ids.is_disjoint(&b_ids),
        "No player may appear in both teams"
    );
}

// ── Optimality ────────────────────────────────────────────────────────────────

#[test]
fn test_uniform_ratings_produce_zero_delta() {
    clear_env();
    let players = make_players(&[1000; 10]);
    let result = exhaustive_balance(&players);
    assert_eq!(
        result.team_delta, 0,
        "Uniform ratings must produce delta = 0"
    );
}

#[test]
fn test_paired_equal_ratings_produce_zero_delta() {
    clear_env();
    // Each rating appears exactly twice — perfect pairing possible
    let players = make_players(&[1000, 1000, 1100, 1100, 1200, 1200, 1300, 1300, 1400, 1400]);
    let result = exhaustive_balance(&players);
    assert_eq!(
        result.team_delta, 0,
        "Paired ratings must achieve delta = 0"
    );
}

#[test]
fn test_arithmetic_sequence_is_optimal() {
    clear_env();
    let ratings = [100u32, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(
        result.team_delta, min_possible,
        "Result must be optimal for arithmetic sequence"
    );
}

#[test]
fn test_extreme_outlier_handled_correctly() {
    clear_env();
    // One player at max rating, rest at minimum
    let ratings = [1u32, 1, 1, 1, 1, 1, 1, 1, 1, 3000];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(
        result.team_delta, min_possible,
        "Extreme outlier must be handled optimally"
    );

    // The outlier goes to one team — delta = |3004 - 5| = 2999
    assert_eq!(result.team_delta, 2999);
}

#[test]
fn test_bimodal_distribution_is_optimal() {
    clear_env();
    // Two clusters: 5 players at 500, 5 players at 1500.
    // Total = 10000. For delta = 0 we need a team summing to 5000.
    // Possible sums with k players from 1500-group + (5-k) from 500-group:
    //   k=0 → 2500, k=1 → 3500, k=2 → 4500, k=3 → 5500
    // None reaches 5000. Minimum achievable delta = |4500 - 5500| = 1000.
    let ratings = [500u32, 500, 500, 500, 500, 1500, 1500, 1500, 1500, 1500];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    // Verify against independent brute-force oracle.
    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(
        result.team_delta, min_possible,
        "Bimodal distribution must be optimally balanced"
    );

    // The true minimum for this specific input is 1000 — not 0.
    assert_eq!(
        result.team_delta, 1000,
        "Bimodal (5×500, 5×1500): minimum achievable delta is 1000"
    );

    assert_eq!(result.team_a.len(), TEAM_SIZE);
    assert_eq!(result.team_b.len(), TEAM_SIZE);
}

#[test]
fn test_bimodal_perfectly_balanced_achieves_zero_delta() {
    clear_env();
    // Carefully constructed input where delta = 0 is achievable.
    // Team A: [500, 600, 700, 800, 900] = 3500
    // Team B: [600, 700, 800, 900, 1000] = 4000
    // Wait — let's use a verified-by-hand input:
    //
    // [1000, 1000, 1000, 1100, 1100, 900, 900, 1000, 1000, 1000]
    // Total = 10000, half = 5000
    // Team A: [1000, 1000, 1000, 1000, 1000] = 5000 ✓
    // Team B: [1100, 1100, 900, 900, 1000] = 5000 ✓
    let ratings = [1000u32, 1000, 1000, 1000, 1000, 1100, 1100, 900, 900, 1000];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(
        result.team_delta, min_possible,
        "Must be optimally balanced"
    );
    assert_eq!(
        result.team_delta, 0,
        "This input allows perfect balance — delta must be 0"
    );

    assert_eq!(result.team_a.len(), TEAM_SIZE);
    assert_eq!(result.team_b.len(), TEAM_SIZE);
}

#[test]
fn test_all_different_ratings_is_optimal() {
    clear_env();
    let ratings = [723u32, 841, 956, 1102, 1287, 1334, 1456, 1589, 1701, 1823];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(
        result.team_delta, min_possible,
        "Varied ratings must be optimally balanced"
    );
}

#[test]
fn test_symmetric_distribution_around_midpoint() {
    clear_env();
    // Symmetric: sum = 15000, each team should sum to 7500
    let ratings = [500u32, 1000, 1000, 1500, 1500, 1500, 1500, 2000, 2000, 2500];
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    let min_possible = brute_force_min_delta(&ratings);
    assert_eq!(result.team_delta, min_possible);
}

// ── Total rating correctness ──────────────────────────────────────────────────

#[test]
fn test_total_rating_matches_sum() {
    clear_env();
    let ratings = [800u32, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700];
    let expected_total: u32 = ratings.iter().sum();
    let players = make_players(&ratings);
    let result = exhaustive_balance(&players);

    assert_eq!(
        result.total_rating, expected_total,
        "total_rating must equal sum of all player ratings"
    );
}

#[test]
fn test_team_delta_matches_computed_sums() {
    clear_env();
    let players = make_players(&[1000, 1100, 1200, 1300, 1400, 1050, 1150, 1250, 1350, 1450]);
    let result = exhaustive_balance(&players);

    let a_sum: u32 = result.team_a.iter().map(|p| p.skill_rating).sum();
    let b_sum: u32 = result.team_b.iter().map(|p| p.skill_rating).sum();
    let computed_delta = a_sum.abs_diff(b_sum);

    assert_eq!(
        result.team_delta, computed_delta,
        "team_delta must equal |sum_a - sum_b|"
    );
}

// ── Determinism ───────────────────────────────────────────────────────────────

#[test]
fn test_deterministic_delta_for_same_ratings() {
    clear_env();
    // Two independent calls with the same ratings must produce the same delta.
    // (UUIDs differ, so team composition may differ, but delta must be equal.)
    let ratings = [
        1000u32, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900,
    ];
    let p1 = make_players(&ratings);
    let p2 = make_players(&ratings);

    let r1 = exhaustive_balance(&p1);
    let r2 = exhaustive_balance(&p2);

    assert_eq!(
        r1.team_delta, r2.team_delta,
        "Same ratings must always produce the same delta"
    );
    assert_eq!(r1.total_rating, r2.total_rating);
}

// ── Multiple formations — no quality regression ───────────────────────────────

#[test]
fn test_balance_quality_consistent_across_formations() {
    clear_env();
    // Run balance 100 times on the same input — delta must always be identical
    let ratings = [900u32, 950, 1000, 1050, 1100, 1150, 1200, 1250, 1300, 1350];
    let expected_min = brute_force_min_delta(&ratings);

    for run in 0..100 {
        let players = make_players(&ratings);
        let result = exhaustive_balance(&players);
        assert_eq!(
            result.team_delta, expected_min,
            "Run {run}: balance quality must be consistent"
        );
    }
}

// ── Using common assert helper ────────────────────────────────────────────────

#[test]
fn test_assert_team_delta_is_optimal_helper() {
    clear_env();
    let core = common::make_core();
    let ctx = common::make_worker_ctx(&core, 1);
    let mut state = matchmaker::engine::matcher::WorkerState::new();

    let players = common::make_match_ready_players(1200);
    for p in &players {
        core.enqueue(p.id, p.skill_rating).unwrap();
    }

    if let matchmaker::engine::matcher::MatchAttemptResult::Success(m) =
        matchmaker::engine::matcher::attempt_match(&ctx, &mut state)
    {
        assert_team_delta_is_optimal(&m);
    } else {
        panic!("Expected successful match for optimal delta validation");
    }
}
