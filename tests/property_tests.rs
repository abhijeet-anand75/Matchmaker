//! Property-based tests using proptest.
//!
//! Validates system invariants across randomly generated inputs.
//! These invariants must hold for ALL valid inputs, not just hand-crafted cases.

mod common;

use std::sync::Arc;

use proptest::prelude::*;
use uuid::Uuid;

use matchmaker::engine::balancer::{exhaustive_balance, MATCH_SIZE, TEAM_SIZE};
use matchmaker::models::Player;

use common::clear_env;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn players_from_ratings(ratings: &[u32; 10]) -> Vec<Arc<Player>> {
    ratings
        .iter()
        .map(|&r| Arc::new(Player::new(Uuid::new_v4(), r)))
        .collect()
}

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

// ── Strategies ────────────────────────────────────────────────────────────────

fn ten_ratings() -> impl Strategy<Value = [u32; 10]> {
    prop::array::uniform10(0u32..=3000u32)
}

fn clustered_ratings() -> impl Strategy<Value = [u32; 10]> {
    (500u32..2500u32).prop_flat_map(|center| {
        prop::array::uniform10(
            center.saturating_sub(200)..=(center + 200).min(3000),
        )
    })
}

fn bimodal_ratings() -> impl Strategy<Value = [u32; 10]> {
    (0u32..1400u32, 1600u32..=3000u32).prop_flat_map(|(low, high)| {
        (
            prop::array::uniform5(low..=(low + 200).min(3000)),
            prop::array::uniform5(high.saturating_sub(200)..=high),
        )
            .prop_map(|(lows, highs)| {
                [
                    lows[0], lows[1], lows[2], lows[3], lows[4],
                    highs[0], highs[1], highs[2], highs[3], highs[4],
                ]
            })
    })
}

// ── Property: team sizes are always 5 ────────────────────────────────────────

proptest! {
    #[test]
    fn prop_team_sizes_always_five(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        prop_assert_eq!(
            result.team_a.len(),
            TEAM_SIZE,
            "team_a must have exactly 5 players for ratings {:?}",
            ratings
        );
        prop_assert_eq!(
            result.team_b.len(),
            TEAM_SIZE,
            "team_b must have exactly 5 players for ratings {:?}",
            ratings
        );
    }
}

// ── Property: every input player in exactly one team ─────────────────────────

proptest! {
    #[test]
    fn prop_every_player_in_exactly_one_team(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let input_ids: std::collections::HashSet<Uuid> =
            players.iter().map(|p| p.id).collect();

        let result = exhaustive_balance(&players);

        let output_ids: std::collections::HashSet<Uuid> = result
            .team_a
            .iter()
            .chain(result.team_b.iter())
            .map(|p| p.id)
            .collect();

        prop_assert_eq!(
            input_ids.len(),
            MATCH_SIZE,
            "Input must have 10 unique players, got {}",
            input_ids.len()
        );
        prop_assert_eq!(
            input_ids,
            output_ids,
            "Every input player must appear in exactly one output team"
        );

        // No player in both teams
        let a_ids: std::collections::HashSet<Uuid> =
            result.team_a.iter().map(|p| p.id).collect();
        let b_ids: std::collections::HashSet<Uuid> =
            result.team_b.iter().map(|p| p.id).collect();
        prop_assert!(
            a_ids.is_disjoint(&b_ids),
            "No player may appear in both teams"
        );
    }
}

// ── Property: team_delta equals |sum_a - sum_b| ───────────────────────────────

proptest! {
    #[test]
    fn prop_team_delta_equals_absolute_sum_difference(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        let a_sum: u32 = result.team_a.iter().map(|p| p.skill_rating).sum();
        let b_sum: u32 = result.team_b.iter().map(|p| p.skill_rating).sum();
        let expected_delta = a_sum.abs_diff(b_sum);

        prop_assert_eq!(
            result.team_delta,
            expected_delta,
            "team_delta must equal |sum_a - sum_b| for ratings {:?}",
            ratings
        );
    }
}

// ── Property: team_delta is always the minimum possible ──────────────────────

proptest! {
    #[test]
    fn prop_team_delta_is_always_optimal(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);
        let min_possible = brute_force_min_delta(&ratings);

        prop_assert_eq!(
            result.team_delta,
            min_possible,
            "team_delta {} is not optimal — minimum possible is {} for ratings {:?}",
            result.team_delta,
            min_possible,
            ratings
        );
    }
}

// ── Property: total_rating equals sum of all input ratings ───────────────────

proptest! {
    #[test]
    fn prop_total_rating_equals_input_sum(ratings in ten_ratings()) {
        clear_env();
        let expected_total: u32 = ratings.iter().sum();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        prop_assert_eq!(
            result.total_rating,
            expected_total,
            "total_rating must equal sum of all input ratings for {:?}",
            ratings
        );
    }
}

// ── Property: sum_a + sum_b == total_rating ───────────────────────────────────

proptest! {
    #[test]
    fn prop_team_sums_add_to_total(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        let a_sum: u32 = result.team_a.iter().map(|p| p.skill_rating).sum();
        let b_sum: u32 = result.team_b.iter().map(|p| p.skill_rating).sum();

        prop_assert_eq!(
            a_sum + b_sum,
            result.total_rating,
            "sum_a + sum_b must equal total_rating for ratings {:?}",
            ratings
        );
    }
}

// ── Property: clustered ratings are optimally balanced ───────────────────────

proptest! {
    #[test]
    fn prop_clustered_ratings_delta_bounded(ratings in clustered_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);
        let min_possible = brute_force_min_delta(&ratings);

        prop_assert_eq!(
            result.team_delta,
            min_possible,
            "Clustered ratings must achieve optimal delta for {:?}",
            ratings
        );
    }
}

// ── Property: bimodal ratings are optimally balanced ─────────────────────────

proptest! {
    #[test]
    fn prop_bimodal_ratings_optimally_balanced(ratings in bimodal_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);
        let min_possible = brute_force_min_delta(&ratings);

        prop_assert_eq!(
            result.team_delta,
            min_possible,
            "Bimodal distribution must be optimally balanced for ratings {:?}",
            ratings
        );

        prop_assert_eq!(
            result.team_a.len(),
            TEAM_SIZE,
            "team_a must have 5 players, got {}",
            result.team_a.len()
        );
        prop_assert_eq!(
            result.team_b.len(),
            TEAM_SIZE,
            "team_b must have 5 players, got {}",
            result.team_b.len()
        );
    }
}

// ── Property: uniform ratings always achieve zero delta ──────────────────────

proptest! {
    #[test]
    fn prop_uniform_ratings_achieve_zero_delta(rating in 0u32..=3000u32) {
        clear_env();
        // 10 players all at the same rating — delta must always be 0
        let ratings = [rating; 10];
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        prop_assert_eq!(
            result.team_delta,
            0,
            "Uniform rating {} must produce delta = 0",
            rating
        );
    }
}

// ── Property: match contains exactly 10 unique players ───────────────────────

proptest! {
    #[test]
    fn prop_match_contains_exactly_ten_unique_players(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        let all_ids: std::collections::HashSet<Uuid> = result
            .team_a
            .iter()
            .chain(result.team_b.iter())
            .map(|p| p.id)
            .collect();

        prop_assert_eq!(
            all_ids.len(),
            MATCH_SIZE,
            "Match must contain exactly 10 unique players, got {}",
            all_ids.len()
        );
        prop_assert_eq!(
            result.team_a.len() + result.team_b.len(),
            MATCH_SIZE,
            "Total players across both teams must be 10, got {}",
            result.team_a.len() + result.team_b.len()
        );
    }
}

// ── Property: team_delta is non-negative ─────────────────────────────────────

proptest! {
    #[test]
    fn prop_team_delta_is_non_negative(ratings in ten_ratings()) {
        clear_env();
        let players = players_from_ratings(&ratings);
        let result = exhaustive_balance(&players);

        // u32 is always >= 0, but we verify the semantic invariant explicitly
        prop_assert!(
            result.team_delta <= result.total_rating,
            "team_delta {} must not exceed total_rating {}",
            result.team_delta,
            result.total_rating
        );
    }
}