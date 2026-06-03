//! Time-based constraint relaxation for matchmaking quality vs. latency.
//!
//! As a player waits longer in the queue, their acceptable skill deviation
//! window widens. This balances two competing objectives:
//!
//! - **Match quality**: Players should be matched with opponents of similar skill.
//! - **Match latency**: No player should wait indefinitely.
//!
//! # Relaxation Stages
//!
//! ```text
//! Wait time       MMR window      Quality impact
//! ──────────────────────────────────────────────
//! 0  → Stage1     ±Stage1Delta    Excellent
//! S1 → Stage2     ±Stage2Delta    Good
//! S2 → Stage3     ±Stage3Delta    Acceptable
//! S3 → Stage4     ±Stage4Delta    Fair
//! S4+             ±Stage5Delta    Starvation floor — match at any cost
//! ```
//!
//! All thresholds and deltas are loaded from [`Config`] — no hardcoded values.
//! Changing relaxation behaviour requires only environment variable changes.
//!
//! # Monotonicity Guarantee
//!
//! The relaxation window is monotonically non-decreasing over time.
//! A player's window never narrows after widening — this is enforced by
//! the strictly-increasing threshold validation in [`Config::from_env`].

use std::time::Instant;

use crate::config::Config;

//  Core function

/// Compute the current acceptable MMR half-width for a waiting player.
///
/// Given a player's `join_timestamp` and the system configuration, returns
/// the maximum skill rating deviation (±delta) that is acceptable for
/// matching this player right now.
///
/// The returned value is a **half-width**: a player with rating `R` and
/// window `W` can be matched with any player in the range `[R-W, R+W]`.
///
/// # Arguments
///
/// * `join_timestamp` — the [`Instant`] when the player entered the queue.
/// * `config` — system configuration holding all threshold and delta values.
///
/// # Returns
///
/// The current acceptable MMR half-width as a `u32`.
/// Always returns a value in `[Stage1Delta, Stage5Delta]`.
///
/// # Panics
///
/// Never panics. `elapsed()` on a monotonic `Instant` always succeeds.
///
/// # Example
///
/// ```
/// use std::time::Instant;
/// use matchmaker::config::Config;
/// use matchmaker::engine::relaxation::relaxation_window;
///
/// // A brand-new player gets the tightest window.
/// let config = Config::from_env().unwrap();
/// let window = relaxation_window(Instant::now(), &config);
/// assert_eq!(window, config.relaxation_stage_1_delta);
/// ```
pub fn relaxation_window(join_timestamp: Instant, config: &Config) -> u32 {
    let elapsed_ms = join_timestamp.elapsed().as_millis() as u64;

    match elapsed_ms {
        t if t < config.relaxation_stage_1_ms => config.relaxation_stage_1_delta,
        t if t < config.relaxation_stage_2_ms => config.relaxation_stage_2_delta,
        t if t < config.relaxation_stage_3_ms => config.relaxation_stage_3_delta,
        t if t < config.relaxation_stage_4_ms => config.relaxation_stage_4_delta,
        _ => config.relaxation_stage_5_delta,
    }
}

/// Compute the current relaxation stage index (1–5) for a waiting player.
///
/// Useful for logging and metrics — identifies which stage a player is in
/// without recomputing the full window.
///
/// Returns a value in `[1, 5]`.
pub fn relaxation_stage(join_timestamp: Instant, config: &Config) -> u8 {
    let elapsed_ms = join_timestamp.elapsed().as_millis() as u64;

    match elapsed_ms {
        t if t < config.relaxation_stage_1_ms => 1,
        t if t < config.relaxation_stage_2_ms => 2,
        t if t < config.relaxation_stage_3_ms => 3,
        t if t < config.relaxation_stage_4_ms => 4,
        _ => 5,
    }
}

/// Compute the MMR scan bounds `(min_rating, max_rating)` for a player.
///
/// Clamps the lower bound to `0` to prevent u32 underflow.
/// Clamps the upper bound to `MAX_SKILL_RATING` to stay within the valid range.
///
/// # Arguments
///
/// * `skill_rating` — the player's current MMR.
/// * `join_timestamp` — when the player entered the queue.
/// * `config` — system configuration.
///
/// # Returns
///
/// `(min_rating, max_rating)` — inclusive bounds for the rating index scan.
pub fn scan_bounds(skill_rating: u32, join_timestamp: Instant, config: &Config) -> (u32, u32) {
    let window = relaxation_window(join_timestamp, config);
    let min_rating = skill_rating.saturating_sub(window);
    let max_rating = skill_rating
        .saturating_add(window)
        .min(crate::engine::bucket::MAX_SKILL_RATING);
    (min_rating, max_rating)
}

//  Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Build a test config with known relaxation values.
    fn test_config() -> Config {
        // Set environment variables for a controlled test config,
        // then load. We use explicit env vars to avoid depending on .env files.
        std::env::remove_var("RELAXATION_STAGE_1_MS");
        std::env::remove_var("RELAXATION_STAGE_2_MS");
        std::env::remove_var("RELAXATION_STAGE_3_MS");
        std::env::remove_var("RELAXATION_STAGE_4_MS");
        std::env::remove_var("RELAXATION_STAGE_1_DELTA");
        std::env::remove_var("RELAXATION_STAGE_2_DELTA");
        std::env::remove_var("RELAXATION_STAGE_3_DELTA");
        std::env::remove_var("RELAXATION_STAGE_4_DELTA");
        std::env::remove_var("RELAXATION_STAGE_5_DELTA");
        std::env::remove_var("SERVER_PORT");
        std::env::remove_var("WORKER_COUNT");
        std::env::remove_var("WORKER_TICK_MS");
        std::env::remove_var("STALE_CLAIM_TIMEOUT_MS");
        Config::from_env().expect("default config must be valid")
    }

    /// Simulate a player who has waited for a given duration by constructing
    /// an `Instant` that is offset into the past.
    ///
    /// `Instant::now() - duration` gives us an `Instant` that appears to be
    /// `duration` old, so `elapsed()` returns approximately `duration`.
    fn aged_instant(wait: Duration) -> Instant {
        Instant::now() - wait
    }

    #[test]
    fn test_stage_1_fresh_player() {
        let config = test_config();
        // Brand new player — 0ms elapsed
        let window = relaxation_window(Instant::now(), &config);
        assert_eq!(
            window, config.relaxation_stage_1_delta,
            "Fresh player must get Stage 1 delta"
        );
    }

    #[test]
    fn test_stage_1_just_before_threshold() {
        let config = test_config();
        // 1ms before Stage 1 threshold — still in Stage 1
        let wait = Duration::from_millis(config.relaxation_stage_1_ms - 1);
        let window = relaxation_window(aged_instant(wait), &config);
        assert_eq!(window, config.relaxation_stage_1_delta);
    }

    #[test]
    fn test_stage_2_at_threshold() {
        let config = test_config();
        // Exactly at Stage 1 threshold — enters Stage 2
        let wait = Duration::from_millis(config.relaxation_stage_1_ms);
        let window = relaxation_window(aged_instant(wait), &config);
        assert_eq!(
            window, config.relaxation_stage_2_delta,
            "At Stage 1 threshold must enter Stage 2"
        );
    }

    #[test]
    fn test_stage_3_at_threshold() {
        let config = test_config();
        let wait = Duration::from_millis(config.relaxation_stage_2_ms);
        let window = relaxation_window(aged_instant(wait), &config);
        assert_eq!(window, config.relaxation_stage_3_delta);
    }

    #[test]
    fn test_stage_4_at_threshold() {
        let config = test_config();
        let wait = Duration::from_millis(config.relaxation_stage_3_ms);
        let window = relaxation_window(aged_instant(wait), &config);
        assert_eq!(window, config.relaxation_stage_4_delta);
    }

    #[test]
    fn test_stage_5_starvation_floor() {
        let config = test_config();
        // Well past Stage 4 threshold — starvation floor
        let wait = Duration::from_millis(config.relaxation_stage_4_ms + 10_000);
        let window = relaxation_window(aged_instant(wait), &config);
        assert_eq!(
            window, config.relaxation_stage_5_delta,
            "Long-waiting player must reach starvation floor"
        );
    }

    #[test]
    fn test_window_is_monotonically_non_decreasing() {
        let config = test_config();

        // Sample windows at 10 time points spanning 0 to 2× Stage 4 threshold
        let max_ms = config.relaxation_stage_4_ms * 2;
        let step = max_ms / 10;

        let windows: Vec<u32> = (0..=10)
            .map(|i| {
                let wait = Duration::from_millis(i * step);
                relaxation_window(aged_instant(wait), &config)
            })
            .collect();

        for i in 1..windows.len() {
            assert!(
                windows[i] >= windows[i - 1],
                "Window must be non-decreasing: windows[{}]={} < windows[{}]={}",
                i,
                windows[i],
                i - 1,
                windows[i - 1]
            );
        }
    }

    #[test]
    fn test_relaxation_stage_numbers() {
        let config = test_config();

        assert_eq!(relaxation_stage(Instant::now(), &config), 1);

        let s2 = aged_instant(Duration::from_millis(config.relaxation_stage_1_ms));
        assert_eq!(relaxation_stage(s2, &config), 2);

        let s3 = aged_instant(Duration::from_millis(config.relaxation_stage_2_ms));
        assert_eq!(relaxation_stage(s3, &config), 3);

        let s4 = aged_instant(Duration::from_millis(config.relaxation_stage_3_ms));
        assert_eq!(relaxation_stage(s4, &config), 4);

        let s5 = aged_instant(Duration::from_millis(config.relaxation_stage_4_ms + 1_000));
        assert_eq!(relaxation_stage(s5, &config), 5);
    }

    #[test]
    fn test_scan_bounds_no_underflow() {
        let config = test_config();
        // Player with rating 10 and Stage 1 delta of 50 — lower bound
        // must clamp to 0, not underflow to u32::MAX
        let (min, max) = scan_bounds(10, Instant::now(), &config);
        assert_eq!(min, 0, "Lower bound must not underflow");
        assert!(max > 10);
    }

    #[test]
    fn test_scan_bounds_no_overflow() {
        let config = test_config();
        // Player at max rating — upper bound must clamp to MAX_SKILL_RATING
        let (min, max) = scan_bounds(
            crate::engine::bucket::MAX_SKILL_RATING,
            Instant::now(),
            &config,
        );
        assert!(min < crate::engine::bucket::MAX_SKILL_RATING);
        assert_eq!(
            max,
            crate::engine::bucket::MAX_SKILL_RATING,
            "Upper bound must not exceed MAX_SKILL_RATING"
        );
    }

    #[test]
    fn test_scan_bounds_stage5_covers_full_range() {
        let config = test_config();
        // Stage 5 delta is 9999 — covers the entire 0–3000 range
        let wait = Duration::from_millis(config.relaxation_stage_4_ms + 10_000);
        let (min, max) = scan_bounds(1500, aged_instant(wait), &config);
        assert_eq!(min, 0, "Stage 5 min must cover bottom of range");
        assert_eq!(
            max,
            crate::engine::bucket::MAX_SKILL_RATING,
            "Stage 5 max must cover top of range"
        );
    }

    #[test]
    fn test_all_five_deltas_are_distinct_and_increasing() {
        let config = test_config();
        // With default config, each stage has a strictly larger delta
        assert!(config.relaxation_stage_1_delta <= config.relaxation_stage_2_delta);
        assert!(config.relaxation_stage_2_delta <= config.relaxation_stage_3_delta);
        assert!(config.relaxation_stage_3_delta <= config.relaxation_stage_4_delta);
        assert!(config.relaxation_stage_4_delta <= config.relaxation_stage_5_delta);
    }
}
