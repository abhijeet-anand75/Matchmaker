//! Lock-free atomic metrics counters.
//!
//! All metrics are maintained as [`AtomicU64`] or [`AtomicI64`] fields.
//! No locks are ever acquired to read or write metrics. This guarantees
//! that metrics collection never contends with matchmaking workers.
//!
//! # Design
//!
//! Metrics are updated as a side-effect of normal matchmaking operations:
//! - Workers call increment methods after each match attempt
//! - The enqueue/cancel path calls increment methods on state change
//! - The Reaper calls an increment method on each recovery
//!
//! The [`MetricsSnapshot`] struct captures a point-in-time read of all
//! counters. Because each counter is read independently (no atomic snapshot
//! across all fields), there is a theoretical inconsistency window between
//! reads. This is acceptable — metrics are advisory, not transactional.
//!
//! # Memory Ordering
//!
//! All counter updates use `Relaxed` ordering. Metrics do not participate
//! in any happens-before relationship with matchmaking correctness — they
//! are purely observational. `Relaxed` costs ~1ns per operation vs ~20ns
//! for a `Mutex` lock.
//!
//! # Rolling Averages
//!
//! Rolling averages (avg_wait_ms, avg_skill_spread, avg_team_delta) are
//! computed from accumulated sum + count pairs. The snapshot computes
//! `mean = sum / count` at read time. This avoids floating-point atomics
//! and keeps the hot path to integer operations only.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;

//  Metrics struct

/// All runtime metrics for the matchmaking service.
///
/// Always heap-allocated behind `Arc<Metrics>`. Constructed once at startup
/// and shared between all workers, the API layer, and the Reaper.
///
/// # Field categories
///
/// - **Counters** (`total_*`): monotonically increasing, never reset
/// - **Gauges** (`current_*`): can increase or decrease; use `AtomicI64`
///   to support safe subtraction without underflow
/// - **Accumulators** (`*_sum` + `*_count`): feed rolling average computation
pub struct Metrics {
    /// Total number of players who have ever joined the queue.
    pub total_players_enqueued: AtomicU64,

    /// Total number of players who cancelled their queue entry.
    pub total_players_cancelled: AtomicU64,

    /// Total number of matches successfully formed.
    pub total_matches_created: AtomicU64,

    /// Total number of players successfully placed in a match.
    /// Always `total_matches_created * 10` in a healthy system.
    pub total_players_matched: AtomicU64,

    /// Total number of match attempts that failed due to insufficient
    /// compatible candidates in the current relaxation window.
    pub match_attempts_insufficient: AtomicU64,

    /// Total number of match attempts that failed due to CAS contention —
    /// enough candidates were found but this worker couldn't claim 10.
    pub match_attempts_claim_failed: AtomicU64,

    /// Total number of worker event loop iterations (notify + tick wakes).
    pub worker_cycles_total: AtomicU64,

    /// Total number of stale claims reset by the Reaper task.
    /// A non-zero value indicates worker crashes or panics have occurred.
    pub total_stale_claims_recovered: AtomicU64,

    //  Gauges
    /// Current number of players in the queue (all states).
    /// Incremented on enqueue, decremented on match or cancel.
    /// `AtomicI64` to allow safe `fetch_sub` without underflow panics.
    pub current_queue_size: AtomicI64,

    //  Accumulators for rolling averages
    /// Sum of all player wait times across all matches (milliseconds).
    /// Divide by `total_players_matched` to get average wait per player.
    pub total_wait_time_ms: AtomicU64,

    /// Sum of skill spreads (max_mmr - min_mmr) across all formed matches.
    pub skill_spread_sum: AtomicU64,

    /// Number of matches contributing to `skill_spread_sum`.
    pub skill_spread_count: AtomicU64,

    /// Sum of team deltas (|team_a_total - team_b_total|) across all matches.
    pub team_delta_sum: AtomicU64,

    /// Number of matches contributing to `team_delta_sum`.
    pub team_delta_count: AtomicU64,
}

impl Metrics {
    /// Construct a new `Metrics` instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            total_players_enqueued: AtomicU64::new(0),
            total_players_cancelled: AtomicU64::new(0),
            total_matches_created: AtomicU64::new(0),
            total_players_matched: AtomicU64::new(0),
            match_attempts_insufficient: AtomicU64::new(0),
            match_attempts_claim_failed: AtomicU64::new(0),
            worker_cycles_total: AtomicU64::new(0),
            total_stale_claims_recovered: AtomicU64::new(0),
            current_queue_size: AtomicI64::new(0),
            total_wait_time_ms: AtomicU64::new(0),
            skill_spread_sum: AtomicU64::new(0),
            skill_spread_count: AtomicU64::new(0),
            team_delta_sum: AtomicU64::new(0),
            team_delta_count: AtomicU64::new(0),
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    ///
    /// Reads all atomic fields in sequence. There is no cross-field atomicity
    /// guarantee — this is intentional and documented. Metrics are advisory.
    ///
    /// Computes rolling averages from accumulated sum/count pairs.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let total_players_matched = self.total_players_matched.load(Ordering::Relaxed);

        let skill_spread_count = self.skill_spread_count.load(Ordering::Relaxed);
        let team_delta_count = self.team_delta_count.load(Ordering::Relaxed);

        let avg_wait_ms = self.total_wait_time_ms.load(Ordering::Relaxed)
            .checked_div(total_players_matched)
            .unwrap_or(0);

        let avg_skill_spread = self.skill_spread_sum.load(Ordering::Relaxed)
            .checked_div(skill_spread_count)
            .unwrap_or(0);

        let avg_team_delta = self.team_delta_sum.load(Ordering::Relaxed)
            .checked_div(team_delta_count)
            .unwrap_or(0);

        MetricsSnapshot {
            total_players_enqueued: self.total_players_enqueued.load(Ordering::Relaxed),
            total_players_cancelled: self.total_players_cancelled.load(Ordering::Relaxed),
            total_matches_created: self.total_matches_created.load(Ordering::Relaxed),
            total_players_matched,
            match_attempts_insufficient: self.match_attempts_insufficient.load(Ordering::Relaxed),
            match_attempts_claim_failed: self.match_attempts_claim_failed.load(Ordering::Relaxed),
            worker_cycles_total: self.worker_cycles_total.load(Ordering::Relaxed),
            total_stale_claims_recovered: self.total_stale_claims_recovered.load(Ordering::Relaxed),
            current_queue_size: self.current_queue_size.load(Ordering::Relaxed),
            total_wait_time_ms: self.total_wait_time_ms.load(Ordering::Relaxed),
            avg_wait_ms,
            avg_skill_spread,
            avg_team_delta,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

//  Snapshot

/// A point-in-time snapshot of all metrics — plain values, no atomics.
///
/// Constructed by [`Metrics::snapshot`] and serialized to JSON for the
/// `GET /metrics` endpoint. All computed fields (averages) are pre-calculated
/// in `snapshot()` so the API handler does zero arithmetic.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// Total players who have ever joined the queue.
    pub total_players_enqueued: u64,

    /// Total players who cancelled their queue entry.
    pub total_players_cancelled: u64,

    /// Total matches successfully formed.
    pub total_matches_created: u64,

    /// Total players successfully placed in matches.
    pub total_players_matched: u64,

    /// Match attempts that failed — insufficient compatible candidates.
    pub match_attempts_insufficient: u64,

    /// Match attempts that failed — CAS contention prevented claiming 10 players.
    pub match_attempts_claim_failed: u64,

    /// Total worker event loop iterations.
    pub worker_cycles_total: u64,

    /// Total stale claims recovered by the Reaper.
    pub total_stale_claims_recovered: u64,

    /// Current number of players in the queue.
    pub current_queue_size: i64,

    /// Total accumulated wait time across all matched players (ms).
    pub total_wait_time_ms: u64,

    /// Average wait time per matched player (ms).
    /// `0` if no players have been matched yet.
    pub avg_wait_ms: u64,

    /// Average skill spread per match (max_mmr - min_mmr across 10 players).
    /// `0` if no matches have been formed yet.
    pub avg_skill_spread: u64,

    /// Average team MMR delta per match (|team_a_total - team_b_total|).
    /// `0` if no matches have been formed yet.
    pub avg_team_delta: u64,
}

//  Constructor helper

impl Metrics {
    /// Construct a new `Arc<Metrics>` — the standard way to create metrics
    /// for distribution to all components.
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

//  Tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_initial_snapshot_all_zeros() {
        let m = Metrics::new();
        let s = m.snapshot();

        assert_eq!(s.total_players_enqueued, 0);
        assert_eq!(s.total_players_cancelled, 0);
        assert_eq!(s.total_matches_created, 0);
        assert_eq!(s.total_players_matched, 0);
        assert_eq!(s.match_attempts_insufficient, 0);
        assert_eq!(s.match_attempts_claim_failed, 0);
        assert_eq!(s.worker_cycles_total, 0);
        assert_eq!(s.total_stale_claims_recovered, 0);
        assert_eq!(s.current_queue_size, 0);
        assert_eq!(s.total_wait_time_ms, 0);
        assert_eq!(s.avg_wait_ms, 0);
        assert_eq!(s.avg_skill_spread, 0);
        assert_eq!(s.avg_team_delta, 0);
    }

    #[test]
    fn test_counter_increments() {
        let m = Metrics::new();

        m.total_players_enqueued.fetch_add(5, Ordering::Relaxed);
        m.total_matches_created.fetch_add(2, Ordering::Relaxed);
        m.total_players_matched.fetch_add(20, Ordering::Relaxed);

        let s = m.snapshot();
        assert_eq!(s.total_players_enqueued, 5);
        assert_eq!(s.total_matches_created, 2);
        assert_eq!(s.total_players_matched, 20);
    }

    #[test]
    fn test_gauge_increment_and_decrement() {
        let m = Metrics::new();

        m.current_queue_size.fetch_add(10, Ordering::Relaxed);
        assert_eq!(m.snapshot().current_queue_size, 10);

        m.current_queue_size.fetch_sub(3, Ordering::Relaxed);
        assert_eq!(m.snapshot().current_queue_size, 7);

        m.current_queue_size.fetch_sub(7, Ordering::Relaxed);
        assert_eq!(m.snapshot().current_queue_size, 0);
    }

    #[test]
    fn test_avg_wait_ms_computed_correctly() {
        let m = Metrics::new();

        // 10 players, total wait = 10_000ms → avg = 1_000ms
        m.total_players_matched.fetch_add(10, Ordering::Relaxed);
        m.total_wait_time_ms.fetch_add(10_000, Ordering::Relaxed);

        let s = m.snapshot();
        assert_eq!(s.avg_wait_ms, 1_000);
    }

    #[test]
    fn test_avg_skill_spread_computed_correctly() {
        let m = Metrics::new();

        // 3 matches with spreads: 100, 200, 300 → avg = 200
        m.skill_spread_sum.fetch_add(600, Ordering::Relaxed);
        m.skill_spread_count.fetch_add(3, Ordering::Relaxed);

        let s = m.snapshot();
        assert_eq!(s.avg_skill_spread, 200);
    }

    #[test]
    fn test_avg_team_delta_computed_correctly() {
        let m = Metrics::new();

        // 2 matches with deltas: 50, 150 → avg = 100
        m.team_delta_sum.fetch_add(200, Ordering::Relaxed);
        m.team_delta_count.fetch_add(2, Ordering::Relaxed);

        let s = m.snapshot();
        assert_eq!(s.avg_team_delta, 100);
    }

    #[test]
    fn test_avg_fields_zero_when_no_matches() {
        let m = Metrics::new();
        let s = m.snapshot();

        // Division by zero must not occur — all averages return 0
        assert_eq!(s.avg_wait_ms, 0);
        assert_eq!(s.avg_skill_spread, 0);
        assert_eq!(s.avg_team_delta, 0);
    }

    #[test]
    fn test_snapshot_serializes_to_json() {
        let m = Metrics::new();
        m.total_players_enqueued.fetch_add(42, Ordering::Relaxed);
        m.total_matches_created.fetch_add(4, Ordering::Relaxed);

        let s = m.snapshot();
        let json = serde_json::to_string(&s).expect("MetricsSnapshot must serialize");

        assert!(json.contains("total_players_enqueued"));
        assert!(json.contains("42"));
        assert!(json.contains("total_matches_created"));
        assert!(json.contains("4"));
    }

    #[test]
    fn test_concurrent_counter_updates_are_correct() {
        let m = Arc::new(Metrics::new());
        let mut handles = Vec::new();

        for _ in 0..10 {
            let m = Arc::clone(&m);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    m.total_players_enqueued.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            m.snapshot().total_players_enqueued,
            1_000,
            "Concurrent increments must be loss-free"
        );
    }

    #[test]
    fn test_stale_claims_counter() {
        let m = Metrics::new();
        m.total_stale_claims_recovered
            .fetch_add(3, Ordering::Relaxed);

        assert_eq!(m.snapshot().total_stale_claims_recovered, 3);
    }

    #[test]
    fn test_new_shared_returns_arc() {
        let m = Metrics::new_shared();
        assert_eq!(Arc::strong_count(&m), 1);
        let m2 = Arc::clone(&m);
        assert_eq!(Arc::strong_count(&m), 2);
        drop(m2);
        assert_eq!(Arc::strong_count(&m), 1);
    }
}
