//! Shared test infrastructure for the matchmaker test suite.
//!
//! Provides:
//! - Environment setup helpers (fast relaxation config for tests)
//! - Player factory functions
//! - Pool seeding utilities
//! - Full Axum test server builder
//! - Assertion helpers for match validation
#![allow(dead_code)]

use std::sync::Arc;

use uuid::Uuid;

use matchmaker::api::create_router;
use matchmaker::config::Config;
use matchmaker::engine::matcher::WorkerContext;
use matchmaker::engine::MatchmakerCore;
use matchmaker::metrics::Metrics;
use matchmaker::models::{Match, Player};

// ── Environment helpers ───────────────────────────────────────────────────────

/// Clear all matchmaker environment variables.
///
/// Must be called before constructing any `Config` in tests to ensure
/// no leftover variables from a previous test pollute the environment.
///
/// NOTE: env var manipulation is process-global. Tests that set env vars
/// must call `clear_env()` in both setup and teardown.
pub fn clear_env() {
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

/// Load the default config for tests without touching global env vars.
pub fn default_config() -> Arc<Config> {
    Arc::new(Config {
        server_port: 8080,
        worker_count: 4,
        worker_tick_ms: 50,
        stale_claim_timeout_ms: 500,
        relaxation_stage_1_ms: 5_000,
        relaxation_stage_2_ms: 15_000,
        relaxation_stage_3_ms: 30_000,
        relaxation_stage_4_ms: 60_000,
        relaxation_stage_1_delta: 50,
        relaxation_stage_2_delta: 100,
        relaxation_stage_3_delta: 200,
        relaxation_stage_4_delta: 400,
        relaxation_stage_5_delta: 9999,
    })
}

/// Load the fast-relaxation config for tests without touching global env vars.
pub fn fast_config() -> Arc<Config> {
    Arc::new(Config {
        server_port: 9090,
        worker_count: 2,
        worker_tick_ms: 20,
        stale_claim_timeout_ms: 200,
        relaxation_stage_1_ms: 50,
        relaxation_stage_2_ms: 100,
        relaxation_stage_3_ms: 200,
        relaxation_stage_4_ms: 400,
        relaxation_stage_1_delta: 50,
        relaxation_stage_2_delta: 100,
        relaxation_stage_3_delta: 200,
        relaxation_stage_4_delta: 400,
        relaxation_stage_5_delta: 9999,
    })
}

// ── Core builder ──────────────────────────────────────────────────────────────

/// Build a `MatchmakerCore` with the default config and fresh metrics.
pub fn make_core() -> Arc<MatchmakerCore> {
    let config = default_config();
    let metrics = Arc::new(Metrics::new());
    Arc::new(MatchmakerCore::new(config, metrics))
}

/// Build a `MatchmakerCore` with fast relaxation config.
pub fn make_fast_core() -> Arc<MatchmakerCore> {
    let config = fast_config();
    let metrics = Arc::new(Metrics::new());
    Arc::new(MatchmakerCore::new(config, metrics))
}

/// Build a `WorkerContext` from a `MatchmakerCore` for direct `attempt_match` calls.
pub fn make_worker_ctx(core: &Arc<MatchmakerCore>, worker_id: u64) -> WorkerContext {
    core.make_worker_context(worker_id)
}

// ── Test Axum server ──────────────────────────────────────────────────────────

/// A running test HTTP server with its base URL.
pub struct TestServer {
    pub base_url: String,
    pub core: Arc<MatchmakerCore>,
    _handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Spin up a full Axum server bound to a random port on localhost.
    ///
    /// Returns immediately — the server runs in a background Tokio task.
    /// The server shuts down when `TestServer` is dropped (task is aborted).
    pub async fn start() -> Self {
        clear_env();
        let config = Arc::new(Config::from_env().expect("config must be valid"));
        let metrics = Arc::new(Metrics::new());
        let core = Arc::new(MatchmakerCore::new(
            Arc::clone(&config),
            Arc::clone(&metrics),
        ));
        let router = create_router(Arc::clone(&core));

        // Bind to port 0 — OS assigns a random available port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind test server");

        let addr = listener.local_addr().expect("Failed to get local address");
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("Test server failed");
        });

        Self {
            base_url,
            core,
            _handle: handle,
        }
    }

    /// Enqueue a player via HTTP POST /enqueue.
    pub async fn enqueue(&self, id: Uuid, skill_rating: u32) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/enqueue", self.base_url))
            .json(&serde_json::json!({ "id": id, "skill_rating": skill_rating }))
            .send()
            .await
            .expect("POST /enqueue request failed")
    }

    /// Cancel a player via HTTP DELETE /enqueue/:id.
    pub async fn cancel(&self, id: Uuid) -> reqwest::Response {
        reqwest::Client::new()
            .delete(format!("{}/enqueue/{}", self.base_url, id))
            .send()
            .await
            .expect("DELETE /enqueue request failed")
    }

    /// GET /health
    pub async fn health(&self) -> reqwest::Response {
        reqwest::Client::new()
            .get(format!("{}/health", self.base_url))
            .send()
            .await
            .expect("GET /health request failed")
    }

    /// GET /metrics
    pub async fn metrics(&self) -> reqwest::Response {
        reqwest::Client::new()
            .get(format!("{}/metrics", self.base_url))
            .send()
            .await
            .expect("GET /metrics request failed")
    }

    /// GET /matches with optional limit
    pub async fn matches(&self, limit: Option<usize>) -> reqwest::Response {
        let url = match limit {
            Some(l) => format!("{}/matches?limit={}", self.base_url, l),
            None => format!("{}/matches", self.base_url),
        };
        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("GET /matches request failed")
    }
}

// ── Player factory ────────────────────────────────────────────────────────────

/// Create a player with a specific skill rating.
pub fn make_player(skill_rating: u32) -> Arc<Player> {
    Arc::new(Player::new(Uuid::new_v4(), skill_rating))
}

/// Create a player with a specific ID and skill rating.
pub fn make_player_with_id(id: Uuid, skill_rating: u32) -> Arc<Player> {
    Arc::new(Player::new(id, skill_rating))
}

/// Create N players all at the same skill rating.
pub fn make_players_uniform(count: usize, skill_rating: u32) -> Vec<Arc<Player>> {
    (0..count).map(|_| make_player(skill_rating)).collect()
}

/// Create N players with skill ratings spread evenly across a range.
pub fn make_players_spread(count: usize, min_rating: u32, max_rating: u32) -> Vec<Arc<Player>> {
    (0..count)
        .map(|i| {
            let rating = if count == 1 {
                min_rating
            } else {
                min_rating + (max_rating - min_rating) * i as u32 / (count - 1) as u32
            };
            make_player(rating)
        })
        .collect()
}

/// Create 10 players suitable for an immediate match at the given base rating.
/// Ratings are within ±25 of `base_rating` — within Stage 1 window (±50).
pub fn make_match_ready_players(base_rating: u32) -> Vec<Arc<Player>> {
    (0..10).map(|i| make_player(base_rating + i * 5)).collect()
}

// ── Pool seeding ──────────────────────────────────────────────────────────────

/// Insert a slice of players into a `MatchmakerCore`.
pub fn seed_core(core: &Arc<MatchmakerCore>, players: &[Arc<Player>]) {
    for p in players {
        core.enqueue(p.id, p.skill_rating)
            .expect("Seed enqueue must succeed");
    }
}

/// Insert N players at a uniform rating directly into a core.
pub fn seed_uniform(core: &Arc<MatchmakerCore>, count: usize, rating: u32) {
    for _ in 0..count {
        core.enqueue(Uuid::new_v4(), rating)
            .expect("Seed enqueue must succeed");
    }
}

// ── Match assertion helpers ───────────────────────────────────────────────────

/// Assert that a `Match` is structurally valid:
/// - Exactly 5 players per team
/// - No player appears in both teams
/// - `team_delta` matches computed sums
pub fn assert_match_valid(m: &Match) {
    assert_eq!(
        m.team_a.players.len(),
        5,
        "team_a must have exactly 5 players"
    );
    assert_eq!(
        m.team_b.players.len(),
        5,
        "team_b must have exactly 5 players"
    );

    let a_ids: std::collections::HashSet<Uuid> = m.team_a.players.iter().map(|p| p.id).collect();
    let b_ids: std::collections::HashSet<Uuid> = m.team_b.players.iter().map(|p| p.id).collect();

    assert!(
        a_ids.is_disjoint(&b_ids),
        "No player may appear in both teams"
    );

    let a_sum: u32 = m.team_a.players.iter().map(|p| p.skill_rating).sum();
    let b_sum: u32 = m.team_b.players.iter().map(|p| p.skill_rating).sum();
    let expected_delta = a_sum.abs_diff(b_sum);

    assert_eq!(
        m.team_delta, expected_delta,
        "team_delta must equal |sum_a - sum_b|"
    );
}

/// Assert that no UUID appears more than once across all matches.
pub fn assert_no_duplicates(matches: &[Match]) {
    let mut seen: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
    for m in matches {
        for player in m.team_a.players.iter().chain(m.team_b.players.iter()) {
            assert!(
                seen.insert(player.id),
                "Player {} appears in more than one match — duplicate assignment detected",
                player.id
            );
        }
    }
}

/// Assert that the team delta is optimal for the given 10 players.
/// Independently brute-forces the minimum possible delta and compares.
pub fn assert_team_delta_is_optimal(m: &Match) {
    let all_ratings: Vec<u32> = m
        .team_a
        .players
        .iter()
        .chain(m.team_b.players.iter())
        .map(|p| p.skill_rating)
        .collect();

    assert_eq!(all_ratings.len(), 10);
    let total: u32 = all_ratings.iter().sum();

    let min_possible = (0u16..1024u16)
        .filter(|m| m.count_ones() == 5)
        .map(|mask| {
            let a: u32 = (0..10)
                .filter(|&i| (mask >> i) & 1 == 1)
                .map(|i| all_ratings[i])
                .sum();
            (2 * a).abs_diff(total)
        })
        .min()
        .unwrap_or(u32::MAX);

    assert_eq!(
        m.team_delta, min_possible,
        "team_delta {} is not optimal — minimum possible is {}",
        m.team_delta, min_possible
    );
}
