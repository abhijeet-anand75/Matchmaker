//! HTTP API layer tests.
//!
//! Validates all five confirmed endpoints against the running Axum server:
//! - POST /enqueue
//! - DELETE /enqueue/:player_id
//! - GET /health
//! - GET /metrics
//! - GET /matches
//!
//! Uses reqwest as the HTTP client against a real TCP server bound to
//! a random port. No mocking — real network stack, real Axum routing.

mod common;

use std::time::Duration;

use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use matchmaker::workers::spawn_all;

use common::TestServer;

// ── POST /enqueue ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_enqueue_valid_player_returns_200() {
    let server = TestServer::start().await;
    let id = Uuid::new_v4();
    let resp = server.enqueue(id, 1000).await;
    assert_eq!(resp.status(), 200, "Valid enqueue must return 200");
}

#[tokio::test]
async fn test_enqueue_response_contains_required_fields() {
    let server = TestServer::start().await;
    let id = Uuid::new_v4();
    let resp = server.enqueue(id, 1000).await;

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("Response must be JSON");

    assert_eq!(
        body["player_id"].as_str().unwrap(),
        id.to_string(),
        "Response must echo back the player_id"
    );
    assert_eq!(
        body["status"].as_str().unwrap(),
        "queued",
        "Response status must be 'queued'"
    );
    assert!(
        body["queue_depth"].as_u64().is_some(),
        "Response must contain queue_depth"
    );
}

#[tokio::test]
async fn test_enqueue_queue_depth_increments() {
    let server = TestServer::start().await;

    for expected_depth in 1..=5u64 {
        let resp = server.enqueue(Uuid::new_v4(), 1000).await;
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["queue_depth"].as_u64().unwrap(),
            expected_depth,
            "queue_depth must increment with each enqueue"
        );
    }
}

#[tokio::test]
async fn test_enqueue_duplicate_player_returns_409() {
    let server = TestServer::start().await;
    let id = Uuid::new_v4();

    let first = server.enqueue(id, 1000).await;
    assert_eq!(first.status(), 200);

    let second = server.enqueue(id, 1000).await;
    assert_eq!(
        second.status(),
        409,
        "Duplicate enqueue must return 409 Conflict"
    );

    let body: serde_json::Value = second.json().await.unwrap();
    assert!(
        body["error"].as_str().is_some(),
        "409 response must contain error field"
    );
    assert_eq!(body["code"].as_str().unwrap(), "CONFLICT");
}

#[tokio::test]
async fn test_enqueue_skill_rating_above_3000_returns_422() {
    let server = TestServer::start().await;
    let resp = server.enqueue(Uuid::new_v4(), 3001).await;
    assert_eq!(
        resp.status(),
        422,
        "skill_rating above 3000 must return 422"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"].as_str().unwrap(), "VALIDATION_ERROR");
}

#[tokio::test]
async fn test_enqueue_skill_rating_at_boundary_3000_returns_200() {
    let server = TestServer::start().await;
    let resp = server.enqueue(Uuid::new_v4(), 3000).await;
    assert_eq!(
        resp.status(),
        200,
        "skill_rating of exactly 3000 must be accepted"
    );
}

#[tokio::test]
async fn test_enqueue_skill_rating_zero_returns_200() {
    let server = TestServer::start().await;
    let resp = server.enqueue(Uuid::new_v4(), 0).await;
    assert_eq!(
        resp.status(),
        200,
        "skill_rating of 0 must be accepted"
    );
}

#[tokio::test]
async fn test_enqueue_malformed_json_returns_400() {
    let server = TestServer::start().await;

    let resp = reqwest::Client::new()
        .post(format!("{}/enqueue", server.base_url))
        .header("content-type", "application/json")
        .body("{ this is not valid json }")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "Malformed JSON body must return 400"
    );
}

#[tokio::test]
async fn test_enqueue_missing_skill_rating_returns_422() {
    let server = TestServer::start().await;

    let resp = reqwest::Client::new()
        .post(format!("{}/enqueue", server.base_url))
        .json(&serde_json::json!({ "id": Uuid::new_v4() }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        422,
        "Missing skill_rating field must return 422"
    );
}

#[tokio::test]
async fn test_enqueue_missing_id_returns_422() {
    let server = TestServer::start().await;

    let resp = reqwest::Client::new()
        .post(format!("{}/enqueue", server.base_url))
        .json(&serde_json::json!({ "skill_rating": 1000 }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        422,
        "Missing id field must return 422"
    );
}

#[tokio::test]
async fn test_enqueue_invalid_uuid_format_returns_422() {
    let server = TestServer::start().await;

    let resp = reqwest::Client::new()
        .post(format!("{}/enqueue", server.base_url))
        .json(&serde_json::json!({
            "id": "not-a-valid-uuid",
            "skill_rating": 1000
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        422,
        "Invalid UUID format must return 422"
    );
}

// ── DELETE /enqueue/:player_id ────────────────────────────────────────────────

#[tokio::test]
async fn test_cancel_queued_player_returns_200() {
    let server = TestServer::start().await;
    let id = Uuid::new_v4();

    server.enqueue(id, 1000).await;
    let resp = server.cancel(id).await;

    assert_eq!(resp.status(), 200, "Cancel of queued player must return 200");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["player_id"].as_str().unwrap(),
        id.to_string()
    );
    assert_eq!(body["status"].as_str().unwrap(), "cancelled");
}

#[tokio::test]
async fn test_cancel_unknown_player_returns_404() {
    let server = TestServer::start().await;
    let resp = server.cancel(Uuid::new_v4()).await;

    assert_eq!(
        resp.status(),
        404,
        "Cancel of unknown player must return 404"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"].as_str().unwrap(), "NOT_FOUND");
}

#[tokio::test]
async fn test_cancel_removes_player_from_queue() {
    let server = TestServer::start().await;
    let id = Uuid::new_v4();

    server.enqueue(id, 1000).await;

    // Verify depth is 1
    let health: serde_json::Value = server.health().await.json().await.unwrap();
    assert_eq!(health["players_waiting"].as_i64().unwrap(), 1);

    server.cancel(id).await;

    // Verify depth is 0
    let health: serde_json::Value = server.health().await.json().await.unwrap();
    assert_eq!(
        health["players_waiting"].as_i64().unwrap(),
        0,
        "Queue must be empty after cancel"
    );
}

#[tokio::test]
async fn test_cancel_already_matched_player_returns_404() {
    let server = TestServer::start().await;

    // Enqueue 10 players and start workers to form a match
    let id = Uuid::new_v4();
    server.enqueue(id, 1000).await;
    for _ in 0..9 {
        server.enqueue(Uuid::new_v4(), 1003).await;
    }

    // Start workers to match the players
    let shutdown = CancellationToken::new();
    let mut worker_set = spawn_all(
        std::sync::Arc::clone(&server.core),
        shutdown.clone(),
    );

    // Wait for match to form
    let matched = timeout(Duration::from_secs(5), async {
        loop {
            sleep(Duration::from_millis(50)).await;
            let snap = server.core.metrics_snapshot();
            if snap.total_matches_created >= 1 {
                return true;
            }
        }
    })
    .await;
    assert!(matched.is_ok(), "Match must form before cancel test");

    shutdown.cancel();
    while worker_set.join_next().await.is_some() {}

    // Now try to cancel an already-matched player
    let resp = server.cancel(id).await;
    assert_eq!(
        resp.status(),
        404,
        "Cancel of matched player must return 404 (player not in queue)"
    );
}

// ── GET /health ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_health_returns_200() {
    let server = TestServer::start().await;
    let resp = server.health().await;
    assert_eq!(resp.status(), 200, "GET /health must return 200");
}

#[tokio::test]
async fn test_health_response_contains_required_fields() {
    let server = TestServer::start().await;
    let body: serde_json::Value = server.health().await.json().await.unwrap();

    assert_eq!(
        body["status"].as_str().unwrap(),
        "ok",
        "health status must be 'ok'"
    );
    assert!(
        body["players_waiting"].as_i64().is_some(),
        "health must contain players_waiting"
    );
    assert!(
        body["uptime_secs"].as_u64().is_some(),
        "health must contain uptime_secs"
    );
}

#[tokio::test]
async fn test_health_players_waiting_reflects_queue() {
    let server = TestServer::start().await;

    let initial: serde_json::Value = server.health().await.json().await.unwrap();
    assert_eq!(initial["players_waiting"].as_i64().unwrap(), 0);

    server.enqueue(Uuid::new_v4(), 1000).await;
    server.enqueue(Uuid::new_v4(), 1000).await;
    server.enqueue(Uuid::new_v4(), 1000).await;

    let after: serde_json::Value = server.health().await.json().await.unwrap();
    assert_eq!(
        after["players_waiting"].as_i64().unwrap(),
        3,
        "players_waiting must reflect current queue size"
    );
}

#[tokio::test]
async fn test_health_uptime_is_non_negative() {
    let server = TestServer::start().await;
    let body: serde_json::Value = server.health().await.json().await.unwrap();
    let uptime = body["uptime_secs"].as_u64().unwrap();
    assert!(uptime < 3600, "Uptime must be less than 1 hour in tests");
}

// ── GET /metrics ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_metrics_returns_200() {
    let server = TestServer::start().await;
    let resp = server.metrics().await;
    assert_eq!(resp.status(), 200, "GET /metrics must return 200");
}

#[tokio::test]
async fn test_metrics_response_is_valid_json() {
    let server = TestServer::start().await;
    let resp = server.metrics().await;
    assert_eq!(resp.status(), 200);

    let body: Result<serde_json::Value, _> = resp.json().await;
    assert!(body.is_ok(), "GET /metrics response must be valid JSON");
}

#[tokio::test]
async fn test_metrics_contains_all_required_fields() {
    let server = TestServer::start().await;
    let body: serde_json::Value = server.metrics().await.json().await.unwrap();

    let required_fields = [
        "total_players_enqueued",
        "total_players_cancelled",
        "total_matches_created",
        "total_players_matched",
        "match_attempts_insufficient",
        "match_attempts_claim_failed",
        "worker_cycles_total",
        "avg_wait_ms",
        "avg_team_delta",
    ];

    for field in &required_fields {
        assert!(
            body.get(field).is_some(),
            "GET /metrics must contain field '{field}'"
        );
    }
}

#[tokio::test]
async fn test_metrics_field_types_are_correct() {
    let server = TestServer::start().await;

    // Enqueue some players so non-zero values appear
    for _ in 0..3 {
        server.enqueue(Uuid::new_v4(), 1000).await;
    }

    let body: serde_json::Value = server.metrics().await.json().await.unwrap();

    // All counter fields must be non-negative integers
    assert!(
        body["total_players_enqueued"].as_u64().is_some(),
        "total_players_enqueued must be a u64"
    );
    assert!(
        body["total_matches_created"].as_u64().is_some(),
        "total_matches_created must be a u64"
    );
    assert!(
        body["avg_wait_ms"].as_u64().is_some(),
        "avg_wait_ms must be a u64"
    );
    assert!(
        body["avg_team_delta"].as_u64().is_some(),
        "avg_team_delta must be a u64"
    );

    // current_queue_size is i64 — can be negative in theory but must be
    // deserializable as a signed integer
    assert!(
        body["current_queue_size"].as_i64().is_some(),
        "current_queue_size must be an i64"
    );
}

#[tokio::test]
async fn test_metrics_enqueued_count_matches_requests() {
    let server = TestServer::start().await;

    for _ in 0..7 {
        server.enqueue(Uuid::new_v4(), 1000).await;
    }

    let body: serde_json::Value = server.metrics().await.json().await.unwrap();
    assert_eq!(
        body["total_players_enqueued"].as_u64().unwrap(),
        7,
        "total_players_enqueued must match number of POST /enqueue calls"
    );
}

// ── GET /matches ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_matches_returns_200() {
    let server = TestServer::start().await;
    let resp = server.matches(None).await;
    assert_eq!(resp.status(), 200, "GET /matches must return 200");
}

#[tokio::test]
async fn test_matches_empty_initially() {
    let server = TestServer::start().await;
    let body: serde_json::Value = server.matches(None).await.json().await.unwrap();

    assert!(
        body["matches"].as_array().is_some(),
        "Response must contain 'matches' array"
    );
    assert_eq!(
        body["matches"].as_array().unwrap().len(),
        0,
        "matches array must be empty before any match forms"
    );
    assert_eq!(
        body["total_matches_formed"].as_u64().unwrap(),
        0,
        "total_matches_formed must be 0 initially"
    );
}

#[tokio::test]
async fn test_matches_response_structure_after_match() {
    let server = TestServer::start().await;

    // Enqueue 10 players and form a match via workers
    for i in 0..10u32 {
        server.enqueue(Uuid::new_v4(), 1000 + i * 3).await;
    }

    let shutdown = CancellationToken::new();
    let mut worker_set = spawn_all(
        std::sync::Arc::clone(&server.core),
        shutdown.clone(),
    );

    let matched = timeout(Duration::from_secs(5), async {
        loop {
            sleep(Duration::from_millis(50)).await;
            if server.core.metrics_snapshot().total_matches_created >= 1 {
                return true;
            }
        }
    })
    .await;
    assert!(matched.is_ok(), "Match must form before structure test");

    shutdown.cancel();
    while worker_set.join_next().await.is_some() {}

    let body: serde_json::Value = server.matches(None).await.json().await.unwrap();
    let matches = body["matches"].as_array().unwrap();

    assert_eq!(matches.len(), 1, "One match must be in history");
    assert_eq!(
        body["total_matches_formed"].as_u64().unwrap(),
        1
    );

    let m = &matches[0];

    // Match must have required top-level fields
    assert!(m["match_id"].as_str().is_some(), "match must have match_id");
    assert!(m["team_a"].is_object(), "match must have team_a");
    assert!(m["team_b"].is_object(), "match must have team_b");
    assert!(m["team_delta"].as_u64().is_some(), "match must have team_delta");
    assert!(m["skill_spread"].as_u64().is_some(), "match must have skill_spread");
    assert!(m["avg_wait_ms"].as_u64().is_some(), "match must have avg_wait_ms");

    // Each team must have exactly 5 players
    let team_a = m["team_a"]["players"].as_array().unwrap();
    let team_b = m["team_b"]["players"].as_array().unwrap();

    assert_eq!(team_a.len(), 5, "team_a must have 5 players");
    assert_eq!(team_b.len(), 5, "team_b must have 5 players");

    // Each player in team must have id, skill_rating, wait_ms
    for player in team_a.iter().chain(team_b.iter()) {
        assert!(
            player["id"].as_str().is_some(),
            "Each player must have an id"
        );
        assert!(
            player["skill_rating"].as_u64().is_some(),
            "Each player must have skill_rating"
        );
        assert!(
            player["wait_ms"].as_u64().is_some(),
            "Each player must have wait_ms"
        );
    }
}

#[tokio::test]
async fn test_matches_limit_parameter_respected() {
    let server = TestServer::start().await;

    // Form 3 matches
    let shutdown = CancellationToken::new();
    let mut worker_set = spawn_all(
        std::sync::Arc::clone(&server.core),
        shutdown.clone(),
    );

    for i in 0..30u32 {
        server.enqueue(Uuid::new_v4(), 1000 + (i % 20)).await;
    }

    let matched = timeout(Duration::from_secs(5), async {
        loop {
            sleep(Duration::from_millis(50)).await;
            if server.core.metrics_snapshot().total_matches_created >= 3 {
                return true;
            }
        }
    })
    .await;
    assert!(matched.is_ok(), "3 matches must form");

    shutdown.cancel();
    while worker_set.join_next().await.is_some() {}

    // Request only 1
    let body: serde_json::Value =
        server.matches(Some(1)).await.json().await.unwrap();
    let returned = body["matches"].as_array().unwrap().len();
    assert_eq!(returned, 1, "limit=1 must return exactly 1 match");

    // Request all 3
    let body: serde_json::Value =
        server.matches(Some(3)).await.json().await.unwrap();
    let returned = body["matches"].as_array().unwrap().len();
    assert_eq!(returned, 3, "limit=3 must return all 3 matches");
}

#[tokio::test]
async fn test_matches_no_duplicate_player_ids_in_response() {
    let server = TestServer::start().await;

    for i in 0..10u32 {
        server.enqueue(Uuid::new_v4(), 1000 + i * 3).await;
    }

    let shutdown = CancellationToken::new();
    let mut worker_set = spawn_all(
        std::sync::Arc::clone(&server.core),
        shutdown.clone(),
    );

    let matched = timeout(Duration::from_secs(5), async {
        loop {
            sleep(Duration::from_millis(50)).await;
            if server.core.metrics_snapshot().total_matches_created >= 1 {
                return true;
            }
        }
    })
    .await;
    assert!(matched.is_ok());

    shutdown.cancel();
    while worker_set.join_next().await.is_some() {}

    let body: serde_json::Value = server.matches(None).await.json().await.unwrap();
    let matches = body["matches"].as_array().unwrap();

    // Collect all player IDs from all matches
    let mut seen_ids = std::collections::HashSet::new();
    for m in matches {
        let team_a = m["team_a"]["players"].as_array().unwrap();
        let team_b = m["team_b"]["players"].as_array().unwrap();
        for player in team_a.iter().chain(team_b.iter()) {
            let id = player["id"].as_str().unwrap().to_string();
            assert!(
                seen_ids.insert(id.clone()),
                "Player {id} appears in multiple matches — duplicate detected in API response"
            );
        }
    }
}