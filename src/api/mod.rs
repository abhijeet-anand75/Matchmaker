//! Axum HTTP API layer — four endpoints.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::engine::{CancelError, EnqueueError, MatchmakerCore};
use crate::models::{
    EnqueueRequest, EnqueueResponse, ErrorResponse, HealthResponse, MatchesResponse,
};

//  App state 

#[derive(Clone)]
pub struct AppState {
    pub core: Arc<MatchmakerCore>,
}

//  Router 

pub fn create_router(core: Arc<MatchmakerCore>) -> Router {
    let state = AppState { core };

    Router::new()
        .route("/enqueue", post(enqueue_handler))
        .route("/enqueue/:player_id", delete(cancel_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/matches", get(matches_handler))
        .with_state(state)
}

//  Error type 

pub enum AppError {
    BadRequest(String),
    Conflict(String),
    NotFound(String),
    UnprocessableEntity(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error, code) = match self {
            AppError::BadRequest(msg) => {
                (StatusCode::BAD_REQUEST, msg, "BAD_REQUEST")
            }
            AppError::Conflict(msg) => {
                (StatusCode::CONFLICT, msg, "CONFLICT")
            }
            AppError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, msg, "NOT_FOUND")
            }
            AppError::UnprocessableEntity(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, msg, "VALIDATION_ERROR")
            }
        };

        (status, Json(ErrorResponse::new(error, code))).into_response()
    }
}

//  Handlers 

/// POST /enqueue — add a player to the matchmaking queue.
async fn enqueue_handler(
    State(state): State<AppState>,
    Json(body): Json<EnqueueRequest>,
) -> Result<(StatusCode, Json<EnqueueResponse>), AppError> {
    if body.skill_rating > crate::engine::bucket::MAX_SKILL_RATING {
        return Err(AppError::UnprocessableEntity(format!(
            "skill_rating must be between 0 and {}",
            crate::engine::bucket::MAX_SKILL_RATING
        )));
    }

    match state.core.enqueue(body.id, body.skill_rating) {
        Ok(queue_depth) => Ok((
            StatusCode::OK,
            Json(EnqueueResponse {
                player_id: body.id,
                status: "queued",
                queue_depth,
            }),
        )),
        Err(EnqueueError::AlreadyQueued(id)) => {
            Err(AppError::Conflict(format!("Player {id} is already queued")))
        }
        Err(EnqueueError::InvalidSkillRating(r, max)) => Err(AppError::UnprocessableEntity(
            format!("skill_rating {r} exceeds maximum {max}"),
        )),
    }
}

/// DELETE /enqueue/:player_id — remove a player from the queue.
async fn cancel_handler(
    State(state): State<AppState>,
    Path(player_id): Path<Uuid>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    match state.core.cancel(&player_id) {
        Ok(()) => Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "player_id": player_id,
                "status": "cancelled"
            })),
        )),
        Err(CancelError::NotFound(id)) => {
            Err(AppError::NotFound(format!("Player {id} not found")))
        }
        Err(CancelError::CurrentlyBeingMatched(id)) => Err(AppError::Conflict(format!(
            "Player {id} is currently being matched — retry shortly"
        ))),
    }
}

/// GET /health — liveness probe.
async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        players_waiting: state.core.players_waiting(),
        uptime_secs: state.core.uptime_secs(),
    })
}

/// GET /metrics — operational metrics snapshot.
async fn metrics_handler(
    State(state): State<AppState>,
) -> Json<crate::metrics::MetricsSnapshot> {
    Json(state.core.metrics_snapshot())
}

/// Query parameters for GET /matches.
#[derive(Deserialize)]
pub struct MatchesQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

/// GET /matches — recent match results.
async fn matches_handler(
    State(state): State<AppState>,
    Query(params): Query<MatchesQuery>,
) -> Result<Json<MatchesResponse>, AppError> {
    let limit = params.limit.max(1);
    let matches = state.core.recent_matches(limit);
    let total = state.core.total_matches_formed();

    Ok(Json(MatchesResponse {
        matches,
        total_matches_formed: total,
    }))
}

//  Tests 

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Method, Request},
    };
    use tower::util::ServiceExt;

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

    fn make_router() -> Router {
        clear_env();
        let config = Arc::new(crate::config::Config::from_env().unwrap());
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let core = Arc::new(MatchmakerCore::new(config, metrics));
        create_router(core)
    }

    async fn post_json(router: &Router, uri: &str, body: serde_json::Value) -> Response {
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn get_uri(router: &Router, uri: &str) -> Response {
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_enqueue_valid_player() {
        let router = make_router();
        let id = Uuid::new_v4();
        let resp = post_json(
            &router,
            "/enqueue",
            serde_json::json!({ "id": id, "skill_rating": 1000 }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_enqueue_duplicate_returns_409() {
        let router = make_router();
        let id = Uuid::new_v4();
        let body = serde_json::json!({ "id": id, "skill_rating": 1000 });

        post_json(&router, "/enqueue", body.clone()).await;
        let resp = post_json(&router, "/enqueue", body).await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_enqueue_invalid_skill_rating_returns_422() {
        let router = make_router();
        let resp = post_json(
            &router,
            "/enqueue",
            serde_json::json!({ "id": Uuid::new_v4(), "skill_rating": 9999 }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn test_enqueue_malformed_body_returns_400() {
        let router = make_router();
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/enqueue")
                    .header("content-type", "application/json")
                    .body(Body::from(b"not json".as_ref()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_cancel_known_player() {
        let router = make_router();
        let id = Uuid::new_v4();
        post_json(
            &router,
            "/enqueue",
            serde_json::json!({ "id": id, "skill_rating": 1000 }),
        )
        .await;

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/enqueue/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_cancel_unknown_player_returns_404() {
        let router = make_router();
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/enqueue/{}", Uuid::new_v4()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_health_returns_ok() {
        let router = make_router();
        let resp = get_uri(&router, "/health").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_metrics_returns_all_fields() {
        let router = make_router();
        let resp = get_uri(&router, "/metrics").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("total_players_enqueued").is_some());
        assert!(json.get("total_matches_created").is_some());
        assert!(json.get("current_queue_size").is_some());
        assert!(json.get("avg_wait_ms").is_some());
    }

    #[tokio::test]
    async fn test_matches_returns_empty_list_initially() {
        let router = make_router();
        let resp = get_uri(&router, "/matches").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["matches"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_matches_limit_clamped() {
        let router = make_router();
        let resp = get_uri(&router, "/matches?limit=999").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
}