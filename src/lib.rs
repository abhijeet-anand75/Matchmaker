//! # Matchmaker
//!
//! A high-performance, thread-safe 5v5 competitive matchmaking engine.
//!
//! ## Architecture
//!
//! The engine is built around four core components:
//!
//! - **[`engine`]**: The matchmaking core — player pool (DashMap + BTreeMap),
//!   candidate discovery, constraint relaxation, atomic claiming, and team balancing.
//! - **[`workers`]**: Fixed Tokio task pool that drives the matchmaking loop,
//!   plus a background Reaper task for worker-crash recovery.
//! - **[`api`]**: Axum HTTP layer — enqueue, cancel, health, metrics, and match history.
//! - **[`metrics`]**: Lock-free atomic counters exposed as a side-channel.
//!   Never contends with matchmaking workers.
//!
//! ## Player Lifecycle
//!
//! ```text
//! Enqueued → Waiting(0) → Claimed(1) → Matched(2) → Evicted from pool
//!                    ↘ Evicted(3)   ↗ (rollback or reaper reset)
//! ```
//!
//! ## Concurrency Model
//!
//! All shared state is protected by one of:
//! - DashMap internal sharding (player primary store)
//! - `RwLock<BTreeMap>` (rating index — shared reads, brief exclusive writes)
//! - `AtomicU8` CAS (player state ownership — the correctness invariant)
//! - `AtomicU64` / `AtomicI64` (all metrics — zero lock involvement)

// ── Module declarations ───────────────────────────────────────────────────────
// All modules are public to allow integration tests in tests/ to access
// internal types directly via `matchmaker::engine::PlayerPool` etc.

pub mod api;
pub mod config;
pub mod engine;
pub mod metrics;
pub mod models;
pub mod utils;
pub mod workers;

// ── Top-level re-exports ──────────────────────────────────────────────────────
// The most commonly needed types are re-exported at the crate root for
// ergonomic use in tests and external consumers.

pub use config::Config;
pub use engine::MatchmakerCore;
pub use metrics::Metrics;
pub use models::{Match, Player, player_state};