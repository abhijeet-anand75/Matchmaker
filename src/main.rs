//! Matchmaker service entry point.
//!
//! Responsibilities:
//! 1. Load and validate configuration
//! 2. Initialise shared state (MatchmakerCore, Metrics)
//! 3. Spawn worker pool and Reaper via [`workers::spawn_all`]
//! 4. Start the Axum HTTP server
//! 5. Wait for SIGTERM or SIGINT
//! 6. Graceful shutdown: cancel workers, drain HTTP, join tasks

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use matchmaker::api::create_router;
use matchmaker::config::Config;
use matchmaker::engine::MatchmakerCore;
use matchmaker::metrics::Metrics;
use matchmaker::workers::spawn_all;

// ── Shutdown timeouts ─────────────────────────────────────────────────────────

/// Maximum time to wait for HTTP connections to drain after shutdown signal.
const HTTP_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Maximum time to wait for all worker tasks to exit after cancellation.
const WORKER_JOIN_TIMEOUT_SECS: u64 = 10;

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // ── Logging ───────────────────────────────────────────────────────────────
    // Initialise structured tracing. Respects RUST_LOG environment variable.
    // Default: INFO level for the matchmaker crate, WARN for dependencies.
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("matchmaker=info,warn")),
        )
        .init();

    // ── Configuration ─────────────────────────────────────────────────────────
    // Fail fast with a descriptive message if any value is missing or invalid.
    let config = match Config::from_env() {
        Ok(cfg) => {
            info!("Configuration loaded: {cfg}");
            Arc::new(cfg)
        }
        Err(e) => {
            // Use eprintln — tracing may not be fully initialised yet.
            eprintln!("[FATAL] Configuration error: {e}");
            std::process::exit(1);
        }
    };

    // ── Core initialisation ───────────────────────────────────────────────────
    let metrics = Arc::new(Metrics::new());
    let core = Arc::new(MatchmakerCore::new(Arc::clone(&config), Arc::clone(&metrics)));

    info!(
        worker_count = config.worker_count,
        server_port = config.server_port,
        "MatchmakerCore initialised"
    );

    // ── Shutdown token ────────────────────────────────────────────────────────
    // Single CancellationToken propagated to all workers and the Reaper.
    let shutdown = CancellationToken::new();

    // ── Worker pool ───────────────────────────────────────────────────────────
    let mut worker_set = spawn_all(Arc::clone(&core), shutdown.clone());

    info!(
        workers = config.worker_count,
        "Worker pool started ({} workers + 1 reaper)",
        config.worker_count
    );

    // ── HTTP server ───────────────────────────────────────────────────────────
    let router = create_router(Arc::clone(&core));
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server_port));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(address = %addr, "HTTP server listening");
            l
        }
        Err(e) => {
            error!(address = %addr, error = %e, "Failed to bind TCP listener");
            // Cancel workers before exiting so they don't leak.
            shutdown.cancel();
            std::process::exit(1);
        }
    };

    // Build the Axum server with graceful shutdown wired to our token.
    let shutdown_token = shutdown.clone();
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        shutdown_token.cancelled().await;
        info!("HTTP server received shutdown — draining connections");
    });

    // ── Main select loop ──────────────────────────────────────────────────────
    // Run the server until a signal fires, then begin orderly shutdown.
    tokio::select! {
        result = server => {
            match result {
                Ok(()) => info!("HTTP server exited normally"),
                Err(e) => error!(error = %e, "HTTP server error"),
            }
        }

        _ = wait_for_signal() => {
            info!("Shutdown signal received — beginning graceful shutdown");
        }
    }

    // ── Graceful shutdown sequence ────────────────────────────────────────────

    // Step 1: Signal all workers and Reaper to stop.
    shutdown.cancel();
    info!("Shutdown token cancelled — workers will exit after current attempt");

    // Step 2: Wait for HTTP connections to drain.
    info!("Waiting up to {HTTP_DRAIN_TIMEOUT_SECS}s for HTTP connections to drain");
    tokio::time::sleep(Duration::from_secs(HTTP_DRAIN_TIMEOUT_SECS)).await;

    // Step 3: Join all worker tasks with a timeout.
    info!("Joining worker tasks (timeout: {WORKER_JOIN_TIMEOUT_SECS}s)");
    let join_deadline =
        tokio::time::Instant::now() + Duration::from_secs(WORKER_JOIN_TIMEOUT_SECS);

    loop {
        tokio::select! {
            result = worker_set.join_next() => {
                match result {
                    Some(Ok(())) => {
                        // Task exited cleanly.
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "Worker task panicked during shutdown");
                    }
                    None => {
                        // All tasks have been joined.
                        info!("All worker tasks joined cleanly");
                        break;
                    }
                }
            }

            _ = tokio::time::sleep_until(join_deadline) => {
                let remaining = worker_set.len();
                if remaining > 0 {
                    error!(
                        remaining,
                        "Worker join timeout — {remaining} tasks did not exit in time"
                    );
                }
                break;
            }
        }
    }

    // Step 4: Log final metrics snapshot.
    let snapshot = core.metrics_snapshot();
    info!(
        total_matches = snapshot.total_matches_created,
        total_players_matched = snapshot.total_players_matched,
        total_enqueued = snapshot.total_players_enqueued,
        avg_wait_ms = snapshot.avg_wait_ms,
        stale_recoveries = snapshot.total_stale_claims_recovered,
        "Final metrics at shutdown"
    );

    info!("Matchmaker shutdown complete");
}

// ── Signal handling ───────────────────────────────────────────────────────────

/// Wait for SIGTERM or SIGINT (Ctrl-C).
///
/// On Unix, listens for both signals. On Windows, only SIGINT is available.
async fn wait_for_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {
                info!("Received SIGINT (Ctrl-C)");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
        info!("Received SIGINT (Ctrl-C)");
    }
}