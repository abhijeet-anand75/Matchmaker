//! Configuration loading and validation.
//!
//! All runtime configuration is sourced from environment variables.
//! A `.env` file is loaded automatically if present (development convenience).
//! Missing or invalid values cause an immediate startup failure with a
//! descriptive error message — no silent defaults for required parameters.
//!
//! # Usage
//!
//! ```no_run
//! use matchmaker::config::Config;
//!
//! let config = Config::from_env().expect("Invalid configuration");
//! ```

use std::fmt;
use thiserror::Error;

// ── Error type ────────────────────────────────────────────────────────────────

/// All configuration errors that can occur during startup.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing environment variable '{0}': {1}")]
    Missing(String, String),

    #[error("Invalid value for '{var}': expected {expected}, got '{actual}'")]
    Invalid {
        var: String,
        expected: String,
        actual: String,
    },

    #[error("Configuration constraint violated: {0}")]
    Constraint(String),
}

// ── Config struct ─────────────────────────────────────────────────────────────

/// Complete runtime configuration for the matchmaker service.
///
/// Constructed once at startup via [`Config::from_env`] and distributed
/// as `Arc<Config>` to all components. All fields are immutable after
/// construction — no runtime reconfiguration.
#[derive(Debug, Clone)]
pub struct Config {
    // ── Server ────────────────────────────────────────────────────────────────
    /// TCP port the HTTP server binds to. Range: 1024–65535.
    pub server_port: u16,

    // ── Worker pool ───────────────────────────────────────────────────────────
    /// Number of concurrent matchmaking worker Tokio tasks. Range: 1–64.
    pub worker_count: usize,

    /// Fallback polling interval per worker in milliseconds.
    /// Workers also wake on `Notify` signal — this is the maximum idle duration.
    pub worker_tick_ms: u64,

    // ── Reaper ────────────────────────────────────────────────────────────────
    /// Duration in milliseconds before a `Claimed` player is considered stale.
    /// The Reaper task resets stale claims to `Waiting`.
    pub stale_claim_timeout_ms: u64,

    // ── Relaxation time thresholds ────────────────────────────────────────────
    /// Wait duration (ms) at which Stage 1 activates (0 → stage_1).
    pub relaxation_stage_1_ms: u64,
    /// Wait duration (ms) at which Stage 2 activates.
    pub relaxation_stage_2_ms: u64,
    /// Wait duration (ms) at which Stage 3 activates.
    pub relaxation_stage_3_ms: u64,
    /// Wait duration (ms) at which Stage 4 activates.
    pub relaxation_stage_4_ms: u64,

    // ── Relaxation skill deviation windows ────────────────────────────────────
    /// Acceptable MMR half-width at Stage 1 (±delta). Highest quality.
    pub relaxation_stage_1_delta: u32,
    /// Acceptable MMR half-width at Stage 2.
    pub relaxation_stage_2_delta: u32,
    /// Acceptable MMR half-width at Stage 3.
    pub relaxation_stage_3_delta: u32,
    /// Acceptable MMR half-width at Stage 4.
    pub relaxation_stage_4_delta: u32,
    /// Acceptable MMR half-width at Stage 5. Starvation prevention floor.
    /// Should be large enough to cover the full skill range (e.g. 9999).
    pub relaxation_stage_5_delta: u32,
}

// ── Display ───────────────────────────────────────────────────────────────────

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Config {{ port={}, workers={}, tick={}ms, stale_timeout={}ms, \
             relaxation=[{}ms/±{}, {}ms/±{}, {}ms/±{}, {}ms/±{}, ∞/±{}] }}",
            self.server_port,
            self.worker_count,
            self.worker_tick_ms,
            self.stale_claim_timeout_ms,
            self.relaxation_stage_1_ms,
            self.relaxation_stage_1_delta,
            self.relaxation_stage_2_ms,
            self.relaxation_stage_2_delta,
            self.relaxation_stage_3_ms,
            self.relaxation_stage_3_delta,
            self.relaxation_stage_4_ms,
            self.relaxation_stage_4_delta,
            self.relaxation_stage_5_delta,
        )
    }
}

// ── Construction ──────────────────────────────────────────────────────────────

impl Config {
    /// Load and validate configuration from environment variables.
    ///
    /// Attempts to load a `.env` file first (silently ignored if absent).
    /// Fails fast with a descriptive [`ConfigError`] if any value is
    /// missing, unparseable, or violates a cross-field constraint.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if:
    /// - A required variable is missing
    /// - A value cannot be parsed as its expected type
    /// - A value is outside its valid range
    /// - Cross-field constraints are violated (e.g. non-monotonic thresholds)
    pub fn from_env() -> Result<Self, ConfigError> {
        // Load .env file if present — silently ignore if absent.
        // This is intentional: production environments set vars directly;
        // .env is a development convenience only.
        let _ = dotenvy::dotenv();

        let config = Self {
            server_port: parse_env_range("SERVER_PORT", 1024u16, 65535u16, 8080u16)?,
            worker_count: parse_env_range("WORKER_COUNT", 1usize, 64usize, 4usize)?,
            worker_tick_ms: parse_env_range("WORKER_TICK_MS", 10u64, 10_000u64, 50u64)?,
            stale_claim_timeout_ms: parse_env_range(
                "STALE_CLAIM_TIMEOUT_MS",
                100u64,
                30_000u64,
                500u64,
            )?,
            relaxation_stage_1_ms: parse_env_range(
                "RELAXATION_STAGE_1_MS",
                1u64,
                300_000u64,
                5_000u64,
            )?,
            relaxation_stage_2_ms: parse_env_range(
                "RELAXATION_STAGE_2_MS",
                1u64,
                300_000u64,
                15_000u64,
            )?,
            relaxation_stage_3_ms: parse_env_range(
                "RELAXATION_STAGE_3_MS",
                1u64,
                300_000u64,
                30_000u64,
            )?,
            relaxation_stage_4_ms: parse_env_range(
                "RELAXATION_STAGE_4_MS",
                1u64,
                300_000u64,
                60_000u64,
            )?,
            relaxation_stage_1_delta: parse_env_range(
                "RELAXATION_STAGE_1_DELTA",
                0u32,
                10_000u32,
                50u32,
            )?,
            relaxation_stage_2_delta: parse_env_range(
                "RELAXATION_STAGE_2_DELTA",
                0u32,
                10_000u32,
                100u32,
            )?,
            relaxation_stage_3_delta: parse_env_range(
                "RELAXATION_STAGE_3_DELTA",
                0u32,
                10_000u32,
                200u32,
            )?,
            relaxation_stage_4_delta: parse_env_range(
                "RELAXATION_STAGE_4_DELTA",
                0u32,
                10_000u32,
                400u32,
            )?,
            relaxation_stage_5_delta: parse_env_range(
                "RELAXATION_STAGE_5_DELTA",
                0u32,
                10_000u32,
                9999u32,
            )?,
        };

        config.validate()?;
        Ok(config)
    }

    /// Cross-field validation — called after all individual fields are parsed.
    fn validate(&self) -> Result<(), ConfigError> {
        // Relaxation time thresholds must be strictly increasing.
        if self.relaxation_stage_1_ms >= self.relaxation_stage_2_ms {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_1_MS ({}) must be less than RELAXATION_STAGE_2_MS ({})",
                self.relaxation_stage_1_ms, self.relaxation_stage_2_ms
            )));
        }
        if self.relaxation_stage_2_ms >= self.relaxation_stage_3_ms {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_2_MS ({}) must be less than RELAXATION_STAGE_3_MS ({})",
                self.relaxation_stage_2_ms, self.relaxation_stage_3_ms
            )));
        }
        if self.relaxation_stage_3_ms >= self.relaxation_stage_4_ms {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_3_MS ({}) must be less than RELAXATION_STAGE_4_MS ({})",
                self.relaxation_stage_3_ms, self.relaxation_stage_4_ms
            )));
        }

        // Relaxation deltas must be non-decreasing.
        if self.relaxation_stage_1_delta > self.relaxation_stage_2_delta {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_1_DELTA ({}) must be <= RELAXATION_STAGE_2_DELTA ({})",
                self.relaxation_stage_1_delta, self.relaxation_stage_2_delta
            )));
        }
        if self.relaxation_stage_2_delta > self.relaxation_stage_3_delta {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_2_DELTA ({}) must be <= RELAXATION_STAGE_3_DELTA ({})",
                self.relaxation_stage_2_delta, self.relaxation_stage_3_delta
            )));
        }
        if self.relaxation_stage_3_delta > self.relaxation_stage_4_delta {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_3_DELTA ({}) must be <= RELAXATION_STAGE_4_DELTA ({})",
                self.relaxation_stage_3_delta, self.relaxation_stage_4_delta
            )));
        }
        if self.relaxation_stage_4_delta > self.relaxation_stage_5_delta {
            return Err(ConfigError::Constraint(format!(
                "RELAXATION_STAGE_4_DELTA ({}) must be <= RELAXATION_STAGE_5_DELTA ({})",
                self.relaxation_stage_4_delta, self.relaxation_stage_5_delta
            )));
        }

        // Stale claim timeout must exceed worker tick to avoid the Reaper
        // resetting claims that are still actively being processed.
        if self.stale_claim_timeout_ms <= self.worker_tick_ms {
            return Err(ConfigError::Constraint(format!(
                "STALE_CLAIM_TIMEOUT_MS ({}) must be greater than WORKER_TICK_MS ({}). \
                 Otherwise the Reaper may reset claims that are still in progress.",
                self.stale_claim_timeout_ms, self.worker_tick_ms
            )));
        }

        Ok(())
    }
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

/// Parse an environment variable as type `T`, falling back to `default` if
/// the variable is unset. Fails with [`ConfigError`] if the variable is set
/// but unparseable or outside `[min, max]`.
fn parse_env_range<T>(name: &str, min: T, max: T, default: T) -> Result<T, ConfigError>
where
    T: std::str::FromStr + std::fmt::Display + PartialOrd + Copy,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Err(_) => {
            // Variable not set — use default. This is intentional: all
            // variables have sensible defaults so the service runs out of
            // the box without a .env file.
            Ok(default)
        }
        Ok(raw) => {
            let value: T = raw.trim().parse().map_err(|e| ConfigError::Invalid {
                var: name.to_string(),
                expected: format!("a valid {}", std::any::type_name::<T>()),
                actual: format!("{raw} ({e})"),
            })?;

            if value < min || value > max {
                return Err(ConfigError::Invalid {
                    var: name.to_string(),
                    expected: format!("a value between {min} and {max}"),
                    actual: value.to_string(),
                });
            }

            Ok(value)
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_env() {
        let vars = [
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
        ];
        for var in &vars {
            std::env::remove_var(var);
        }
    }

    #[test]
    #[serial]
    fn test_defaults_are_valid() {
        clear_env();
        let config = Config::from_env().expect("Default config must be valid");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.worker_tick_ms, 50);
        assert_eq!(config.stale_claim_timeout_ms, 500);
        assert_eq!(config.relaxation_stage_1_delta, 50);
        assert_eq!(config.relaxation_stage_5_delta, 9999);
    }

    #[test]
    #[serial]
    fn test_custom_values_are_loaded() {
        clear_env();
        std::env::set_var("SERVER_PORT", "9090");
        std::env::set_var("WORKER_COUNT", "8");
        let config = Config::from_env().expect("Custom config must be valid");
        assert_eq!(config.server_port, 9090);
        assert_eq!(config.worker_count, 8);
        clear_env();
    }

    #[test]
    #[serial]
    fn test_invalid_worker_count_fails() {
        clear_env();
        std::env::set_var("WORKER_COUNT", "0");
        assert!(Config::from_env().is_err());
        clear_env();
    }

    #[test]
    #[serial]
    fn test_non_monotonic_thresholds_fail() {
        clear_env();
        std::env::set_var("RELAXATION_STAGE_1_MS", "20000");
        std::env::set_var("RELAXATION_STAGE_2_MS", "10000");
        std::env::set_var("RELAXATION_STAGE_3_MS", "30000");
        std::env::set_var("RELAXATION_STAGE_4_MS", "60000");
        let result = Config::from_env();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("STAGE_1_MS"));
        clear_env();
    }

    #[test]
    #[serial]
    fn test_stale_timeout_must_exceed_tick() {
        clear_env();
        std::env::set_var("WORKER_TICK_MS", "200");
        std::env::set_var("STALE_CLAIM_TIMEOUT_MS", "100");
        let result = Config::from_env();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("STALE_CLAIM_TIMEOUT_MS"));
        clear_env();
    }

    #[test]
    #[serial]
    fn test_non_numeric_value_fails() {
        clear_env();
        std::env::set_var("SERVER_PORT", "not_a_number");
        assert!(Config::from_env().is_err());
        clear_env();
    }

    #[test]
    #[serial]
    fn test_display_does_not_panic() {
        clear_env();
        let config = Config::from_env().unwrap();
        let s = format!("{config}");
        assert!(s.contains("Config"));
    }
}
