//! Shared utility functions used across modules.

use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current Unix timestamp in milliseconds.
///
/// Identical to [`crate::engine::matcher::unix_ms`] but re-exported here
/// so modules outside `engine` can use it without a deep import path.
///
/// Returns `0` if the system clock is before the Unix epoch
/// (impossible on any real system).
#[inline]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Returns the current Unix timestamp in seconds.
#[inline]
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Clamp a `u32` skill rating to the valid range `[0, max]`.
///
/// Used by the API validation layer as a belt-and-suspenders guard.
#[inline]
pub fn clamp_rating(rating: u32, max: u32) -> u32 {
    rating.min(max)
}

/// Compute the average of a slice of `u64` values.
///
/// Returns `0` if the slice is empty.
#[inline]
pub fn average_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.iter().sum::<u64>() / values.len() as u64
}

/// Compute the population count (number of set bits) in a `u16`.
///
/// Used by the exhaustive balance algorithm. Wraps `u16::count_ones`
/// for a more descriptive call site name.
#[inline]
pub fn popcount_u16(mask: u16) -> u32 {
    mask.count_ones()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_unix_ms_is_plausible() {
        let ts = now_unix_ms();
        // Must be after 2024-01-01T00:00:00Z = 1_704_067_200_000 ms
        assert!(ts > 1_704_067_200_000, "timestamp must be after 2024-01-01");
    }

    #[test]
    fn test_now_unix_secs_is_plausible() {
        let ts = now_unix_secs();
        assert!(ts > 1_704_067_200, "timestamp must be after 2024-01-01");
    }

    #[test]
    fn test_clamp_rating_within_range() {
        assert_eq!(clamp_rating(1000, 3000), 1000);
    }

    #[test]
    fn test_clamp_rating_at_max() {
        assert_eq!(clamp_rating(3000, 3000), 3000);
    }

    #[test]
    fn test_clamp_rating_above_max() {
        assert_eq!(clamp_rating(5000, 3000), 3000);
    }

    #[test]
    fn test_average_u64_normal() {
        assert_eq!(average_u64(&[100, 200, 300]), 200);
    }

    #[test]
    fn test_average_u64_empty() {
        assert_eq!(average_u64(&[]), 0);
    }

    #[test]
    fn test_average_u64_single() {
        assert_eq!(average_u64(&[42]), 42);
    }

    #[test]
    fn test_popcount_u16_zero() {
        assert_eq!(popcount_u16(0b0000_0000_0000_0000), 0);
    }

    #[test]
    fn test_popcount_u16_all_ones() {
        assert_eq!(popcount_u16(0b0000_0011_1111_1111), 10);
    }

    #[test]
    fn test_popcount_u16_five_bits() {
        // 0b0000_0001_0001_0111 = bits 0,1,2,4,8 set = 5 bits
        assert_eq!(popcount_u16(0b0000_0001_0001_0111), 5);
    }
}