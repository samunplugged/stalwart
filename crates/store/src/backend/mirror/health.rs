/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Circuit breaker for one side of a mirrored blob store.

use std::{
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub struct SideHealth {
    threshold: u32,
    down_for_ms: u64,
    base: Instant,
    /// Consecutive failures since the last success.
    failures: AtomicU32,
    /// Milliseconds after `base` until which the side is down.
    down_until_ms: AtomicU64,
    /// Set when the side was marked down and has not succeeded since.
    tripped: AtomicBool,
}

impl SideHealth {
    pub fn new(threshold: u32, down_for: Duration) -> Self {
        SideHealth {
            threshold: threshold.max(1),
            down_for_ms: u64::try_from(down_for.as_millis()).unwrap_or(u64::MAX),
            base: Instant::now(),
            failures: AtomicU32::new(0),
            down_until_ms: AtomicU64::new(0),
            tripped: AtomicBool::new(false),
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Whether the side is marked down right now.
    pub fn is_down(&self) -> bool {
        self.now_ms() < self.down_until_ms.load(Ordering::Acquire)
    }

    /// Records a success. Returns `true` when the side was marked down
    /// before and is now considered up again.
    pub fn record_success(&self) -> bool {
        self.failures.store(0, Ordering::Release);
        if self.tripped.swap(false, Ordering::AcqRel) {
            self.down_until_ms.store(0, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Records a failure. Returns `true` when this failure marks the side
    /// down. A side that was marked down and fails again on its first try
    /// after the pause is marked down again at once.
    pub fn record_failure(&self) -> bool {
        if self.is_down() {
            // Calls that started before the side was marked down
            return false;
        }
        let trip = if self.tripped.load(Ordering::Acquire) {
            true
        } else {
            self.failures.fetch_add(1, Ordering::AcqRel) + 1 >= self.threshold
        };
        if trip {
            self.failures.store(0, Ordering::Release);
            self.tripped.store(true, Ordering::Release);
            self.down_until_ms.store(
                self.now_ms().saturating_add(self.down_for_ms),
                Ordering::Release,
            );
        }
        trip
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_after_threshold_and_recovers() {
        let health = SideHealth::new(3, Duration::from_millis(50));
        assert!(!health.record_failure());
        assert!(!health.record_failure());
        assert!(!health.is_down());
        assert!(health.record_failure());
        assert!(health.is_down());

        // Failures while down do not extend the pause
        assert!(!health.record_failure());

        std::thread::sleep(Duration::from_millis(60));
        assert!(!health.is_down());

        // The first failure after the pause marks it down again
        assert!(health.record_failure());
        assert!(health.is_down());

        std::thread::sleep(Duration::from_millis(60));
        assert!(health.record_success());
        assert!(!health.is_down());
        assert!(!health.record_success());

        // Back to counting from zero
        assert!(!health.record_failure());
        assert!(!health.record_failure());
        assert!(health.record_failure());
    }

    #[test]
    fn success_resets_the_count() {
        let health = SideHealth::new(2, Duration::from_secs(30));
        assert!(!health.record_failure());
        assert!(!health.record_success());
        assert!(!health.record_failure());
        assert!(!health.is_down());
        assert!(health.record_failure());
        assert!(health.is_down());
    }
}
