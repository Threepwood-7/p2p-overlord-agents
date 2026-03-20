use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Tracks incoming packet counts per IP to detect flooding.
pub struct PacketTracker {
    /// (packet_count, window_start)
    counts: HashMap<IpAddr, (u32, Instant)>,
    max_per_window: u32,
    window: Duration,
}

impl PacketTracker {
    pub fn new(max_per_window: u32, window: Duration) -> Self {
        Self {
            counts: HashMap::new(),
            max_per_window,
            window,
        }
    }

    /// Record an incoming packet from this IP.
    /// Returns `true` if the packet should be allowed, `false` if it's over limit.
    pub fn record_and_check(&mut self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let entry = self.counts.entry(ip).or_insert((0, now));

        // Reset window if expired
        if now.duration_since(entry.1) >= self.window {
            *entry = (0, now);
        }

        entry.0 += 1;
        entry.0 <= self.max_per_window
    }

    /// Prune stale entries (call periodically to prevent memory growth).
    pub fn prune(&mut self) {
        let now = Instant::now();
        self.counts
            .retain(|_, (_, window_start)| now.duration_since(*window_start) < self.window * 2);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn test_normal_traffic_passes() {
        let mut tracker = PacketTracker::new(10, Duration::from_secs(1));
        let addr = ip("1.2.3.4");
        for _ in 0..10 {
            assert!(tracker.record_and_check(addr));
        }
    }

    #[test]
    fn test_flood_over_limit_blocked() {
        let mut tracker = PacketTracker::new(5, Duration::from_secs(1));
        let addr = ip("1.2.3.4");
        // First 5 pass
        for _ in 0..5 {
            assert!(tracker.record_and_check(addr));
        }
        // 6th and beyond are blocked
        assert!(!tracker.record_and_check(addr));
        assert!(!tracker.record_and_check(addr));
    }

    #[test]
    fn test_window_reset() {
        // Use a very short window to test expiry
        let mut tracker = PacketTracker::new(2, Duration::from_millis(50));
        let addr = ip("5.6.7.8");
        assert!(tracker.record_and_check(addr));
        assert!(tracker.record_and_check(addr));
        assert!(!tracker.record_and_check(addr)); // over limit

        // Wait for window to expire
        std::thread::sleep(Duration::from_millis(60));

        // Window should be reset — packets allowed again
        assert!(tracker.record_and_check(addr));
    }
}
