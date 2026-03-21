use std::collections::{HashMap, VecDeque};
use std::str::FromStr;

use chrono::{DateTime, TimeDelta, Utc};
use overlord_agent_common::{HashType, SnoopEntry};
use overlord_kad_proto::NodeId;

use crate::config::SnoopQueueConfig;

/// In-memory scheduler state for KAD snooped requests.
#[derive(Debug, Clone)]
pub struct SnoopQueue {
    config: SnoopQueueConfig,
    entries: HashMap<String, SnoopEntry>,
    recent_drains: VecDeque<DateTime<Utc>>,
}

impl SnoopQueue {
    /// Creates an empty snoop queue with the provided scheduling settings.
    pub fn new(config: SnoopQueueConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            recent_drains: VecDeque::new(),
        }
    }

    /// Restores persisted entries into the in-memory queue.
    pub fn merge_snapshot(&mut self, entries: Vec<SnoopEntry>) {
        for entry in entries {
            self.merge_entry(entry);
        }
    }

    /// Returns a snapshot suitable for flush/persistence calls.
    pub fn snapshot(&self) -> Vec<SnoopEntry> {
        let mut entries = self.entries.values().cloned().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.query.cmp(&right.query));
        entries
    }

    /// Returns the number of unique snooped query keys currently tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Records a snooped request occurrence.
    pub fn record(&mut self, query: String, hash: Option<HashType>, now: DateTime<Utc>) {
        self.merge_entry(SnoopEntry {
            query,
            hash,
            hit_count: 1,
            first_seen: now,
            last_seen: now,
            last_drained_at: None,
        });
    }

    /// Selects the next keyword target eligible for passive drain and marks it as drained.
    pub fn select_next_keyword_target(&mut self, now: DateTime<Utc>) -> Option<NodeId> {
        self.prune_recent_drains(now);
        if self.recent_drains.len() >= self.config.max_queries_per_600s as usize {
            return None;
        }

        let dedup_cutoff = now - seconds(self.config.dedup_window_secs);
        let cooldown_cutoff = now - seconds(self.config.drain_cooldown_secs);
        let mut recent = Vec::new();
        let mut stale = Vec::new();

        for entry in self.entries.values() {
            let Some(raw_target) = entry.query.strip_prefix("keyword:") else {
                continue;
            };
            if entry
                .last_drained_at
                .as_ref()
                .is_some_and(|last_drained_at| last_drained_at > &cooldown_cutoff)
            {
                continue;
            }
            if let Ok(target) = NodeId::from_str(raw_target) {
                if entry.last_seen >= dedup_cutoff {
                    recent.push((
                        target,
                        entry.hit_count,
                        entry.last_seen,
                        entry.query.clone(),
                    ));
                } else {
                    stale.push((
                        target,
                        entry.hit_count,
                        entry.last_seen,
                        entry.query.clone(),
                    ));
                }
            }
        }

        recent.sort_by(candidate_cmp);
        stale.sort_by(candidate_cmp);
        let selected = recent
            .into_iter()
            .next()
            .or_else(|| stale.into_iter().next())?;
        if let Some(entry) = self.entries.get_mut(&selected.3) {
            entry.last_drained_at = Some(now);
        }
        self.recent_drains.push_back(now);
        Some(selected.0)
    }

    fn merge_entry(&mut self, entry: SnoopEntry) {
        let SnoopEntry {
            query,
            hash,
            hit_count,
            first_seen,
            last_seen,
            last_drained_at,
        } = entry;
        if let Some(existing) = self.entries.get_mut(&query) {
            existing.hit_count = existing.hit_count.saturating_add(hit_count);
            existing.last_seen = existing.last_seen.max(last_seen);
            existing.first_seen = existing.first_seen.min(first_seen);
            if existing.hash.is_none() {
                existing.hash = hash;
            }
            let current_last_drained_at = existing.last_drained_at.take();
            existing.last_drained_at = match (current_last_drained_at, last_drained_at) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(left), None) => Some(left),
                (None, right) => right,
            };
            return;
        }
        self.entries.insert(
            query.clone(),
            SnoopEntry {
                query,
                hash,
                hit_count,
                first_seen,
                last_seen,
                last_drained_at,
            },
        );
    }

    fn prune_recent_drains(&mut self, now: DateTime<Utc>) {
        let cutoff = now - TimeDelta::minutes(10);
        while self
            .recent_drains
            .front()
            .is_some_and(|drained_at| drained_at < &cutoff)
        {
            self.recent_drains.pop_front();
        }
    }
}

fn seconds(value: u64) -> TimeDelta {
    TimeDelta::seconds(i64::try_from(value).unwrap_or(i64::MAX))
}

fn candidate_cmp(
    left: &(NodeId, u32, DateTime<Utc>, String),
    right: &(NodeId, u32, DateTime<Utc>, String),
) -> std::cmp::Ordering {
    right
        .1
        .cmp(&left.1)
        .then_with(|| right.2.cmp(&left.2))
        .then_with(|| left.3.cmp(&right.3))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use overlord_agent_common::HashType;

    use super::SnoopQueue;
    use crate::config::SnoopQueueConfig;

    fn queue() -> SnoopQueue {
        SnoopQueue::new(SnoopQueueConfig {
            dedup_window_secs: 60,
            max_queries_per_600s: 2,
            drain_cooldown_secs: 30,
        })
    }

    fn keyword(id: &str) -> String {
        format!("keyword:{id}")
    }

    fn ts(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    #[test]
    fn repeated_hits_merge_and_preserve_first_seen() {
        let mut queue = queue();
        let query = keyword("00112233445566778899aabbccddeeff");
        queue.record(query.clone(), None, ts(100));
        queue.record(query.clone(), None, ts(140));

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].hit_count, 2);
        assert_eq!(snapshot[0].first_seen, ts(100));
        assert_eq!(snapshot[0].last_seen, ts(140));
    }

    #[test]
    fn source_and_notes_entries_are_not_selected_for_keyword_drain() {
        let mut queue = queue();
        queue.record(
            "source:00112233445566778899aabbccddeeff".to_string(),
            Some(HashType::Ed2k(
                "00112233445566778899aabbccddeeff".to_string(),
            )),
            ts(100),
        );
        queue.record(
            "notes:00112233445566778899aabbccddeeff".to_string(),
            Some(HashType::Ed2k(
                "00112233445566778899aabbccddeeff".to_string(),
            )),
            ts(110),
        );
        queue.record(keyword("00112233445566778899aabbccddeeff"), None, ts(120));

        let selected = queue.select_next_keyword_target(ts(130));
        assert!(selected.is_some());
    }

    #[test]
    fn cooldown_blocks_immediate_reselection_and_later_allows_retry() {
        let mut queue = queue();
        queue.record(keyword("00112233445566778899aabbccddeeff"), None, ts(100));

        assert!(queue.select_next_keyword_target(ts(110)).is_some());
        assert!(queue.select_next_keyword_target(ts(120)).is_none());
        assert!(queue.select_next_keyword_target(ts(141)).is_some());
    }

    #[test]
    fn rate_limit_caps_drains_within_ten_minutes() {
        let mut queue = queue();
        queue.record(keyword("00112233445566778899aabbccddeeff"), None, ts(100));
        queue.record(keyword("11112222333344445555666677778888"), None, ts(101));
        queue.record(keyword("9999aaaabbbbccccddddeeeeffff0000"), None, ts(102));

        assert!(queue.select_next_keyword_target(ts(110)).is_some());
        assert!(queue.select_next_keyword_target(ts(150)).is_some());
        assert!(queue.select_next_keyword_target(ts(200)).is_none());
        assert!(queue.select_next_keyword_target(ts(711)).is_some());
    }

    #[test]
    fn snapshot_merge_round_trips_last_drained_at() {
        let mut queue = queue();
        let entry = overlord_agent_common::SnoopEntry {
            query: keyword("00112233445566778899aabbccddeeff"),
            hash: None,
            hit_count: 5,
            first_seen: ts(100),
            last_seen: ts(120),
            last_drained_at: Some(ts(130)),
        };

        queue.merge_snapshot(vec![entry.clone()]);
        let snapshot = queue.snapshot();

        assert_eq!(snapshot, vec![entry]);
    }
}
