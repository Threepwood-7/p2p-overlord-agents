use chrono::{DateTime, Utc};
use overlord_agent_nat::NatStatusSnapshot;
pub use overlord_agent_nat::{AgentNetworkReport, AgentNetworkingConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Kad2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HashType {
    Ed2k(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    Video,
    Audio,
    Document,
    Archive,
    Software,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TagEntry {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub protocol: Protocol,
    pub address: String,
    #[serde(default)]
    pub extra: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRecord {
    #[serde(default)]
    pub hashes: Vec<HashType>,
    #[serde(default)]
    pub names: Vec<String>,
    pub size: Option<u64>,
    pub content_type: Option<ContentType>,
    #[serde(default)]
    pub tags: Vec<TagEntry>,
    #[serde(default)]
    pub sources: Vec<Source>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchKind {
    Keyword,
    Source,
    Notes,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchJob {
    pub job_id: Uuid,
    pub kind: SearchKind,
    pub query: Option<String>,
    pub file_hash: Option<HashType>,
    pub file_size: Option<u64>,
    pub callback_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchEventStatus {
    Started,
    BatchReceived,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchEvent {
    pub job_id: Uuid,
    pub indexer_id: Uuid,
    pub status: SearchEventStatus,
    pub result_count: Option<u32>,
    pub batch_count: Option<u32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchCancelRequest {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultBatch {
    pub job_id: Option<Uuid>,
    pub indexer_id: Uuid,
    pub protocol: Protocol,
    #[serde(default)]
    pub files: Vec<FileRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerStats {
    pub indexer_id: Uuid,
    pub protocol: Protocol,
    pub peers_connected: u32,
    pub crawl_rate: f32,
    pub snoop_queue_depth: u32,
    pub staging_queue_depth: u32,
    pub uptime_secs: u64,
    pub nat: Option<NatStatusSnapshot>,
    pub interface_report: Option<AgentNetworkReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigUpdate {
    pub protocol: Protocol,
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum SnoopEntry {
    Keyword {
        logical_key: String,
        target: String,
        start_position: u16,
        restrictive_payload_hex: Option<String>,
        hit_count: u32,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        last_drained_at: Option<DateTime<Utc>>,
    },
    Source {
        logical_key: String,
        target: String,
        start_position: u16,
        size: u64,
        hit_count: u32,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        last_drained_at: Option<DateTime<Utc>>,
    },
    Notes {
        logical_key: String,
        target: String,
        size: u64,
        hit_count: u32,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        last_drained_at: Option<DateTime<Utc>>,
    },
}

impl SnoopEntry {
    #[must_use]
    pub fn logical_key(&self) -> &str {
        match self {
            SnoopEntry::Keyword { logical_key, .. }
            | SnoopEntry::Source { logical_key, .. }
            | SnoopEntry::Notes { logical_key, .. } => logical_key,
        }
    }

    #[must_use]
    pub fn target(&self) -> &str {
        match self {
            SnoopEntry::Keyword { target, .. }
            | SnoopEntry::Source { target, .. }
            | SnoopEntry::Notes { target, .. } => target,
        }
    }

    #[must_use]
    pub fn hit_count(&self) -> u32 {
        match self {
            SnoopEntry::Keyword { hit_count, .. }
            | SnoopEntry::Source { hit_count, .. }
            | SnoopEntry::Notes { hit_count, .. } => *hit_count,
        }
    }

    pub fn set_hit_count(&mut self, value: u32) {
        match self {
            SnoopEntry::Keyword { hit_count, .. }
            | SnoopEntry::Source { hit_count, .. }
            | SnoopEntry::Notes { hit_count, .. } => *hit_count = value,
        }
    }

    #[must_use]
    pub fn first_seen(&self) -> DateTime<Utc> {
        match self {
            SnoopEntry::Keyword { first_seen, .. }
            | SnoopEntry::Source { first_seen, .. }
            | SnoopEntry::Notes { first_seen, .. } => first_seen.clone(),
        }
    }

    pub fn set_first_seen(&mut self, value: DateTime<Utc>) {
        match self {
            SnoopEntry::Keyword { first_seen, .. }
            | SnoopEntry::Source { first_seen, .. }
            | SnoopEntry::Notes { first_seen, .. } => *first_seen = value,
        }
    }

    #[must_use]
    pub fn last_seen(&self) -> DateTime<Utc> {
        match self {
            SnoopEntry::Keyword { last_seen, .. }
            | SnoopEntry::Source { last_seen, .. }
            | SnoopEntry::Notes { last_seen, .. } => last_seen.clone(),
        }
    }

    pub fn set_last_seen(&mut self, value: DateTime<Utc>) {
        match self {
            SnoopEntry::Keyword { last_seen, .. }
            | SnoopEntry::Source { last_seen, .. }
            | SnoopEntry::Notes { last_seen, .. } => *last_seen = value,
        }
    }

    #[must_use]
    pub fn last_drained_at(&self) -> Option<DateTime<Utc>> {
        match self {
            SnoopEntry::Keyword {
                last_drained_at, ..
            }
            | SnoopEntry::Source {
                last_drained_at, ..
            }
            | SnoopEntry::Notes {
                last_drained_at, ..
            } => last_drained_at.clone(),
        }
    }

    pub fn set_last_drained_at(&mut self, value: Option<DateTime<Utc>>) {
        match self {
            SnoopEntry::Keyword {
                last_drained_at, ..
            }
            | SnoopEntry::Source {
                last_drained_at, ..
            }
            | SnoopEntry::Notes {
                last_drained_at, ..
            } => *last_drained_at = value,
        }
    }

    #[must_use]
    pub fn restrictive_payload_hex(&self) -> Option<&str> {
        match self {
            SnoopEntry::Keyword {
                restrictive_payload_hex,
                ..
            } => restrictive_payload_hex.as_deref(),
            SnoopEntry::Source { .. } | SnoopEntry::Notes { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PopularHash {
    pub hash: HashType,
    pub canonical_name: String,
    pub size: u64,
    pub source_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub indexer_id: Uuid,
    pub protocol: Protocol,
    pub url: String,
    pub hostname: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerRegistration {
    pub indexer_id: Uuid,
    pub protocol: Protocol,
    pub url: String,
    pub hostname: String,
    pub version: String,
    pub registered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub registered: IndexerRegistration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInterfacesView {
    pub registration: IndexerRegistration,
    pub report: Option<AgentNetworkReport>,
    pub config: AgentNetworkingConfig,
    pub nat: Option<NatStatusSnapshot>,
    pub last_error: Option<String>,
}
