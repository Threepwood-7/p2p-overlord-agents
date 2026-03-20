use chrono::{DateTime, Utc};
pub use overlord_agent_nat::{AgentNetworkReport, AgentNetworkingConfig};
use overlord_agent_nat::NatStatusSnapshot;
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
pub struct SearchJob {
    pub job_id: Uuid,
    pub query: String,
    pub callback_url: String,
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
pub struct SnoopEntry {
    pub query: String,
    pub hash: Option<HashType>,
    pub hit_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
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
