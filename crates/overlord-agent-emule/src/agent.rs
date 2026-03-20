use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use md4::{Digest, Md4};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use overlord_agent_common::{
    ConfigUpdate, ContentType, CoordinatorClient, FileRecord, HashType, IndexerServer,
    IndexerService, IndexerStats, PopularHash, Protocol, RegisterRequest, ResultBatch, SearchJob,
    SnoopEntry, Source, TagEntry,
};
use overlord_kad_dht::{
    DhtConfig, DhtNode, SearchResult,
    bootstrap::{BootstrapContact, encode_nodes_dat},
};
use overlord_kad_proto::{
    Ed2kHash, KadPacket, NodeId, Tag, TagName, TagValue, constants::K, packet::ContactEntry,
    tag_name,
};
use overlord_kad_routing::Contact;

use crate::config::{EmuleAgentConfig, KadConfig};

const ACTIVE_BATCH_SIZE: usize = 25;
const PASSIVE_BATCH_SIZE: usize = 50;
const BOOTSTRAP_RETRY_SECS: u64 = 30;
const SNOOP_FLUSH_SECS: u64 = 30;
const PASSIVE_CRAWL_SECS: u64 = 45;

#[derive(Clone)]
struct AgentStatePaths {
    node_id_path: PathBuf,
    udp_key_path: PathBuf,
    nodes_dat_path: PathBuf,
}

pub struct OverlordAgentEmule {
    config: Arc<RwLock<EmuleAgentConfig>>,
    coordinator: CoordinatorClient,
    indexer_id: Uuid,
    dht: DhtNode,
    started_at: Instant,
    state_paths: AgentStatePaths,
    snoop_queue: Arc<Mutex<HashMap<String, SnoopEntry>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    started: AtomicBool,
    shutdown: Arc<AtomicBool>,
    passive_result_count: Arc<AtomicU64>,
}

impl OverlordAgentEmule {
    pub async fn new(config: EmuleAgentConfig) -> Result<Self> {
        let indexer_id = load_or_create_indexer_id(&config.agent.indexer_id_path)?;
        let coordinator = CoordinatorClient::new(&config.coordinator.url)?;
        let state_paths = AgentStatePaths::from_config(&config);
        ensure_parent_dir(&state_paths.node_id_path)?;
        ensure_parent_dir(&state_paths.udp_key_path)?;
        ensure_parent_dir(&state_paths.nodes_dat_path)?;

        let node_id = load_or_create_node_id(&state_paths.node_id_path)?;
        let udp_key = load_or_create_udp_key(&state_paths.udp_key_path)?;
        let bind_addr: SocketAddr = config
            .kad
            .udp_bind_addr
            .parse()
            .context("invalid kad.udp_bind_addr")?;
        let nodes_dat = read_optional_bytes(&state_paths.nodes_dat_path)?;
        let nodes_text =
            (!config.kad.bootstrap_nodes.is_empty()).then(|| config.kad.bootstrap_nodes.join("\n"));

        let dht = DhtNode::new(DhtConfig {
            bind_addr,
            node_id,
            max_routing_table_size: 12_000,
            max_concurrent_searches: 5,
            search_timeout: Duration::from_secs(config.kad.search_timeout_secs),
            store_timeout: Duration::from_secs(config.kad.store_timeout_secs),
            republish_interval: Duration::from_secs(config.kad.republish_interval_secs),
            max_outbound_pps: config.kad.max_outbound_pps,
            search_phase2_fanout: config.kad.search_phase2_fanout,
            keyword_result_cap: config.kad.keyword_result_cap,
            source_result_cap: config.kad.source_result_cap,
            notes_result_cap: config.kad.notes_result_cap,
            obfuscation_enabled: config.kad.obfuscation_enabled,
            udp_key,
            nodes_dat,
            nodes_text,
        })
        .await?;

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            coordinator,
            indexer_id,
            dht,
            started_at: Instant::now(),
            state_paths,
            snoop_queue: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
            started: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            passive_result_count: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn register_with_coordinator(&self) -> Result<()> {
        let config = self.config.read().await.clone();
        let registration = self
            .coordinator
            .register(&RegisterRequest {
                indexer_id: self.indexer_id,
                protocol: Protocol::Kad2,
                url: format!("http://{}", config.agent.bind_addr),
                hostname: config.agent.hostname,
                version: config.agent.version,
            })
            .await?;
        info!(
            "registered agent {} at {}",
            registration.indexer_id, registration.url
        );
        Ok(())
    }

    pub async fn serve(self: Arc<Self>) -> Result<()> {
        let bind_addr: SocketAddr = self
            .config
            .read()
            .await
            .agent
            .bind_addr
            .parse()
            .context("invalid agent.bind_addr")?;
        IndexerServer::new(self).serve(bind_addr).await
    }
}

async fn do_active_keyword_search(
    dht: &DhtNode,
    indexer_id: Uuid,
    job: &SearchJob,
    config: Arc<RwLock<EmuleAgentConfig>>,
) -> Result<()> {
    let target = keyword_target(&job.query);
    let mut stream = dht.search_keywords(target);
    let callback_client = CoordinatorClient::new(&job.callback_url)?;
    let mut files = Vec::new();
    let mut seen = 0usize;

    while let Some(result) = stream.next().await {
        seen += 1;
        files.push(map_search_result_for(dht, &result)?);
        if files.len() >= ACTIVE_BATCH_SIZE {
            callback_client
                .post_results(&ResultBatch {
                    job_id: Some(job.job_id),
                    indexer_id,
                    protocol: Protocol::Kad2,
                    files: std::mem::take(&mut files),
                })
                .await?;
        }
    }

    if !files.is_empty() {
        callback_client
            .post_results(&ResultBatch {
                job_id: Some(job.job_id),
                indexer_id,
                protocol: Protocol::Kad2,
                files,
            })
            .await?;
    }

    if seen == 0 && config.read().await.kad.enable_mock_results {
        callback_client
            .post_results(&ResultBatch {
                job_id: Some(job.job_id),
                indexer_id,
                protocol: Protocol::Kad2,
                files: vec![mock_file_record(&job.query, dht.bind_addr()?.to_string())],
            })
            .await?;
    }

    Ok(())
}

async fn seed_popular_impl(dht: &DhtNode, hashes: Vec<PopularHash>) -> Result<()> {
    if !dht.is_bootstrapped() {
        anyhow::bail!("kad node is not bootstrapped yet");
    }

    let bind_addr = dht.bind_addr()?;
    for hash in hashes {
        let HashType::Ed2k(raw_hash) = hash.hash;
        let file_hash = Ed2kHash::from_str(&raw_hash)
            .with_context(|| format!("invalid Ed2k hash {raw_hash}"))?;
        let keyword_hash = keyword_target(&hash.canonical_name);
        let keyword_tags = vec![
            Tag::filename(hash.canonical_name.clone()),
            Tag::filesize(hash.size),
            Tag::sources(hash.source_count),
        ];
        let _ = dht
            .publish_keyword(keyword_hash, file_hash, keyword_tags)
            .await;
        let source_tags = vec![
            Tag::new_short(tag_name::SOURCEPORT, TagValue::U16(bind_addr.port())),
            Tag::new_short(tag_name::SOURCEUPORT, TagValue::U16(bind_addr.port())),
            Tag::new_short(tag_name::SOURCETYPE, TagValue::U8(1)),
        ];
        let _ = dht.publish_source(file_hash, source_tags).await;
    }

    Ok(())
}

async fn restore_snoop_queue(
    coordinator: &CoordinatorClient,
    indexer_id: Uuid,
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
) {
    match coordinator.restore_snoop(indexer_id).await {
        Ok(entries) => merge_snoop_entries(snoop_queue, entries).await,
        Err(error) => warn!("failed to restore snoop queue: {error}"),
    }
}

async fn flush_snoop_queue(
    coordinator: &CoordinatorClient,
    indexer_id: Uuid,
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
) -> Result<()> {
    let entries = {
        let queue = snoop_queue.lock().await;
        queue.values().cloned().collect::<Vec<_>>()
    };
    coordinator.flush_snoop(indexer_id, &entries).await
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn read_optional_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(fs::read(path).with_context(|| {
        format!("failed to read {}", path.display())
    })?))
}

fn load_or_create_indexer_id(path: &str) -> Result<Uuid> {
    let path = Path::new(path);
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return Ok(Uuid::parse_str(contents.trim())
            .with_context(|| format!("invalid uuid in {}", path.display()))?);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let generated = Uuid::new_v4();
    fs::write(path, generated.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(generated)
}

fn load_or_create_node_id(path: &Path) -> Result<NodeId> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return NodeId::from_str(contents.trim())
            .with_context(|| format!("invalid node id in {}", path.display()));
    }
    let node_id = NodeId::from_bytes(rand::random());
    fs::write(path, node_id.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(node_id)
}

fn load_or_create_udp_key(path: &Path) -> Result<u32> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return contents
            .trim()
            .parse::<u32>()
            .with_context(|| format!("invalid udp key in {}", path.display()));
    }
    let udp_key: u32 = rand::random();
    fs::write(path, udp_key.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(udp_key)
}

fn significant_keyword_words(query: &str) -> Vec<String> {
    let words: Vec<String> = query
        .split(|char: char| !char.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .filter(|word| word.len() >= 3)
        .collect();
    if words.is_empty() {
        vec![query.to_lowercase()]
    } else {
        words
    }
}

fn keyword_target(query: &str) -> NodeId {
    let first_word = significant_keyword_words(query)
        .into_iter()
        .next()
        .unwrap_or_else(|| query.to_lowercase());
    let mut hasher = Md4::new();
    hasher.update(first_word.as_bytes());
    let digest: [u8; 16] = hasher.finalize().into();
    let mut wire = [0u8; 16];
    for chunk in 0..4 {
        let base = chunk * 4;
        wire[base] = digest[base + 3];
        wire[base + 1] = digest[base + 2];
        wire[base + 2] = digest[base + 1];
        wire[base + 3] = digest[base];
    }
    NodeId::from_bytes(wire)
}

fn guess_content_type(name: Option<&String>) -> Option<ContentType> {
    let Some(name) = name else {
        return Some(ContentType::Unknown);
    };
    let lower = name.to_lowercase();
    let value = if [".mkv", ".mp4", ".avi", ".mov"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Video
    } else if [".mp3", ".flac", ".wav", ".ogg"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Audio
    } else if [".pdf", ".epub", ".txt", ".doc", ".docx"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Document
    } else if [".zip", ".rar", ".7z", ".tar"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Archive
    } else if [".exe", ".msi", ".iso"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Software
    } else {
        ContentType::Unknown
    };
    Some(value)
}

fn mock_file_record(query: &str, bind_addr: String) -> FileRecord {
    FileRecord {
        hashes: vec![HashType::Ed2k(hex::encode(keyword_target(query).0))],
        names: vec![format!(
            "{}.bin",
            query.trim().replace(' ', "_").to_lowercase()
        )],
        size: Some(1_048_576),
        content_type: Some(ContentType::Unknown),
        tags: vec![TagEntry {
            key: "origin".into(),
            value: serde_json::json!("mock_fallback"),
        }],
        sources: vec![Source {
            protocol: Protocol::Kad2,
            address: bind_addr,
            extra: serde_json::json!({ "search_mode": "mock" }),
        }],
    }
}

fn tag_to_entry(tag: &Tag) -> TagEntry {
    let key = match &tag.name {
        TagName::Short(value) => format!("tag_{value:02x}"),
        TagName::Long(value) => value.clone(),
    };
    let value = match &tag.value {
        TagValue::Hash(value) => serde_json::json!(value.to_string()),
        TagValue::String(value) => serde_json::json!(value),
        TagValue::U64(value) => serde_json::json!(value),
        TagValue::U32(value) => serde_json::json!(value),
        TagValue::U16(value) => serde_json::json!(value),
        TagValue::U8(value) => serde_json::json!(value),
        TagValue::Float(value) => serde_json::json!(value),
        TagValue::Bool(value) => serde_json::json!(value),
        TagValue::Blob(value) => serde_json::json!(hex::encode(value)),
    };
    TagEntry { key, value }
}

fn map_search_result_for(dht: &DhtNode, result: &SearchResult) -> Result<FileRecord> {
    Ok(FileRecord {
        hashes: vec![HashType::Ed2k(result.hash.to_string())],
        names: result.names.clone(),
        size: result.size,
        content_type: guess_content_type(result.names.first()),
        tags: result.tags.iter().map(tag_to_entry).collect(),
        sources: vec![Source {
            protocol: Protocol::Kad2,
            address: dht.bind_addr()?.to_string(),
            extra: serde_json::json!({
                "search_mode": "network",
                "availability": result.availability,
            }),
        }],
    })
}

async fn merge_snoop_entries(
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
    entries: Vec<SnoopEntry>,
) {
    let mut queue = snoop_queue.lock().await;
    for entry in entries {
        merge_snoop_entry(&mut queue, entry);
    }
}

fn merge_snoop_entry(queue: &mut HashMap<String, SnoopEntry>, entry: SnoopEntry) {
    queue
        .entry(entry.query.clone())
        .and_modify(|existing| {
            existing.hit_count = existing.hit_count.saturating_add(entry.hit_count);
            existing.last_seen = existing.last_seen.max(entry.last_seen);
            existing.first_seen = existing.first_seen.min(entry.first_seen);
            if existing.hash.is_none() {
                existing.hash = entry.hash.clone();
            }
        })
        .or_insert(entry);
}

async fn record_snoop_entry(
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
    query: String,
    hash: Option<HashType>,
) {
    let now = Utc::now();
    let entry = SnoopEntry {
        query,
        hash,
        hit_count: 1,
        first_seen: now,
        last_seen: now,
    };
    let mut queue = snoop_queue.lock().await;
    merge_snoop_entry(&mut queue, entry);
}

async fn next_passive_keyword_target(
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
) -> Option<NodeId> {
    let queue = snoop_queue.lock().await;
    queue
        .values()
        .filter_map(|entry| entry.query.strip_prefix("keyword:"))
        .filter_map(|raw| NodeId::from_str(raw).ok())
        .next()
}

async fn persist_nodes_dat_for(dht: &DhtNode, state_paths: &AgentStatePaths) -> Result<()> {
    let contacts = dht
        .routing_contacts()
        .await
        .into_iter()
        .map(|contact| BootstrapContact {
            node_id: contact.id,
            ip: contact.ip,
            udp_port: contact.udp_port,
            tcp_port: contact.tcp_port,
            version: contact.kad_version,
        })
        .collect::<Vec<_>>();
    let bytes = encode_nodes_dat(&contacts)?;
    fs::write(&state_paths.nodes_dat_path, bytes)
        .with_context(|| format!("failed to write {}", state_paths.nodes_dat_path.display()))?;
    Ok(())
}

async fn handle_unsolicited_packet(
    dht: &DhtNode,
    snoop_queue: &Arc<Mutex<HashMap<String, SnoopEntry>>>,
    packet: KadPacket,
    from: SocketAddr,
) -> Result<()> {
    match packet {
        KadPacket::Ping => dht.send_packet(from, &KadPacket::Pong).await?,
        KadPacket::HelloReq(req) => {
            if let Some(udp_key) = req.udp_key {
                dht.register_peer_key(from, udp_key);
            }
            if let std::net::IpAddr::V4(ip) = from.ip() {
                let _ = dht
                    .add_contact(Contact::new(
                        req.node_id,
                        ip,
                        from.port(),
                        req.tcp_port,
                        req.version,
                    ))
                    .await;
            }
            let bind_addr = dht.bind_addr()?;
            let tcp_ip = match bind_addr.ip() {
                std::net::IpAddr::V4(ip) => u32::from_be_bytes(ip.octets()),
                std::net::IpAddr::V6(_) => 0,
            };
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::HelloRes(overlord_kad_proto::HelloRes {
                        node_id: dht.own_id(),
                        tcp_ip,
                        tcp_port: bind_addr.port(),
                        version: overlord_kad_proto::KAD_VERSION,
                        udp_key: Some(dht.udp_key()),
                        tags: Vec::new(),
                    }),
                )
                .await;
        }
        KadPacket::HelloRes(res) => {
            if let Some(udp_key) = res.udp_key {
                dht.register_peer_key(from, udp_key);
            }
            if let std::net::IpAddr::V4(ip) = from.ip() {
                let _ = dht
                    .add_contact(Contact::new(
                        res.node_id,
                        ip,
                        from.port(),
                        res.tcp_port,
                        res.version,
                    ))
                    .await;
            }
            let _ = dht.send_packet(from, &KadPacket::HelloResAck).await;
        }
        KadPacket::BootstrapReq => {
            let bind_addr = dht.bind_addr()?;
            let contacts = dht
                .closest_contacts(&dht.own_id(), K)
                .await
                .into_iter()
                .map(contact_to_entry)
                .collect();
            dht.send_packet(
                from,
                &KadPacket::BootstrapRes(overlord_kad_proto::BootstrapRes {
                    sender_id: dht.own_id(),
                    sender_tcp_port: bind_addr.port(),
                    sender_version: overlord_kad_proto::KAD_VERSION,
                    contacts,
                }),
            )
            .await?;
        }
        KadPacket::Req(req) => {
            let contacts = dht
                .closest_contacts(&req.target, req.count as usize)
                .await
                .into_iter()
                .map(contact_to_entry)
                .collect();
            dht.send_packet(
                from,
                &KadPacket::Res(overlord_kad_proto::Res {
                    target: req.target,
                    contacts,
                }),
            )
            .await?;
        }
        KadPacket::SearchKeyReq(req) => {
            record_snoop_entry(snoop_queue, format!("keyword:{}", req.target), None).await
        }
        KadPacket::SearchSourceReq(req) => {
            record_snoop_entry(
                snoop_queue,
                format!("source:{}", req.target),
                Some(HashType::Ed2k(hex::encode(req.target.0))),
            )
            .await
        }
        KadPacket::SearchNotesReq(req) => {
            record_snoop_entry(
                snoop_queue,
                format!("notes:{}", req.target),
                Some(HashType::Ed2k(hex::encode(req.target.0))),
            )
            .await
        }
        KadPacket::PublishKeyReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        KadPacket::PublishSourceReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        KadPacket::PublishNotesReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        _ => {}
    }
    Ok(())
}

fn contact_to_entry(contact: Contact) -> ContactEntry {
    ContactEntry {
        node_id: contact.id,
        ip: u32::from_be_bytes(contact.ip.octets()),
        udp_port: contact.udp_port,
        tcp_port: contact.tcp_port,
        version: contact.kad_version,
    }
}

#[cfg(test)]
mod tests {
    use super::{keyword_target, significant_keyword_words};

    #[test]
    fn significant_words_ignore_short_tokens() {
        assert_eq!(
            significant_keyword_words("A torino x train"),
            vec!["torino".to_string(), "train".to_string()]
        );
    }

    #[test]
    fn keyword_target_is_stable() {
        assert_eq!(
            hex::encode(keyword_target("Torino Train").0),
            "b2bc3aa39f375069e7c27eb83ce6baf3"
        );
    }
}

impl AgentStatePaths {
    fn from_config(config: &EmuleAgentConfig) -> Self {
        let state_dir = PathBuf::from(&config.agent.state_dir);
        let nodes_dat_path = if config.kad.nodes_dat_path.trim().is_empty() {
            state_dir.join("overlord-kad.nodes.dat")
        } else {
            PathBuf::from(&config.kad.nodes_dat_path)
        };
        Self {
            node_id_path: state_dir.join("overlord-kad.node-id"),
            udp_key_path: state_dir.join("overlord-kad.udp-key"),
            nodes_dat_path,
        }
    }
}

#[async_trait]
impl IndexerService for OverlordAgentEmule {
    fn protocol(&self) -> Protocol {
        Protocol::Kad2
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn indexer_id(&self) -> Uuid {
        self.indexer_id
    }

    async fn start(&self) -> Result<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.shutdown.store(false, Ordering::SeqCst);
        restore_snoop_queue(&self.coordinator, self.indexer_id, &self.snoop_queue).await;
        self.tasks.lock().await.push(self.dht.start());
        self.spawn_background_tasks().await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };
        for task in tasks {
            task.abort();
        }
        flush_snoop_queue(&self.coordinator, self.indexer_id, &self.snoop_queue).await?;
        Ok(())
    }

    async fn search(&self, job: SearchJob) -> Result<()> {
        let dht = self.dht.clone();
        let indexer_id = self.indexer_id;
        let config = self.config.clone();
        tokio::spawn(async move {
            if let Err(error) = do_active_keyword_search(&dht, indexer_id, &job, config).await {
                warn!("active search failed: {error}");
            }
        });
        Ok(())
    }

    async fn stats(&self) -> Result<IndexerStats> {
        let queue_depth = self.snoop_queue.lock().await.len() as u32;
        let uptime_secs = self.started_at.elapsed().as_secs();
        let crawl_rate = if uptime_secs == 0 {
            0.0
        } else {
            self.passive_result_count.load(Ordering::Relaxed) as f32 / uptime_secs as f32
        };

        Ok(IndexerStats {
            indexer_id: self.indexer_id,
            protocol: Protocol::Kad2,
            peers_connected: self.dht.routing_table_size() as u32,
            crawl_rate,
            snoop_queue_depth: queue_depth,
            staging_queue_depth: 0,
            uptime_secs,
        })
    }

    async fn apply_config(&self, config: ConfigUpdate) -> Result<()> {
        let next_kad: KadConfig = serde_json::from_value(config.config)
            .context("invalid Kad config payload for overlord-agent-emule")?;
        self.config.write().await.kad = next_kad;
        anyhow::bail!("live Kad config updates are not supported yet; restart the agent")
    }

    async fn seed_popular(&self, hashes: Vec<PopularHash>) -> Result<()> {
        seed_popular_impl(&self.dht, hashes).await
    }

    async fn flush_snoop(&self) -> Result<Vec<SnoopEntry>> {
        let queue = self.snoop_queue.lock().await;
        Ok(queue.values().cloned().collect())
    }
}

impl OverlordAgentEmule {
    async fn spawn_background_tasks(&self) {
        let dht = self.dht.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let state_paths = self.state_paths.clone();
        self.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) && !dht.is_bootstrapped() {
                match dht.bootstrap().await {
                    Ok(()) => {
                        if let Err(error) = persist_nodes_dat_for(&dht, &state_paths).await {
                            warn!("failed to persist nodes.dat after bootstrap: {error}");
                        }
                        break;
                    }
                    Err(error) => debug!("bootstrap retry failed: {error}"),
                }
                tokio::time::sleep(Duration::from_secs(BOOTSTRAP_RETRY_SECS)).await;
            }
        }));

        let dht = self.dht.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        self.tasks.lock().await.push(tokio::spawn(async move {
            let mut packets = dht.subscribe_packets();
            while !shutdown.load(Ordering::Relaxed) {
                match packets.recv().await {
                    Ok((packet, from)) => {
                        if let Err(error) =
                            handle_unsolicited_packet(&dht, &snoop_queue, packet, from).await
                        {
                            debug!("unsolicited packet handling failed: {error}");
                        }
                    }
                    Err(error) => {
                        debug!("packet subscription closed: {error}");
                        break;
                    }
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let dht = self.dht.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        let indexer_id = self.indexer_id;
        let passive_result_count = Arc::clone(&self.passive_result_count);
        self.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(PASSIVE_CRAWL_SECS)).await;
                if shutdown.load(Ordering::Relaxed) || !dht.is_bootstrapped() {
                    continue;
                }
                let Some(target) = next_passive_keyword_target(&snoop_queue).await else {
                    continue;
                };
                let mut stream = dht.search_keywords(target);
                let mut files = Vec::new();
                while let Some(result) = stream.next().await {
                    if let Ok(file) = map_search_result_for(&dht, &result) {
                        passive_result_count.fetch_add(1, Ordering::Relaxed);
                        files.push(file);
                        if files.len() >= PASSIVE_BATCH_SIZE {
                            let payload = ResultBatch {
                                job_id: None,
                                indexer_id,
                                protocol: Protocol::Kad2,
                                files: std::mem::take(&mut files),
                            };
                            if let Err(error) = coordinator.post_results(&payload).await {
                                warn!("failed to post passive result batch: {error}");
                            }
                        }
                    }
                }
                if !files.is_empty() {
                    let payload = ResultBatch {
                        job_id: None,
                        indexer_id,
                        protocol: Protocol::Kad2,
                        files,
                    };
                    if let Err(error) = coordinator.post_results(&payload).await {
                        warn!("failed to post passive result batch: {error}");
                    }
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        let indexer_id = self.indexer_id;
        self.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(SNOOP_FLUSH_SECS)).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(error) = flush_snoop_queue(&coordinator, indexer_id, &snoop_queue).await
                {
                    debug!("snoop flush failed: {error}");
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let dht = self.dht.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let republish_secs = self.config.read().await.kad.republish_interval_secs;
        self.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(republish_secs)).await;
                if shutdown.load(Ordering::Relaxed) || !dht.is_bootstrapped() {
                    continue;
                }
                match coordinator.popular_hashes().await {
                    Ok(hashes) => {
                        if let Err(error) = seed_popular_impl(&dht, hashes).await {
                            debug!("republish cycle failed: {error}");
                        }
                    }
                    Err(error) => debug!("popular hash refresh failed: {error}"),
                }
            }
        }));
    }
}
