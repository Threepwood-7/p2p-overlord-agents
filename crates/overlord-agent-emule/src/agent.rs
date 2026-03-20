use std::{fs, net::SocketAddr, path::Path, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use overlord_agent_common::{
    ConfigUpdate, ContentType, CoordinatorClient, FileRecord, HashType, IndexerServer,
    IndexerService, IndexerStats, Protocol, RegisterRequest, ResultBatch, SearchJob, Source,
    TagEntry,
};

use crate::{
    config::{EmuleAgentConfig, KadConfig},
    kad::dht::KadEngine,
};

pub struct OverlordAgentEmule {
    config: Arc<RwLock<EmuleAgentConfig>>,
    coordinator: CoordinatorClient,
    indexer_id: Uuid,
    kad: KadEngine,
    started_at: Instant,
}

impl OverlordAgentEmule {
    pub async fn new(config: EmuleAgentConfig) -> Result<Self> {
        let indexer_id = load_or_create_indexer_id(&config.agent.indexer_id_path)?;
        let coordinator = CoordinatorClient::new(&config.coordinator.url)?;
        let kad = KadEngine::new(config.kad.clone()).await?;
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            coordinator,
            indexer_id,
            kad,
            started_at: Instant::now(),
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

    fn map_results_to_batch(
        &self,
        job: &SearchJob,
        results: Vec<crate::kad::dht::KeywordSearchResult>,
    ) -> ResultBatch {
        ResultBatch {
            job_id: Some(job.job_id),
            indexer_id: self.indexer_id,
            protocol: Protocol::Kad2,
            files: results
                .into_iter()
                .map(|result| FileRecord {
                    hashes: vec![HashType::Ed2k(result.file_hash_hex.clone())],
                    names: vec![result.file_name.clone()],
                    size: Some(result.file_size),
                    content_type: Some(ContentType::Unknown),
                    tags: crate::kad::dht::KadEngine::result_tags(&result)
                        .into_iter()
                        .map(|(key, value)| TagEntry { key, value })
                        .collect(),
                    sources: vec![Source {
                        protocol: Protocol::Kad2,
                        address: self
                            .kad
                            .udp_bind_addr()
                            .unwrap_or_else(|_| "0.0.0.0:41000".to_string()),
                        extra: serde_json::json!({
                            "search_mode": "mock",
                            "availability": result.availability,
                        }),
                    }],
                })
                .collect(),
        }
    }
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
        let contacts = self.kad.bootstrap().await?;
        info!("bootstrap complete with {} configured contacts", contacts);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn search(&self, job: SearchJob) -> Result<()> {
        let results = self.kad.search_keywords(&job.query).await?;
        let batch = self.map_results_to_batch(&job, results);
        let callback_client = CoordinatorClient::new(&job.callback_url)?;
        callback_client.post_results(&batch).await?;
        Ok(())
    }

    async fn stats(&self) -> Result<IndexerStats> {
        Ok(IndexerStats {
            indexer_id: self.indexer_id,
            protocol: Protocol::Kad2,
            peers_connected: self.kad.peers_connected().await as u32,
            crawl_rate: 0.0,
            snoop_queue_depth: 0,
            staging_queue_depth: 0,
            uptime_secs: self.started_at.elapsed().as_secs(),
        })
    }

    async fn apply_config(&self, config: ConfigUpdate) -> Result<()> {
        let mut current = self.config.write().await;
        let next_kad: KadConfig = serde_json::from_value(config.config)
            .context("invalid Kad config payload for overlord-agent-emule")?;
        self.kad.apply_config(next_kad.clone()).await?;
        current.kad = next_kad;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::OverlordAgentEmule;
    use crate::config::EmuleAgentConfig;
    use overlord_agent_common::{IndexerService, SearchJob};
    use uuid::Uuid;

    #[tokio::test]
    async fn search_posts_non_empty_batch_shape() {
        let mut config = EmuleAgentConfig::default();
        config.agent.indexer_id_path = format!(
            "{}/overlord-agent-emule-test-id",
            std::env::temp_dir().display()
        );
        config.kad.udp_bind_addr = "127.0.0.1:0".to_string();
        let agent = OverlordAgentEmule::new(config).await.expect("agent");
        agent.start().await.expect("start");
        let stats = agent.stats().await.expect("stats");
        assert_eq!(stats.protocol, overlord_agent_common::Protocol::Kad2);
        let batch = agent.map_results_to_batch(
            &SearchJob {
                job_id: Uuid::new_v4(),
                query: "torino train".to_string(),
                callback_url: "http://127.0.0.1:13300".to_string(),
            },
            agent
                .kad
                .search_keywords("torino train")
                .await
                .expect("results"),
        );
        assert_eq!(batch.files.len(), 1);
        assert_eq!(batch.files[0].names[0], "torino_train.bin");
    }
}
