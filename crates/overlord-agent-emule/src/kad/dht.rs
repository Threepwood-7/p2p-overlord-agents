use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use serde_json::json;
use tokio::sync::RwLock;

use crate::config::KadConfig;

use super::{
    net::KadUdpEndpoint,
    proto::{NodeId, keyword_target},
    routing::RoutingTable,
};

#[derive(Debug, Clone)]
pub struct KeywordSearchResult {
    pub file_name: String,
    pub file_size: u64,
    pub file_hash_hex: String,
    pub availability: u32,
    pub target_hex: String,
}

pub struct KadEngine {
    config: Arc<RwLock<KadConfig>>,
    endpoint: KadUdpEndpoint,
    routing_table: Arc<RwLock<RoutingTable>>,
    started_at: Instant,
}

impl KadEngine {
    pub async fn new(config: KadConfig) -> Result<Self> {
        let endpoint = KadUdpEndpoint::bind(&config.udp_bind_addr).await?;
        let routing_table = Arc::new(RwLock::new(RoutingTable::default()));
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            endpoint,
            routing_table,
            started_at: Instant::now(),
        })
    }

    pub async fn bootstrap(&self) -> Result<usize> {
        let config = self.config.read().await.clone();
        let mut routing = self.routing_table.write().await;
        for node in &config.bootstrap_nodes {
            routing.add_contact(node.clone(), NodeId::from_bytes([0; 16]));
        }
        Ok(routing.len())
    }

    pub async fn apply_config(&self, config: KadConfig) -> Result<()> {
        *self.config.write().await = config;
        Ok(())
    }

    pub async fn peers_connected(&self) -> usize {
        self.routing_table.read().await.len()
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn udp_bind_addr(&self) -> Result<String> {
        Ok(self.endpoint.local_addr()?.to_string())
    }

    pub async fn search_keywords(&self, query: &str) -> Result<Vec<KeywordSearchResult>> {
        let config = self.config.read().await.clone();
        let target = keyword_target(query);
        let target_hex = hex::encode(target.into_bytes());

        if !config.enable_mock_results {
            return Ok(Vec::new());
        }

        let seed_name = query.trim().replace(' ', "_").to_lowercase();
        let seed = if seed_name.is_empty() {
            "untitled".to_string()
        } else {
            seed_name
        };
        let hash_hex = &target_hex[..32];

        Ok(vec![KeywordSearchResult {
            file_name: format!("{seed}.bin"),
            file_size: 1_048_576,
            file_hash_hex: hash_hex.to_string(),
            availability: 1,
            target_hex,
        }])
    }

    pub fn result_tags(result: &KeywordSearchResult) -> Vec<(String, serde_json::Value)> {
        vec![
            ("availability".to_string(), json!(result.availability)),
            ("target".to_string(), json!(result.target_hex)),
            ("origin".to_string(), json!("scaffold_mock")),
        ]
    }
}
