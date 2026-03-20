use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EmuleAgentConfig {
    pub coordinator: CoordinatorConfig,
    pub agent: AgentConfig,
    pub kad: KadConfig,
    pub log: LogConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CoordinatorConfig {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    pub bind_addr: String,
    pub indexer_id_path: String,
    pub state_dir: String,
    pub hostname: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KadConfig {
    pub udp_bind_addr: String,
    pub bootstrap_nodes: Vec<String>,
    pub search_timeout_secs: u64,
    pub enable_mock_results: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
}

impl Default for EmuleAgentConfig {
    fn default() -> Self {
        Self {
            coordinator: CoordinatorConfig::default(),
            agent: AgentConfig::default(),
            kad: KadConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:13300".to_string(),
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:13301".to_string(),
            indexer_id_path: "./runtime/overlord-agent-emule.indexer-id".to_string(),
            state_dir: "./runtime".to_string(),
            hostname: "localhost".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl Default for KadConfig {
    fn default() -> Self {
        Self {
            udp_bind_addr: "0.0.0.0:41000".to_string(),
            bootstrap_nodes: Vec::new(),
            search_timeout_secs: 8,
            enable_mock_results: true,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

impl EmuleAgentConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read config from {}", path.display()))?;
        toml::from_str(&contents)
            .with_context(|| format!("failed to parse config from {}", path.display()))
    }
}
