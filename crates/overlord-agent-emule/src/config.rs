use std::{fs, path::Path};

use anyhow::{Context, Result};
use overlord_agent_nat::NatConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EmuleAgentConfig {
    pub coordinator: CoordinatorConfig,
    pub agent: AgentConfig,
    pub kad: KadConfig,
    pub nat: NatConfig,
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
    pub ed2k_bind_addr: String,
    pub nodes_dat_path: String,
    pub bootstrap_nodes: Vec<String>,
    pub search_timeout_secs: u64,
    pub store_timeout_secs: u64,
    pub republish_interval_secs: u64,
    pub max_outbound_pps: u32,
    pub search_phase2_fanout: usize,
    pub keyword_result_cap: usize,
    pub source_result_cap: usize,
    pub notes_result_cap: usize,
    pub obfuscation_enabled: bool,
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
            nat: NatConfig::default(),
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
            ed2k_bind_addr: "0.0.0.0:41001".to_string(),
            nodes_dat_path: "./runtime/overlord-kad.nodes.dat".to_string(),
            bootstrap_nodes: Vec::new(),
            search_timeout_secs: 45,
            store_timeout_secs: 140,
            republish_interval_secs: 18_000,
            max_outbound_pps: 50,
            search_phase2_fanout: 50,
            keyword_result_cap: 5_000,
            source_result_cap: 1_000,
            notes_result_cap: 1_000,
            obfuscation_enabled: true,
            enable_mock_results: false,
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
        let mut config: Self = toml::from_str(&contents)
            .with_context(|| format!("failed to parse config from {}", path.display()))?;
        normalize_nat_config(&mut config.nat);
        Ok(config)
    }
}

fn normalize_nat_config(config: &mut NatConfig) {
    for value in [
        &mut config.selected_interface_name,
        &mut config.bind_ip,
        &mut config.igd_ip,
        &mut config.external_ip_override,
    ] {
        if value
            .as_deref()
            .is_some_and(|inner| inner.trim().is_empty())
        {
            *value = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NatConfig, normalize_nat_config};

    #[test]
    fn normalize_nat_config_drops_blank_optional_fields() {
        let mut config = NatConfig {
            selected_interface_name: Some(" ".to_string()),
            bind_ip: Some(" ".to_string()),
            igd_ip: Some(String::new()),
            external_ip_override: Some("\t".to_string()),
            ..NatConfig::default()
        };

        normalize_nat_config(&mut config);

        assert_eq!(config.selected_interface_name, None);
        assert_eq!(config.bind_ip, None);
        assert_eq!(config.igd_ip, None);
        assert_eq!(config.external_ip_override, None);
    }
}
