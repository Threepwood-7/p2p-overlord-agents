use std::{fs, path::Path};

use anyhow::{Context, Result};
use overlord_agent_nat::default_upnp_backend_order;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EmuleAgentConfig {
    pub coordinator: CoordinatorConfig,
    pub agent: AgentConfig,
    pub control: ControlConfig,
    pub p2p: P2pConfig,
    pub nat: NatConfig,
    pub log: LogConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CoordinatorConfig {
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    pub indexer_id_path: String,
    pub state_dir: String,
    pub hostname: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlConfig {
    pub bind_iface: Option<String>,
    pub bind_ip: Option<String>,
    pub selection_confirmed: bool,
    pub listen_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct P2pConfig {
    pub bind_iface: Option<String>,
    pub bind_ip: Option<String>,
    pub selection_confirmed: bool,
    pub kad: KadConfig,
    pub ed2k: Ed2kConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct KadConfig {
    pub listen_port: u16,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Ed2kConfig {
    pub listen_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatConfig {
    pub p2p: NatP2pConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatP2pConfig {
    pub enabled: bool,
    pub backend_order: Vec<String>,
    pub igd_ip: Option<String>,
    pub minissdpd_socket: Option<String>,
    pub ssdp_local_port: Option<u16>,
    pub discovery_timeout_secs: u64,
    pub lease_duration_secs: u32,
    pub renew_margin_secs: u64,
    pub external_ip_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
}

impl Default for EmuleAgentConfig {
    fn default() -> Self {
        Self {
            coordinator: CoordinatorConfig::default(),
            agent: AgentConfig::default(),
            control: ControlConfig::default(),
            p2p: P2pConfig::default(),
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
            indexer_id_path: "./runtime/overlord-agent-emule.indexer-id".to_string(),
            state_dir: "./runtime".to_string(),
            hostname: "localhost".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            bind_iface: None,
            bind_ip: None,
            selection_confirmed: false,
            listen_port: 13_301,
        }
    }
}

impl Default for P2pConfig {
    fn default() -> Self {
        Self {
            bind_iface: None,
            bind_ip: None,
            selection_confirmed: false,
            kad: KadConfig::default(),
            ed2k: Ed2kConfig::default(),
        }
    }
}

impl Default for KadConfig {
    fn default() -> Self {
        Self {
            listen_port: 41_000,
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

impl Default for Ed2kConfig {
    fn default() -> Self {
        Self {
            listen_port: 41_001,
        }
    }
}

impl Default for NatConfig {
    fn default() -> Self {
        Self {
            p2p: NatP2pConfig::default(),
        }
    }
}

impl Default for NatP2pConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend_order: default_upnp_backend_order(),
            igd_ip: None,
            minissdpd_socket: None,
            ssdp_local_port: None,
            discovery_timeout_secs: 5,
            lease_duration_secs: 3_600,
            renew_margin_secs: 300,
            external_ip_override: None,
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
        normalize_control_config(&mut config.control);
        normalize_p2p_config(&mut config.p2p);
        normalize_nat_config(&mut config.nat);
        Ok(config)
    }
}

fn normalize_control_config(config: &mut ControlConfig) {
    for value in [&mut config.bind_iface, &mut config.bind_ip] {
        if value
            .as_deref()
            .is_some_and(|inner| inner.trim().is_empty())
        {
            *value = None;
        }
    }
}

fn normalize_p2p_config(config: &mut P2pConfig) {
    for value in [&mut config.bind_iface, &mut config.bind_ip] {
        if value
            .as_deref()
            .is_some_and(|inner| inner.trim().is_empty())
        {
            *value = None;
        }
    }
}

fn normalize_nat_config(config: &mut NatConfig) {
    for value in [
        &mut config.p2p.igd_ip,
        &mut config.p2p.minissdpd_socket,
        &mut config.p2p.external_ip_override,
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
    use super::{
        ControlConfig, NatConfig, NatP2pConfig, P2pConfig, normalize_control_config,
        normalize_nat_config, normalize_p2p_config,
    };
    use overlord_agent_nat::{UPNP_MINIUPNPC_BACKEND, UPNP_RUPNP_BACKEND};

    #[test]
    fn normalize_nat_config_drops_blank_optional_fields() {
        let mut config = NatConfig {
            p2p: NatP2pConfig {
                igd_ip: Some(String::new()),
                minissdpd_socket: Some(" ".to_string()),
                external_ip_override: Some("\t".to_string()),
                ..NatP2pConfig::default()
            },
        };

        normalize_nat_config(&mut config);

        assert_eq!(config.p2p.igd_ip, None);
        assert_eq!(config.p2p.minissdpd_socket, None);
        assert_eq!(config.p2p.external_ip_override, None);
    }

    #[test]
    fn default_nat_config_prefers_miniupnpc_then_rupnp() {
        assert_eq!(
            NatP2pConfig::default().backend_order,
            vec![
                UPNP_MINIUPNPC_BACKEND.to_string(),
                UPNP_RUPNP_BACKEND.to_string()
            ]
        );
    }

    #[test]
    fn normalize_control_config_drops_blank_optional_fields() {
        let mut config = ControlConfig {
            bind_iface: Some(" ".to_string()),
            bind_ip: Some("\t".to_string()),
            ..ControlConfig::default()
        };

        normalize_control_config(&mut config);

        assert_eq!(config.bind_iface, None);
        assert_eq!(config.bind_ip, None);
    }

    #[test]
    fn normalize_p2p_config_drops_blank_optional_fields() {
        let mut config = P2pConfig {
            bind_iface: Some(" ".to_string()),
            bind_ip: Some("\t".to_string()),
            ..P2pConfig::default()
        };

        normalize_p2p_config(&mut config);

        assert_eq!(config.bind_iface, None);
        assert_eq!(config.bind_ip, None);
    }
}
