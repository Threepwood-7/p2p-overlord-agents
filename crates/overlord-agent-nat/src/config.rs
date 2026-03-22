use serde::{Deserialize, Serialize};

use crate::provider::default_upnp_backend_order;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatConfig {
    pub enabled: bool,
    pub backend_order: Vec<String>,
    pub bind_ip: Option<String>,
    pub igd_ip: Option<String>,
    pub minissdpd_socket: Option<String>,
    pub ssdp_local_port: Option<u16>,
    pub discovery_timeout_secs: u64,
    pub lease_duration_secs: u32,
    pub renew_margin_secs: u64,
    pub external_ip_override: Option<String>,
}

impl Default for NatConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend_order: default_upnp_backend_order(),
            bind_ip: None,
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

#[cfg(test)]
mod tests {
    use super::NatConfig;
    use crate::provider::UPNP_MINIUPNPC_BACKEND;

    #[test]
    fn default_nat_config_prefers_miniupnpc_only() {
        assert_eq!(
            NatConfig::default().backend_order,
            vec![UPNP_MINIUPNPC_BACKEND.to_string()]
        );
    }
}
