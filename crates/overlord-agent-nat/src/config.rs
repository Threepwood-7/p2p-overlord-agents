use serde::{Deserialize, Serialize};

use crate::provider::default_upnp_backend_order;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatConfig {
    pub enabled: bool,
    pub backend_order: Vec<String>,
    pub bind_ip: Option<String>,
    pub igd_ip: Option<String>,
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
    use crate::provider::UPNP_RUPNP_BACKEND;

    #[test]
    fn default_nat_config_uses_explicit_rupnp_backend() {
        assert_eq!(
            NatConfig::default().backend_order,
            vec![UPNP_RUPNP_BACKEND.to_string()]
        );
    }
}
