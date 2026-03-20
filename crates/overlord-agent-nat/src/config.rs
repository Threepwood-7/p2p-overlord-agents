use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatConfig {
    pub enabled: bool,
    pub backend_order: Vec<String>,
    pub selected_interface_name: Option<String>,
    pub selection_confirmed: bool,
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
            backend_order: vec!["upnp".to_string()],
            selected_interface_name: None,
            selection_confirmed: false,
            bind_ip: None,
            igd_ip: None,
            discovery_timeout_secs: 5,
            lease_duration_secs: 3_600,
            renew_margin_secs: 300,
            external_ip_override: None,
        }
    }
}
