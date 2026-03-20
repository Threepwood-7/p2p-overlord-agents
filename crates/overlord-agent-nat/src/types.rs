use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

impl TransportProtocol {
    pub fn as_upnp_token(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MappingExposure {
    #[default]
    Required,
    Preferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MappingSpec {
    pub name: String,
    pub local_addr: SocketAddr,
    pub protocol: TransportProtocol,
    #[serde(default)]
    pub exposure: MappingExposure,
    pub preferred_external_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedGateway {
    pub backend: String,
    pub control_url: String,
    pub local_addr: Option<String>,
    pub gateway_addr: Option<String>,
    pub external_ip: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MappedEndpoint {
    pub name: String,
    pub protocol: TransportProtocol,
    pub local_addr: SocketAddr,
    pub external_addr: SocketAddr,
    pub lease_expires_in_secs: u32,
    pub backend: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatStatusSnapshot {
    pub enabled: bool,
    pub gateway_discovered: bool,
    pub backend: Option<String>,
    pub bind_ip: Option<String>,
    pub igd_ip: Option<String>,
    pub external_ip_override: Option<String>,
    pub gateway: Option<SelectedGateway>,
    #[serde(default)]
    pub mappings: Vec<MappedEndpoint>,
    #[serde(default)]
    pub observed_external_addresses: Vec<String>,
    pub last_refresh_unix_secs: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatStatus {
    pub enabled: bool,
    pub gateway_discovered: bool,
    pub backend: Option<String>,
    pub bind_ip: Option<String>,
    pub igd_ip: Option<String>,
    pub external_ip_override: Option<String>,
    pub gateway: Option<SelectedGateway>,
    pub mappings: Vec<MappedEndpoint>,
    pub observed_external_addresses: Vec<String>,
    pub last_refresh_unix_secs: Option<u64>,
    pub last_error: Option<String>,
}

impl Default for NatStatus {
    fn default() -> Self {
        Self {
            enabled: false,
            gateway_discovered: false,
            backend: None,
            bind_ip: None,
            igd_ip: None,
            external_ip_override: None,
            gateway: None,
            mappings: Vec::new(),
            observed_external_addresses: Vec::new(),
            last_refresh_unix_secs: None,
            last_error: None,
        }
    }
}

impl NatStatus {
    pub fn snapshot(&self) -> NatStatusSnapshot {
        NatStatusSnapshot {
            enabled: self.enabled,
            gateway_discovered: self.gateway_discovered,
            backend: self.backend.clone(),
            bind_ip: self.bind_ip.clone(),
            igd_ip: self.igd_ip.clone(),
            external_ip_override: self.external_ip_override.clone(),
            gateway: self.gateway.clone(),
            mappings: self.mappings.clone(),
            observed_external_addresses: self.observed_external_addresses.clone(),
            last_refresh_unix_secs: self.last_refresh_unix_secs,
            last_error: self.last_error.clone(),
        }
    }
}
