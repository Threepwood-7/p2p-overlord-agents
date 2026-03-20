use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use rupnp::{
    Device, Service,
    ssdp::{SearchTarget, URN},
};
use tokio::sync::RwLock;

use crate::{
    config::NatConfig,
    types::{MappedEndpoint, MappingSpec, NatStatus},
};

const WAN_IP_CONNECTION_1: URN = URN::service("schemas-upnp-org", "WANIPConnection", 1);
const WAN_IP_CONNECTION_2: URN = URN::service("schemas-upnp-org", "WANIPConnection", 2);
const WAN_PPP_CONNECTION_1: URN = URN::service("schemas-upnp-org", "WANPPPConnection", 1);

#[async_trait]
pub trait NatCapableAgent: Send + Sync + 'static {
    fn nat_config(&self) -> NatConfig;
    fn nat_mappings(&self) -> Vec<MappingSpec>;
}

#[async_trait]
pub trait PortMappingProvider: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn reconcile(
        &self,
        config: &NatConfig,
        mappings: &[MappingSpec],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()>;

    async fn release(
        &self,
        config: &NatConfig,
        mappings: &[MappedEndpoint],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct RupnpPortMappingProvider;

#[derive(Clone)]
struct GatewayHandle {
    device: Device,
    service: Service,
}

#[async_trait]
impl PortMappingProvider for RupnpPortMappingProvider {
    fn name(&self) -> &'static str {
        "upnp"
    }

    async fn reconcile(
        &self,
        config: &NatConfig,
        mappings: &[MappingSpec],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()> {
        if mappings.is_empty() {
            return Ok(());
        }

        let gateway = discover_gateway(config).await?;
        let external_ip_text = if let Some(override_ip) = config.external_ip_override.clone() {
            Some(override_ip)
        } else {
            gateway.external_ip().await.ok()
        };

        let mut mapped = Vec::with_capacity(mappings.len());
        for spec in mappings {
            let external_port = spec
                .preferred_external_port
                .unwrap_or_else(|| spec.local_addr.port());
            gateway
                .add_mapping(config, config.lease_duration_secs, spec, external_port)
                .await
                .with_context(|| format!("failed to add {} mapping", spec.name))?;

            let external_ip = external_ip_text
                .clone()
                .unwrap_or_else(|| gateway.mapping_internal_ip(config, spec).to_string())
                .parse::<IpAddr>()
                .with_context(|| format!("invalid external ip for {}", spec.name))?;

            mapped.push(MappedEndpoint {
                name: spec.name.clone(),
                protocol: spec.protocol,
                local_addr: spec.local_addr,
                external_addr: SocketAddr::new(external_ip, external_port),
                lease_expires_in_secs: config.lease_duration_secs,
                backend: self.name().to_string(),
            });
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut guard = status.write().await;
        guard.enabled = true;
        guard.gateway_discovered = true;
        guard.backend = Some(self.name().to_string());
        guard.bind_ip = config.bind_ip.clone();
        guard.igd_ip = config.igd_ip.clone();
        guard.gateway = Some(gateway.selected_gateway(external_ip_text.clone()));
        guard.observed_external_addresses = external_ip_text.into_iter().collect();
        guard.mappings = mapped;
        guard.last_refresh_unix_secs = Some(now);
        guard.last_error = None;
        Ok(())
    }

    async fn release(
        &self,
        config: &NatConfig,
        mappings: &[MappedEndpoint],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()> {
        if mappings.is_empty() {
            return Ok(());
        }
        let gateway = discover_gateway(config).await?;
        for mapping in mappings {
            let spec = MappingSpec {
                name: mapping.name.clone(),
                local_addr: mapping.local_addr,
                protocol: mapping.protocol,
                exposure: Default::default(),
                preferred_external_port: Some(mapping.external_addr.port()),
            };
            let _ = gateway
                .delete_mapping(&spec, mapping.external_addr.port())
                .await;
        }
        let mut guard = status.write().await;
        guard.mappings.clear();
        Ok(())
    }
}

impl GatewayHandle {
    async fn add_mapping(
        &self,
        config: &NatConfig,
        lease_secs: u32,
        spec: &MappingSpec,
        external_port: u16,
    ) -> Result<()> {
        let local_ip = self.mapping_internal_ip(config, spec);
        let args = format!(
            "<NewRemoteHost></NewRemoteHost>\
             <NewExternalPort>{external_port}</NewExternalPort>\
             <NewProtocol>{}</NewProtocol>\
             <NewInternalPort>{}</NewInternalPort>\
             <NewInternalClient>{}</NewInternalClient>\
             <NewEnabled>1</NewEnabled>\
             <NewPortMappingDescription>{}</NewPortMappingDescription>\
             <NewLeaseDuration>{lease_secs}</NewLeaseDuration>",
            spec.protocol.as_upnp_token(),
            spec.local_addr.port(),
            local_ip,
            xml_escape(&spec.name),
        );
        self.service
            .action(self.device.url(), "AddPortMapping", &args)
            .await
            .map(|_| ())
            .context("AddPortMapping failed")
    }

    async fn delete_mapping(&self, spec: &MappingSpec, external_port: u16) -> Result<()> {
        let args = format!(
            "<NewRemoteHost></NewRemoteHost>\
             <NewExternalPort>{external_port}</NewExternalPort>\
             <NewProtocol>{}</NewProtocol>",
            spec.protocol.as_upnp_token(),
        );
        self.service
            .action(self.device.url(), "DeletePortMapping", &args)
            .await
            .map(|_| ())
            .context("DeletePortMapping failed")
    }

    async fn external_ip(&self) -> Result<String> {
        let response = self
            .service
            .action(self.device.url(), "GetExternalIPAddress", "")
            .await
            .context("GetExternalIPAddress failed")?;
        response
            .get("NewExternalIPAddress")
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("UPnP gateway did not return an external IP"))
    }

    fn mapping_internal_ip(&self, config: &NatConfig, spec: &MappingSpec) -> Ipv4Addr {
        if !spec.local_addr.ip().is_unspecified() {
            return match spec.local_addr.ip() {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => Ipv4Addr::LOCALHOST,
            };
        }
        if let Some(bind_ip) = config.bind_ip.as_deref() {
            if let Ok(IpAddr::V4(ip)) = bind_ip.parse::<IpAddr>() {
                return ip;
            }
        }
        Ipv4Addr::LOCALHOST
    }

    fn selected_gateway(&self, external_ip: Option<String>) -> crate::types::SelectedGateway {
        crate::types::SelectedGateway {
            backend: "upnp".to_string(),
            control_url: self.device.url().to_string(),
            local_addr: None,
            gateway_addr: self.device.url().host().map(ToString::to_string),
            external_ip,
        }
    }
}

async fn discover_gateway(config: &NatConfig) -> Result<GatewayHandle> {
    if let Some(ip) = config.bind_ip.as_deref() {
        ip.parse::<IpAddr>()
            .with_context(|| format!("invalid nat.bind_ip {ip}"))?;
    }
    let timeout = Duration::from_secs(config.discovery_timeout_secs.max(1));

    for urn in [
        WAN_IP_CONNECTION_2,
        WAN_IP_CONNECTION_1,
        WAN_PPP_CONNECTION_1,
    ] {
        let search_target = SearchTarget::URN(urn.clone());
        let devices = rupnp::discover(&search_target, timeout, None).await?;
        let mut devices = Box::pin(devices);
        while let Some(device) = devices.try_next().await? {
            if let Some(requested_igd_ip) = config.igd_ip.as_deref() {
                let matches = device
                    .url()
                    .host()
                    .map(|host| host == requested_igd_ip)
                    .unwrap_or(false);
                if !matches {
                    continue;
                }
            }
            let service = device.find_service(&urn).cloned();
            if let Some(service) = service {
                return Ok(GatewayHandle { device, service });
            }
        }
    }

    if config.igd_ip.is_some() {
        Err(anyhow!("no matching IGD found for configured nat.igd_ip"))
    } else {
        Err(anyhow!("no UPnP IGD service discovered"))
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::xml_escape;

    #[test]
    fn xml_escape_covers_port_mapping_description_chars() {
        assert_eq!(
            xml_escape("udp & tcp <nat> 'map' \"desc\""),
            "udp &amp; tcp &lt;nat&gt; &apos;map&apos; &quot;desc&quot;"
        );
    }
}
