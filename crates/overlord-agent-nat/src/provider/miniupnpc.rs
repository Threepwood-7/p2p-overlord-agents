use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use miniupnpc::{DiscoveryOptions, Gateway, gateway_from_url};
use tokio::{sync::RwLock, task};
use tracing::debug;

use crate::{
    config::NatConfig,
    types::{MappedEndpoint, MappingSpec, NatStatus},
};

use super::{PortMappingProvider, UPNP_MINIUPNPC_BACKEND};

#[derive(Debug, Default)]
pub struct MiniupnpcPortMappingProvider;

#[derive(Debug)]
struct ReconcileOutcome {
    gateway: crate::types::SelectedGateway,
    observed_external_addresses: Vec<String>,
    mappings: Vec<MappedEndpoint>,
}

#[async_trait]
impl PortMappingProvider for MiniupnpcPortMappingProvider {
    fn name(&self) -> &'static str {
        UPNP_MINIUPNPC_BACKEND
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

        let backend_name = self.name().to_string();
        let config = config.clone();
        let status_config = config.clone();
        let mappings = mappings.to_vec();
        let outcome =
            task::spawn_blocking(move || reconcile_blocking(&backend_name, &config, &mappings))
                .await
                .context("miniupnpc reconcile task failed")??;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut guard = status.write().await;
        guard.enabled = true;
        guard.gateway_discovered = true;
        guard.backend = Some(self.name().to_string());
        guard.bind_ip = status_config.bind_ip.clone();
        guard.igd_ip = status_config.igd_ip.clone();
        guard.minissdpd_socket = status_config.minissdpd_socket.clone();
        guard.ssdp_local_port = status_config.ssdp_local_port;
        guard.external_ip_override = status_config.external_ip_override.clone();
        guard.gateway = Some(outcome.gateway);
        guard.observed_external_addresses = outcome.observed_external_addresses;
        guard.mappings = outcome.mappings;
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

        let config = config.clone();
        let mappings = mappings.to_vec();
        task::spawn_blocking(move || release_blocking(&config, &mappings))
            .await
            .context("miniupnpc release task failed")??;

        let mut guard = status.write().await;
        guard.mappings.clear();
        Ok(())
    }
}

fn reconcile_blocking(
    backend_name: &str,
    config: &NatConfig,
    mappings: &[MappingSpec],
) -> Result<ReconcileOutcome> {
    let gateway = discover_gateway(config)?;
    let local_ip = gateway_local_ip(config, &gateway)?;
    let external_ip_text = config
        .external_ip_override
        .clone()
        .or_else(|| gateway.fetch_external_ip().ok().flatten())
        .or_else(|| gateway.external_ip().map(ToString::to_string));

    let mut applied = Vec::new();
    let mut mapped = Vec::with_capacity(mappings.len());
    for spec in mappings {
        let external_port = spec
            .preferred_external_port
            .unwrap_or_else(|| spec.local_addr.port());
        if let Err(error) = gateway
            .add_port_mapping(
                external_port,
                spec.local_addr.port(),
                &mapping_internal_ip(config, spec, &local_ip).to_string(),
                &spec.name,
                spec.protocol.as_upnp_token(),
                config.lease_duration_secs,
            )
            .with_context(|| format!("failed to add {} mapping", spec.name))
        {
            for (protocol, port) in applied.into_iter().rev() {
                let _ = gateway.delete_port_mapping(port, protocol);
            }
            return Err(error);
        }
        applied.push((spec.protocol.as_upnp_token(), external_port));

        let external_ip = external_ip_text
            .clone()
            .unwrap_or_else(|| local_ip.to_string())
            .parse::<IpAddr>()
            .with_context(|| format!("invalid external ip for {}", spec.name))?;
        mapped.push(MappedEndpoint {
            name: spec.name.clone(),
            protocol: spec.protocol,
            local_addr: spec.local_addr,
            external_addr: SocketAddr::new(external_ip, external_port),
            lease_expires_in_secs: config.lease_duration_secs,
            backend: backend_name.to_string(),
        });
    }

    Ok(ReconcileOutcome {
        gateway: crate::types::SelectedGateway {
            backend: backend_name.to_string(),
            control_url: gateway.control_url().to_string(),
            local_ip: gateway.local_ip().map(ToString::to_string),
            gateway_ip: gateway.gateway_ip().map(ToString::to_string),
            external_ip: external_ip_text.clone(),
        },
        observed_external_addresses: external_ip_text.into_iter().collect(),
        mappings: mapped,
    })
}

fn release_blocking(config: &NatConfig, mappings: &[MappedEndpoint]) -> Result<()> {
    let gateway = discover_gateway(config)?;
    for mapping in mappings {
        let _ = gateway.delete_port_mapping(
            mapping.external_addr.port(),
            mapping.protocol.as_upnp_token(),
        );
    }
    Ok(())
}

fn discover_gateway(config: &NatConfig) -> Result<Gateway> {
    if let Some(igd_ip) = config.igd_ip.as_deref() {
        for root_description_url in candidate_root_description_urls(igd_ip) {
            if let Some(gateway) = gateway_from_url(&root_description_url)? {
                debug!(
                    "miniupnpc direct IGD probe succeeded for configured gateway {} via {}",
                    igd_ip, root_description_url
                );
                return Ok(gateway);
            }
        }
        return Err(anyhow!("no matching IGD found for configured nat.igd_ip"));
    }

    let (discovery, gateway) = miniupnpc::discover(&DiscoveryOptions {
        timeout: Duration::from_secs(config.discovery_timeout_secs.max(1)),
        multicast_interface: config.bind_ip.clone(),
        minissdpd_socket: config.minissdpd_socket.as_ref().map(PathBuf::from),
        local_port: config.ssdp_local_port,
        ..DiscoveryOptions::default()
    })?;

    debug!(
        "miniupnpc discovery found {} device(s); gateway discovered={}",
        discovery.devices.len(),
        discovery.gateway.is_some()
    );

    if let Some(gateway) = gateway {
        return Ok(gateway);
    }

    if let Some(bind_ip) = config.bind_ip.as_deref() {
        Err(anyhow!(
            "no UPnP IGD service discovered for nat.bind_ip {bind_ip}; on point-to-point VPNs you may need to set nat.igd_ip explicitly"
        ))
    } else {
        Err(anyhow!("no UPnP IGD service discovered"))
    }
}

fn candidate_root_description_urls(igd_ip: &str) -> [String; 4] {
    [
        format!("http://{igd_ip}:1900/gateDesc.xml"),
        format!("http://{igd_ip}:1900/rootDesc.xml"),
        format!("http://{igd_ip}:5000/rootDesc.xml"),
        format!("http://{igd_ip}:49152/rootDesc.xml"),
    ]
}

fn gateway_local_ip(config: &NatConfig, gateway: &Gateway) -> Result<Ipv4Addr> {
    if let Some(bind_ip) = config.bind_ip.as_deref()
        && let Ok(IpAddr::V4(ip)) = bind_ip.parse::<IpAddr>()
    {
        return Ok(ip);
    }
    if let Some(local_ip) = gateway.local_ip()
        && let Ok(IpAddr::V4(ip)) = local_ip.parse::<IpAddr>()
    {
        return Ok(ip);
    }
    Err(anyhow!(
        "miniupnpc did not provide a usable IPv4 LAN address"
    ))
}

fn mapping_internal_ip(
    config: &NatConfig,
    spec: &MappingSpec,
    gateway_local_ip: &Ipv4Addr,
) -> Ipv4Addr {
    if !spec.local_addr.ip().is_unspecified() {
        return match spec.local_addr.ip() {
            IpAddr::V4(ip) => ip,
            IpAddr::V6(_) => Ipv4Addr::LOCALHOST,
        };
    }
    if let Some(bind_ip) = config.bind_ip.as_deref()
        && let Ok(IpAddr::V4(ip)) = bind_ip.parse::<IpAddr>()
    {
        return ip;
    }
    *gateway_local_ip
}
