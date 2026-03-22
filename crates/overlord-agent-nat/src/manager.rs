use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{debug, warn};

use crate::{
    config::NatConfig,
    provider::PortMappingProvider,
    reachability::ReachabilityStrategy,
    types::{MappedEndpoint, MappingSpec, NatStatus},
};

pub struct NatManagerBuilder {
    config: NatConfig,
    mappings: Vec<MappingSpec>,
    providers: Vec<Arc<dyn PortMappingProvider>>,
    reachability: Arc<dyn ReachabilityStrategy>,
}

impl NatManagerBuilder {
    pub fn new(config: NatConfig) -> Self {
        Self {
            config,
            mappings: Vec::new(),
            providers: Vec::new(),
            reachability: Arc::new(crate::NoopReachabilityStrategy),
        }
    }

    pub fn with_mappings(mut self, mappings: Vec<MappingSpec>) -> Self {
        self.mappings = mappings;
        self
    }

    pub fn with_provider(mut self, provider: Arc<dyn PortMappingProvider>) -> Self {
        self.providers.push(provider);
        self
    }

    pub fn with_providers(mut self, providers: Vec<Arc<dyn PortMappingProvider>>) -> Self {
        self.providers.extend(providers);
        self
    }

    pub fn with_reachability(mut self, reachability: Arc<dyn ReachabilityStrategy>) -> Self {
        self.reachability = reachability;
        self
    }

    pub fn build(self) -> NatManager {
        NatManager {
            config: self.config,
            mappings: self.mappings,
            providers: self.providers,
            reachability: self.reachability,
            status: Arc::new(RwLock::new(NatStatus::default())),
            task: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct NatManager {
    config: NatConfig,
    mappings: Vec<MappingSpec>,
    providers: Vec<Arc<dyn PortMappingProvider>>,
    reachability: Arc<dyn ReachabilityStrategy>,
    status: Arc<RwLock<NatStatus>>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    shutdown: Arc<AtomicBool>,
}

impl NatManager {
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled || self.mappings.is_empty() {
            let mut status = self.status.write().await;
            status.enabled = self.config.enabled;
            status.bind_ip = self.config.bind_ip.clone();
            status.igd_ip = self.config.igd_ip.clone();
            status.minissdpd_socket = self.config.minissdpd_socket.clone();
            status.ssdp_local_port = self.config.ssdp_local_port;
            status.external_ip_override = self.config.external_ip_override.clone();
            return Ok(());
        }

        let mut slot = self.task.lock().await;
        if slot.is_some() {
            return Ok(());
        }

        {
            let mut status = self.status.write().await;
            status.enabled = self.config.enabled;
            status.bind_ip = self.config.bind_ip.clone();
            status.igd_ip = self.config.igd_ip.clone();
            status.minissdpd_socket = self.config.minissdpd_socket.clone();
            status.ssdp_local_port = self.config.ssdp_local_port;
            status.external_ip_override = self.config.external_ip_override.clone();
            status.last_error = None;
        }

        self.shutdown.store(false, Ordering::SeqCst);
        let config = self.config.clone();
        let mappings = self.mappings.clone();
        let providers = self.providers.clone();
        let status = Arc::clone(&self.status);
        let shutdown = Arc::clone(&self.shutdown);
        let reachability = Arc::clone(&self.reachability);
        *slot = Some(tokio::spawn(async move {
            run_manager_loop(config, mappings, providers, status, reachability, shutdown).await;
        }));
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);

        if let Some(task) = self.task.lock().await.take() {
            task.abort();
        }

        let status = self.status.read().await.clone();
        let mappings = if status.mappings.is_empty() {
            release_targets_from_specs(&self.mappings, self.config.lease_duration_secs)
        } else {
            status.mappings
        };
        if mappings.is_empty() {
            return Ok(());
        }

        for backend_name in release_backend_order(&status.backend, &self.config.backend_order) {
            if let Some(provider) = self
                .providers
                .iter()
                .find(|provider| provider.name() == backend_name.as_str())
            {
                let _ = provider
                    .release(&self.config, &mappings, Arc::clone(&self.status))
                    .await;
            }
        }
        Ok(())
    }

    pub async fn status(&self) -> NatStatus {
        self.status.read().await.clone()
    }
}

async fn run_manager_loop(
    config: NatConfig,
    mappings: Vec<MappingSpec>,
    providers: Vec<Arc<dyn PortMappingProvider>>,
    status: Arc<RwLock<NatStatus>>,
    reachability: Arc<dyn ReachabilityStrategy>,
    shutdown: Arc<AtomicBool>,
) {
    let refresh_period = Duration::from_secs(
        config
            .lease_duration_secs
            .saturating_sub(config.renew_margin_secs as u32)
            .max(30)
            .into(),
    );

    while !shutdown.load(Ordering::Relaxed) {
        let reconcile_result =
            reconcile_once(&config, &mappings, &providers, Arc::clone(&status)).await;
        match reconcile_result {
            Ok(()) => {
                let snapshot = status.read().await.clone();
                reachability.on_nat_status_changed(snapshot).await;
                tokio::time::sleep(refresh_period).await;
            }
            Err(error) => {
                warn!("nat mapping reconcile failed: {error}");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let mut guard = status.write().await;
                guard.enabled = config.enabled;
                guard.bind_ip = config.bind_ip.clone();
                guard.igd_ip = config.igd_ip.clone();
                guard.minissdpd_socket = config.minissdpd_socket.clone();
                guard.ssdp_local_port = config.ssdp_local_port;
                guard.external_ip_override = config.external_ip_override.clone();
                guard.last_error = Some(error.to_string());
                guard.last_refresh_unix_secs = Some(now);
                drop(guard);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

async fn reconcile_once(
    config: &NatConfig,
    mappings: &[MappingSpec],
    providers: &[Arc<dyn PortMappingProvider>],
    status: Arc<RwLock<NatStatus>>,
) -> Result<()> {
    let mut attempted = false;
    let mut last_error = None;
    for backend_name in &config.backend_order {
        let Some(provider) = providers
            .iter()
            .find(|provider| provider.name() == backend_name.as_str())
        else {
            continue;
        };
        attempted = true;
        match provider
            .reconcile(config, mappings, Arc::clone(&status))
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                debug!("nat backend {} failed: {error}", provider.name());
                last_error = Some(format!("{}: {error}", provider.name()));
            }
        }
    }

    if attempted {
        Err(anyhow!(last_error.unwrap_or_else(|| {
            "all configured NAT backends failed".to_string()
        })))
    } else {
        Err(anyhow!("no configured NAT backends are available"))
    }
}

fn release_targets_from_specs(
    mappings: &[MappingSpec],
    lease_duration_secs: u32,
) -> Vec<MappedEndpoint> {
    mappings
        .iter()
        .map(|spec| {
            let external_port = spec
                .preferred_external_port
                .unwrap_or_else(|| spec.local_addr.port());
            MappedEndpoint {
                name: spec.name.clone(),
                protocol: spec.protocol,
                local_addr: spec.local_addr,
                external_addr: std::net::SocketAddr::new(spec.local_addr.ip(), external_port),
                lease_expires_in_secs: lease_duration_secs,
                backend: String::new(),
            }
        })
        .collect()
}

fn release_backend_order(
    selected_backend: &Option<String>,
    backend_order: &[String],
) -> Vec<String> {
    let mut ordered = Vec::new();
    if let Some(selected_backend) = selected_backend.as_deref() {
        ordered.push(selected_backend.to_string());
    }
    for backend_name in backend_order {
        if !ordered.contains(backend_name) {
            ordered.push(backend_name.clone());
        }
    }
    ordered
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;

    use super::*;
    use crate::{
        UPNP_IGD_BACKEND, UPNP_MINIUPNPC_BACKEND, UPNP_RUPNP_BACKEND,
        built_in_upnp_port_mapping_providers,
        types::{MappedEndpoint, MappingExposure, MappingSpec, TransportProtocol},
    };

    struct FakeProvider {
        name: &'static str,
        failures_before_success: AtomicUsize,
        release_calls: AtomicUsize,
    }

    #[async_trait]
    impl PortMappingProvider for FakeProvider {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn reconcile(
            &self,
            config: &NatConfig,
            mappings: &[MappingSpec],
            status: Arc<RwLock<NatStatus>>,
        ) -> Result<()> {
            if self.failures_before_success.fetch_sub(1, Ordering::SeqCst) > 0 {
                return Err(anyhow!("boom"));
            }
            let mut guard = status.write().await;
            guard.enabled = config.enabled;
            guard.bind_ip = config.bind_ip.clone();
            guard.igd_ip = config.igd_ip.clone();
            guard.minissdpd_socket = config.minissdpd_socket.clone();
            guard.ssdp_local_port = config.ssdp_local_port;
            guard.external_ip_override = config.external_ip_override.clone();
            guard.gateway_discovered = config.igd_ip.is_some();
            guard.backend = Some(self.name.to_string());
            guard.mappings = mappings
                .iter()
                .map(|spec| MappedEndpoint {
                    name: spec.name.clone(),
                    protocol: spec.protocol,
                    local_addr: spec.local_addr,
                    external_addr: spec.local_addr,
                    lease_expires_in_secs: config.lease_duration_secs,
                    backend: self.name.to_string(),
                })
                .collect();
            Ok(())
        }

        async fn release(
            &self,
            _config: &NatConfig,
            _mappings: &[MappedEndpoint],
            _status: Arc<RwLock<NatStatus>>,
        ) -> Result<()> {
            self.release_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn sample_mapping() -> MappingSpec {
        MappingSpec {
            name: "kad".to_string(),
            local_addr: "0.0.0.0:41000".parse().unwrap(),
            protocol: TransportProtocol::Udp,
            exposure: MappingExposure::Required,
            preferred_external_port: None,
        }
    }

    #[tokio::test]
    async fn reconcile_once_uses_matching_backend() {
        let provider = Arc::new(FakeProvider {
            name: UPNP_RUPNP_BACKEND,
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let status = Arc::new(RwLock::new(NatStatus::default()));
        let config = NatConfig {
            enabled: true,
            backend_order: vec![UPNP_RUPNP_BACKEND.to_string()],
            bind_ip: Some("192.168.1.10".to_string()),
            igd_ip: Some("192.168.1.1".to_string()),
            ..NatConfig::default()
        };
        reconcile_once(
            &config,
            &[sample_mapping()],
            &[provider],
            Arc::clone(&status),
        )
        .await
        .unwrap();

        let status = status.read().await.clone();
        assert_eq!(status.backend.as_deref(), Some(UPNP_RUPNP_BACKEND));
        assert_eq!(status.bind_ip.as_deref(), Some("192.168.1.10"));
        assert_eq!(status.igd_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(status.mappings.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_once_reports_no_matching_backend_when_name_is_unknown() {
        let status = Arc::new(RwLock::new(NatStatus::default()));
        let config = NatConfig {
            enabled: true,
            backend_order: vec!["unknown_backend".to_string()],
            ..NatConfig::default()
        };

        let error = reconcile_once(
            &config,
            &[sample_mapping()],
            &[Arc::new(FakeProvider {
                name: UPNP_RUPNP_BACKEND,
                failures_before_success: AtomicUsize::new(0),
                release_calls: AtomicUsize::new(0),
            })],
            Arc::clone(&status),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "no configured NAT backends are available"
        );
    }

    #[tokio::test]
    async fn reconcile_once_surfaces_scaffolded_igd_backend_error() {
        let status = Arc::new(RwLock::new(NatStatus::default()));
        let config = NatConfig {
            enabled: true,
            backend_order: vec![UPNP_IGD_BACKEND.to_string()],
            ..NatConfig::default()
        };

        let error = reconcile_once(
            &config,
            &[sample_mapping()],
            &built_in_upnp_port_mapping_providers(),
            Arc::clone(&status),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("{UPNP_IGD_BACKEND}: {UPNP_IGD_BACKEND} backend not implemented yet")
        );
    }

    #[tokio::test]
    async fn reconcile_once_prefers_miniupnpc_when_configured() {
        let provider = Arc::new(FakeProvider {
            name: UPNP_MINIUPNPC_BACKEND,
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let status = Arc::new(RwLock::new(NatStatus::default()));
        let config = NatConfig {
            enabled: true,
            backend_order: vec![UPNP_MINIUPNPC_BACKEND.to_string()],
            ..NatConfig::default()
        };

        reconcile_once(
            &config,
            &[sample_mapping()],
            &[provider],
            Arc::clone(&status),
        )
        .await
        .unwrap();

        assert_eq!(
            status.read().await.backend.as_deref(),
            Some(UPNP_MINIUPNPC_BACKEND)
        );
    }

    #[tokio::test]
    async fn start_sets_desired_status_before_reconcile_finishes() {
        let manager = NatManagerBuilder::new(NatConfig {
            enabled: true,
            bind_ip: Some("192.168.1.10".to_string()),
            igd_ip: Some("192.168.1.1".to_string()),
            minissdpd_socket: Some("/var/run/minissdpd.sock".to_string()),
            ssdp_local_port: Some(1900),
            external_ip_override: Some("203.0.113.10".to_string()),
            ..NatConfig::default()
        })
        .with_mappings(vec![sample_mapping()])
        .build();

        manager.start().await.unwrap();
        let status = manager.status().await;

        assert!(status.enabled);
        assert_eq!(status.bind_ip.as_deref(), Some("192.168.1.10"));
        assert_eq!(status.igd_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(
            status.minissdpd_socket.as_deref(),
            Some("/var/run/minissdpd.sock")
        );
        assert_eq!(status.ssdp_local_port, Some(1900));
        assert_eq!(status.external_ip_override.as_deref(), Some("203.0.113.10"));

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stop_releases_configured_ports_when_reconcile_never_populated_status_mappings() {
        let provider = Arc::new(FakeProvider {
            name: UPNP_RUPNP_BACKEND,
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let manager = NatManagerBuilder::new(NatConfig {
            enabled: true,
            backend_order: vec![UPNP_RUPNP_BACKEND.to_string()],
            ..NatConfig::default()
        })
        .with_mappings(vec![sample_mapping()])
        .with_provider(provider.clone())
        .build();

        manager.stop().await.unwrap();

        assert_eq!(provider.release_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stop_releases_selected_backend_before_fallback_backends() {
        let selected_provider = Arc::new(FakeProvider {
            name: UPNP_RUPNP_BACKEND,
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let fallback_provider = Arc::new(FakeProvider {
            name: UPNP_MINIUPNPC_BACKEND,
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let manager = NatManagerBuilder::new(NatConfig {
            enabled: true,
            backend_order: vec![
                UPNP_MINIUPNPC_BACKEND.to_string(),
                UPNP_RUPNP_BACKEND.to_string(),
            ],
            ..NatConfig::default()
        })
        .with_mappings(vec![sample_mapping()])
        .with_provider(selected_provider.clone())
        .with_provider(fallback_provider.clone())
        .build();

        {
            let mut status = manager.status.write().await;
            status.backend = Some(UPNP_RUPNP_BACKEND.to_string());
            status.mappings = vec![MappedEndpoint {
                name: "kad".to_string(),
                protocol: TransportProtocol::Udp,
                local_addr: "0.0.0.0:41000".parse().unwrap(),
                external_addr: "203.0.113.10:41000".parse().unwrap(),
                lease_expires_in_secs: 300,
                backend: UPNP_RUPNP_BACKEND.to_string(),
            }];
        }

        manager.stop().await.unwrap();

        assert_eq!(selected_provider.release_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_provider.release_calls.load(Ordering::SeqCst), 1);
    }
}
