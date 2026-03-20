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
    types::{MappingSpec, NatStatus},
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
            return Ok(());
        }

        let mut slot = self.task.lock().await;
        if slot.is_some() {
            return Ok(());
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

        let mappings = self.status.read().await.mappings.clone();
        if mappings.is_empty() {
            return Ok(());
        }

        let selected_backend = self.status.read().await.backend.clone();
        if let Some(provider) = self.providers.iter().find(|provider| {
            selected_backend
                .as_deref()
                .map(|backend| backend == provider.name())
                .unwrap_or(false)
        }) {
            let _ = provider
                .release(&self.config, &mappings, Arc::clone(&self.status))
                .await;
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;

    use super::*;
    use crate::types::{MappedEndpoint, MappingExposure, MappingSpec, TransportProtocol};

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
            name: "upnp",
            failures_before_success: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
        });
        let status = Arc::new(RwLock::new(NatStatus::default()));
        let config = NatConfig {
            enabled: true,
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
        assert_eq!(status.backend.as_deref(), Some("upnp"));
        assert_eq!(status.bind_ip.as_deref(), Some("192.168.1.10"));
        assert_eq!(status.igd_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(status.mappings.len(), 1);
    }
}
