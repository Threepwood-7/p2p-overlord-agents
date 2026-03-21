use std::{
    collections::HashMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use md4::{Digest, Md4};
use overlord_agent_nat::{
    AgentControlConfig, AgentEd2kConfig, AgentInterface, AgentKadConfig, AgentNatConfig,
    AgentNatP2pConfig, AgentNetworkReport, AgentNetworkingConfig, AgentP2pConfig,
    InterfaceBindingSelection, InterfaceSelectionState, MappingExposure, MappingSpec,
    NatCapableAgent, NatManager, NatManagerBuilder, ResolvedInterfaceBindingReport,
    TransportProtocol, build_interface_binding_report, built_in_upnp_port_mapping_providers,
    default_upnp_backend_order, detect_interfaces, recommend_interface, resolve_bind_ip,
};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use overlord_agent_common::{
    AgentInterfacesView, ConfigUpdate, ContentType, CoordinatorClient, FileRecord, HashType,
    IndexerServer, IndexerService, IndexerStats, PopularHash, Protocol, RegisterRequest,
    ResultBatch, RunningIndexerServer, SearchEvent, SearchEventStatus, SearchJob, SearchKind,
    SnoopEntry, Source, TagEntry,
};
use overlord_kad_dht::{
    DhtConfig, DhtNode, SearchResult, SourceResult,
    bootstrap::{BootstrapContact, encode_nodes_dat},
};
use overlord_kad_proto::{
    Ed2kHash, KadPacket, NodeId, Tag, TagName, TagValue, constants::K, packet::ContactEntry,
    tag_name,
};
use overlord_kad_routing::Contact;

use crate::config::EmuleAgentConfig;
use crate::snoop_queue::SnoopQueue;

const ACTIVE_BATCH_SIZE: usize = 25;
const PASSIVE_BATCH_SIZE: usize = 50;
const BOOTSTRAP_RETRY_SECS: u64 = 30;
const SNOOP_FLUSH_SECS: u64 = 30;
const PASSIVE_CRAWL_SECS: u64 = 45;

#[derive(Clone)]
struct AgentStatePaths {
    node_id_path: PathBuf,
    udp_key_path: PathBuf,
    nodes_dat_path: PathBuf,
    networking_config_path: PathBuf,
}

#[derive(Clone)]
struct AgentNetworkRuntime {
    dht: DhtNode,
    nat: Arc<NatManager>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    shutdown: Arc<AtomicBool>,
    passive_result_count: Arc<std::sync::atomic::AtomicU64>,
}

struct ControlServerRuntime {
    bind_addr: SocketAddr,
    server: RunningIndexerServer,
}

#[derive(Clone)]
struct ActiveSearchHandle {
    cancel: CancellationToken,
}

pub struct OverlordAgentEmule {
    config: Arc<RwLock<EmuleAgentConfig>>,
    coordinator: CoordinatorClient,
    indexer_id: Uuid,
    started_at: Instant,
    state_paths: AgentStatePaths,
    snoop_queue: Arc<Mutex<SnoopQueue>>,
    runtime: Arc<Mutex<Option<AgentNetworkRuntime>>>,
    control_server: Arc<Mutex<Option<ControlServerRuntime>>>,
    control_selection_state: Arc<RwLock<ResolvedInterfaceBindingReport>>,
    p2p_selection_state: Arc<RwLock<ResolvedInterfaceBindingReport>>,
    active_searches: Arc<Mutex<HashMap<Uuid, ActiveSearchHandle>>>,
    restart_requested: Arc<AtomicBool>,
    restart_notify: Arc<Notify>,
    started: AtomicBool,
}

pub enum AgentExit {
    Stopped,
    RestartRequested,
}

impl OverlordAgentEmule {
    pub async fn new(mut config: EmuleAgentConfig) -> Result<Self> {
        load_persisted_networking_config(&mut config)?;
        let indexer_id = load_or_create_indexer_id(&config.agent.indexer_id_path)?;
        let coordinator = CoordinatorClient::new(&config.coordinator.url)?;
        let state_paths = AgentStatePaths::from_config(&config);
        ensure_parent_dir(&state_paths.node_id_path)?;
        ensure_parent_dir(&state_paths.udp_key_path)?;
        ensure_parent_dir(&state_paths.nodes_dat_path)?;
        ensure_parent_dir(&state_paths.networking_config_path)?;
        let interfaces = detect_interfaces().unwrap_or_default();
        let control_selection_state =
            Self::resolve_control_selection_state(&config, &interfaces, None, false, false);
        let p2p_selection_state =
            Self::resolve_p2p_selection_state(&config, &interfaces, None, false, false);
        let snoop_queue_config = config.p2p.snoop_queue.clone();

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            coordinator,
            indexer_id,
            started_at: Instant::now(),
            state_paths,
            snoop_queue: Arc::new(Mutex::new(SnoopQueue::new(snoop_queue_config))),
            runtime: Arc::new(Mutex::new(None)),
            control_server: Arc::new(Mutex::new(None)),
            control_selection_state: Arc::new(RwLock::new(control_selection_state)),
            p2p_selection_state: Arc::new(RwLock::new(p2p_selection_state)),
            active_searches: Arc::new(Mutex::new(HashMap::new())),
            restart_requested: Arc::new(AtomicBool::new(false)),
            restart_notify: Arc::new(Notify::new()),
            started: AtomicBool::new(false),
        })
    }

    pub async fn register_with_coordinator(&self) -> Result<()> {
        let config = self.config.read().await.clone();
        let url = self.current_registration_url(&config).await?;
        let registration = self
            .coordinator
            .register(&RegisterRequest {
                indexer_id: self.indexer_id,
                protocol: Protocol::Kad2,
                url,
                hostname: config.agent.hostname,
                version: config.agent.version,
            })
            .await?;
        info!(
            "registered agent {} at {}",
            registration.indexer_id, registration.url
        );
        Ok(())
    }

    pub async fn serve(self: Arc<Self>) -> Result<AgentExit> {
        let config = self.config.read().await.clone();
        let bind_addr = Self::startup_control_bind_addr(&config)?;
        self.start_control_server_with_retry(bind_addr).await?;
        self.register_with_coordinator().await?;

        if self.sync_networking_config_from_coordinator().await? {
            self.stop().await?;
            self.stop_control_server().await?;
            return Ok(AgentExit::RestartRequested);
        }

        tokio::select! {
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c.context("failed while waiting for ctrl-c")?;
            }
            _ = self.restart_notify.notified() => {}
        }
        self.stop().await?;
        self.stop_control_server().await?;
        Ok(if self.restart_requested.load(Ordering::SeqCst) {
            AgentExit::RestartRequested
        } else {
            AgentExit::Stopped
        })
    }

    fn control_selection(config: &EmuleAgentConfig) -> InterfaceBindingSelection {
        InterfaceBindingSelection {
            bind_iface: config.control.bind_iface.clone(),
            bind_ip: config.control.bind_ip.clone(),
            selection_confirmed: config.control.selection_confirmed,
        }
    }

    fn p2p_selection(config: &EmuleAgentConfig) -> InterfaceBindingSelection {
        InterfaceBindingSelection {
            bind_iface: config.p2p.bind_iface.clone(),
            bind_ip: config.p2p.bind_ip.clone(),
            selection_confirmed: config.p2p.selection_confirmed,
        }
    }

    fn control_config(config: &EmuleAgentConfig) -> AgentControlConfig {
        AgentControlConfig {
            bind_iface: config.control.bind_iface.clone(),
            bind_ip: config.control.bind_ip.clone(),
            selection_confirmed: config.control.selection_confirmed,
            listen_port: config.control.listen_port,
        }
    }

    fn p2p_config(config: &EmuleAgentConfig) -> AgentP2pConfig {
        AgentP2pConfig {
            bind_iface: config.p2p.bind_iface.clone(),
            bind_ip: config.p2p.bind_ip.clone(),
            selection_confirmed: config.p2p.selection_confirmed,
            kad: AgentKadConfig {
                listen_port: config.p2p.kad.listen_port,
            },
            ed2k: AgentEd2kConfig {
                listen_port: config.p2p.ed2k.listen_port,
            },
        }
    }

    fn desired_nat_config(config: &EmuleAgentConfig) -> AgentNatConfig {
        AgentNatConfig {
            p2p: AgentNatP2pConfig {
                enabled: config.nat.p2p.enabled,
                backend_order: if config.nat.p2p.backend_order.is_empty() {
                    default_upnp_backend_order()
                } else {
                    config.nat.p2p.backend_order.clone()
                },
                igd_ip: config.nat.p2p.igd_ip.clone(),
                minissdpd_socket: config.nat.p2p.minissdpd_socket.clone(),
                ssdp_local_port: config.nat.p2p.ssdp_local_port,
                discovery_timeout_secs: config.nat.p2p.discovery_timeout_secs,
                lease_duration_secs: config.nat.p2p.lease_duration_secs,
                renew_margin_secs: config.nat.p2p.renew_margin_secs,
                external_ip_override: config.nat.p2p.external_ip_override.clone(),
            },
        }
    }

    fn networking_config(config: &EmuleAgentConfig) -> AgentNetworkingConfig {
        AgentNetworkingConfig {
            control: Self::control_config(config),
            p2p: Self::p2p_config(config),
            nat: Self::desired_nat_config(config),
        }
    }

    fn bootstrap_control_bind_addr(config: &EmuleAgentConfig) -> Result<SocketAddr> {
        resolved_socket_addr(config.control.listen_port, None)
    }

    fn startup_control_bind_addr(config: &EmuleAgentConfig) -> Result<SocketAddr> {
        let interfaces = detect_interfaces().unwrap_or_default();
        let selection = Self::control_selection(config);
        if selection.selection_confirmed
            && let Some(bind_ip) = resolve_bind_ip(
                &interfaces,
                selection.bind_iface.as_deref(),
                selection.bind_ip.as_deref(),
            )
        {
            return Self::selected_control_bind_addr(config, Some(&bind_ip));
        }

        Self::bootstrap_control_bind_addr(config)
    }

    fn selected_control_bind_addr(
        config: &EmuleAgentConfig,
        bind_ip: Option<&str>,
    ) -> Result<SocketAddr> {
        resolved_socket_addr(config.control.listen_port, bind_ip)
    }

    fn resolve_binding_state(
        interfaces: &[AgentInterface],
        selection: InterfaceBindingSelection,
        runtime_error: Option<String>,
        ready: bool,
        applied: bool,
    ) -> ResolvedInterfaceBindingReport {
        let recommended_interface_name = recommend_interface(interfaces);
        let resolved_bind_ip = resolve_bind_ip(
            interfaces,
            selection.bind_iface.as_deref(),
            selection.bind_ip.as_deref(),
        );

        let (state, last_error) = if let Some(error) = runtime_error {
            (InterfaceSelectionState::Error, Some(error))
        } else if applied {
            (InterfaceSelectionState::Applied, None)
        } else if !selection.selection_confirmed {
            (InterfaceSelectionState::Pending, None)
        } else if resolved_bind_ip.is_some() {
            (InterfaceSelectionState::Confirmed, None)
        } else {
            (
                InterfaceSelectionState::Error,
                Some(
                    "selected interface does not currently resolve to an IPv4 bind address"
                        .to_string(),
                ),
            )
        };

        ResolvedInterfaceBindingReport {
            bind_iface: selection.bind_iface,
            bind_ip: resolved_bind_ip,
            recommended_interface_name,
            selection_confirmed: selection.selection_confirmed,
            ready,
            state,
            last_error,
        }
    }

    fn resolve_control_selection_state(
        config: &EmuleAgentConfig,
        interfaces: &[AgentInterface],
        runtime_error: Option<String>,
        ready: bool,
        applied: bool,
    ) -> ResolvedInterfaceBindingReport {
        Self::resolve_binding_state(
            interfaces,
            Self::control_selection(config),
            runtime_error,
            ready,
            applied,
        )
    }

    fn resolve_p2p_selection_state(
        config: &EmuleAgentConfig,
        interfaces: &[AgentInterface],
        runtime_error: Option<String>,
        ready: bool,
        applied: bool,
    ) -> ResolvedInterfaceBindingReport {
        Self::resolve_binding_state(
            interfaces,
            Self::p2p_selection(config),
            runtime_error,
            ready,
            applied,
        )
    }

    async fn current_control_bind_addr(&self) -> Option<SocketAddr> {
        self.control_server
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.bind_addr)
    }

    async fn current_registration_url(&self, config: &EmuleAgentConfig) -> Result<String> {
        let bind_addr = self
            .current_control_bind_addr()
            .await
            .unwrap_or(Self::bootstrap_control_bind_addr(config)?);
        let host = if bind_addr.ip().is_unspecified() {
            config.agent.hostname.clone()
        } else {
            bind_addr.ip().to_string()
        };
        Ok(format!("http://{}:{}", host, bind_addr.port()))
    }

    async fn start_control_server(self: &Arc<Self>, bind_addr: SocketAddr) -> Result<()> {
        let server = IndexerServer::new(Arc::clone(self))
            .spawn(bind_addr)
            .await?;
        let local_addr = server.local_addr();
        *self.control_server.lock().await = Some(ControlServerRuntime {
            bind_addr: local_addr,
            server,
        });
        Ok(())
    }

    async fn start_control_server_with_retry(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
    ) -> Result<()> {
        let mut last_error = None;
        for _attempt in 0..20 {
            match self.start_control_server(bind_addr).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("control server failed to start")))
    }

    async fn stop_control_server(&self) -> Result<()> {
        if let Some(runtime) = self.control_server.lock().await.take() {
            runtime.server.shutdown().await?;
        }
        Ok(())
    }

    fn request_restart(&self) {
        self.restart_requested.store(true, Ordering::SeqCst);
        self.restart_notify.notify_waiters();
    }

    fn nat_mappings_from_config(
        config: &EmuleAgentConfig,
        bind_ip: Option<&str>,
    ) -> Result<Vec<MappingSpec>> {
        let kad_addr = resolved_socket_addr(config.p2p.kad.listen_port, bind_ip)
            .context("invalid p2p.kad.listen_port for NAT mapping")?;
        let ed2k_addr = resolved_socket_addr(config.p2p.ed2k.listen_port, bind_ip)
            .context("invalid p2p.ed2k.listen_port for NAT mapping")?;

        Ok(vec![
            MappingSpec {
                name: "kad".to_string(),
                local_addr: kad_addr,
                protocol: TransportProtocol::Udp,
                exposure: MappingExposure::Required,
                preferred_external_port: None,
            },
            MappingSpec {
                name: "ed2k".to_string(),
                local_addr: ed2k_addr,
                protocol: TransportProtocol::Tcp,
                exposure: MappingExposure::Preferred,
                preferred_external_port: None,
            },
        ])
    }

    async fn sync_networking_config_from_coordinator(&self) -> Result<bool> {
        let view = self
            .coordinator
            .agent_interfaces_view(self.indexer_id)
            .await?;
        self.sync_networking_config_from_view(&view).await
    }

    async fn sync_networking_config_from_view(&self, view: &AgentInterfacesView) -> Result<bool> {
        let current = self
            .config
            .try_read()
            .map(|config| Self::networking_config(&config))
            .unwrap_or_else(|_| empty_networking_config());

        if current == view.config {
            return Ok(false);
        }

        {
            let mut guard = self.config.write().await;
            apply_networking_config(&mut guard, &view.config);
            persist_networking_config(&self.state_paths, &view.config)?;
        }

        Ok(true)
    }

    async fn interface_report(&self) -> AgentNetworkReport {
        let config = self.config.read().await.clone();
        let interfaces = detect_interfaces().unwrap_or_default();
        let runtime_active = self.runtime.lock().await.is_some();
        let control_bind_addr = self.current_control_bind_addr().await;

        let control_error = self
            .control_selection_state
            .try_read()
            .ok()
            .and_then(|state| {
                matches!(state.state, InterfaceSelectionState::Error)
                    .then(|| state.last_error.clone())
                    .flatten()
            });
        let p2p_error = self.p2p_selection_state.try_read().ok().and_then(|state| {
            matches!(state.state, InterfaceSelectionState::Error)
                .then(|| state.last_error.clone())
                .flatten()
        });

        let control_applied = Self::control_selection(&config).selection_confirmed
            && control_bind_addr.is_some_and(|bind_addr| {
                resolve_bind_ip(
                    &interfaces,
                    config.control.bind_iface.as_deref(),
                    config.control.bind_ip.as_deref(),
                )
                .is_some_and(|resolved_ip| bind_addr.ip().to_string() == resolved_ip)
            });
        let control = Self::resolve_control_selection_state(
            &config,
            &interfaces,
            control_error,
            control_bind_addr.is_some(),
            control_applied,
        );
        let p2p = Self::resolve_p2p_selection_state(
            &config,
            &interfaces,
            p2p_error,
            runtime_active,
            runtime_active,
        );

        AgentNetworkReport {
            interfaces,
            control: build_interface_binding_report(&control),
            p2p: build_interface_binding_report(&p2p),
        }
    }

    async fn reconcile_runtime(&self) -> Result<()> {
        let config = self.config.read().await.clone();
        let interfaces = detect_interfaces().unwrap_or_default();
        let binding = Self::resolve_p2p_selection_state(&config, &interfaces, None, false, false);
        {
            let mut selection_state = self.p2p_selection_state.write().await;
            *selection_state = binding.clone();
        }

        if !binding.selection_confirmed {
            self.stop_runtime().await?;
            return Ok(());
        }

        let Some(bind_ip) = binding.bind_ip.clone() else {
            self.stop_runtime().await?;
            return Ok(());
        };

        self.stop_runtime().await?;
        match self.build_runtime(&config, &bind_ip).await {
            Ok(runtime) => {
                let dht_task = runtime.dht.start();
                runtime.tasks.lock().await.push(dht_task);
                runtime.nat.start().await?;
                self.spawn_background_tasks(&runtime, &config).await;
                *self.runtime.lock().await = Some(runtime);
                let mut selection_state = self.p2p_selection_state.write().await;
                selection_state.state = InterfaceSelectionState::Applied;
                selection_state.ready = true;
                selection_state.last_error = None;
            }
            Err(error) => {
                let mut selection_state = self.p2p_selection_state.write().await;
                selection_state.state = InterfaceSelectionState::Error;
                selection_state.last_error = Some(error.to_string());
            }
        }

        Ok(())
    }

    async fn stop_runtime(&self) -> Result<()> {
        self.cancel_active_searches().await;
        if let Some(runtime) = self.runtime.lock().await.take() {
            runtime.shutdown.store(true, Ordering::SeqCst);
            let tasks = {
                let mut tasks = runtime.tasks.lock().await;
                std::mem::take(&mut *tasks)
            };
            for task in tasks {
                task.abort();
            }
            runtime.nat.stop().await?;
        }
        Ok(())
    }

    async fn cancel_active_searches(&self) {
        let handles = {
            let mut active = self.active_searches.lock().await;
            active.drain().map(|(_, handle)| handle).collect::<Vec<_>>()
        };
        for handle in handles {
            handle.cancel.cancel();
        }
    }

    async fn build_runtime(
        &self,
        config: &EmuleAgentConfig,
        bind_ip: &str,
    ) -> Result<AgentNetworkRuntime> {
        let node_id = load_or_create_node_id(&self.state_paths.node_id_path)?;
        let udp_key = load_or_create_udp_key(&self.state_paths.udp_key_path)?;
        let bind_addr = resolved_socket_addr(config.p2p.kad.listen_port, Some(bind_ip))
            .context("invalid p2p.kad.listen_port")?;
        let nodes_dat = read_optional_bytes(&self.state_paths.nodes_dat_path)?;
        let nodes_text = (!config.p2p.kad.bootstrap_nodes.is_empty())
            .then(|| config.p2p.kad.bootstrap_nodes.join("\n"));

        let dht = DhtNode::new(DhtConfig {
            bind_addr,
            node_id,
            max_routing_table_size: 12_000,
            max_concurrent_searches: 5,
            search_timeout: Duration::from_secs(config.p2p.kad.search_timeout_secs),
            store_timeout: Duration::from_secs(config.p2p.kad.store_timeout_secs),
            republish_interval: Duration::from_secs(config.p2p.kad.republish_interval_secs),
            max_outbound_pps: config.p2p.kad.max_outbound_pps,
            search_phase2_fanout: config.p2p.kad.search_phase2_fanout,
            keyword_result_cap: config.p2p.kad.keyword_result_cap,
            source_result_cap: config.p2p.kad.source_result_cap,
            notes_result_cap: config.p2p.kad.notes_result_cap,
            obfuscation_enabled: config.p2p.kad.obfuscation_enabled,
            udp_key,
            nodes_dat,
            nodes_text,
        })
        .await?;

        let nat_config = overlord_agent_nat::NatConfig {
            enabled: config.nat.p2p.enabled,
            backend_order: if config.nat.p2p.backend_order.is_empty() {
                default_upnp_backend_order()
            } else {
                config.nat.p2p.backend_order.clone()
            },
            bind_ip: Some(bind_ip.to_string()),
            igd_ip: config.nat.p2p.igd_ip.clone(),
            minissdpd_socket: config.nat.p2p.minissdpd_socket.clone(),
            ssdp_local_port: config.nat.p2p.ssdp_local_port,
            discovery_timeout_secs: config.nat.p2p.discovery_timeout_secs,
            lease_duration_secs: config.nat.p2p.lease_duration_secs,
            renew_margin_secs: config.nat.p2p.renew_margin_secs,
            external_ip_override: config.nat.p2p.external_ip_override.clone(),
        };

        let nat = Arc::new(
            NatManagerBuilder::new(nat_config)
                .with_mappings(Self::nat_mappings_from_config(config, Some(bind_ip))?)
                .with_providers(built_in_upnp_port_mapping_providers())
                .build(),
        );

        Ok(AgentNetworkRuntime {
            dht,
            nat,
            tasks: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            passive_result_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }
}

#[derive(Default)]
struct SearchRunStats {
    result_count: u32,
    batch_count: u32,
}

async fn emit_search_event(
    callback_client: &CoordinatorClient,
    job_id: Uuid,
    indexer_id: Uuid,
    status: SearchEventStatus,
    stats: &SearchRunStats,
    error: Option<String>,
) -> Result<()> {
    callback_client
        .post_search_event(&SearchEvent {
            job_id,
            indexer_id,
            status,
            result_count: Some(stats.result_count),
            batch_count: Some(stats.batch_count),
            error,
        })
        .await
}

async fn post_search_batch(
    callback_client: &CoordinatorClient,
    job_id: Uuid,
    indexer_id: Uuid,
    files: Vec<FileRecord>,
    stats: &mut SearchRunStats,
) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }
    stats.result_count += files.len() as u32;
    stats.batch_count += 1;
    callback_client
        .post_results(&ResultBatch {
            job_id: Some(job_id),
            indexer_id,
            protocol: Protocol::Kad2,
            files,
        })
        .await?;
    emit_search_event(
        callback_client,
        job_id,
        indexer_id,
        SearchEventStatus::BatchReceived,
        stats,
        None,
    )
    .await
}

fn search_query(job: &SearchJob) -> Result<&str> {
    job.query
        .as_deref()
        .filter(|query| !query.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("search job is missing query text"))
}

fn search_file_hash(job: &SearchJob) -> Result<Ed2kHash> {
    let Some(HashType::Ed2k(value)) = job.file_hash.as_ref() else {
        anyhow::bail!("search job is missing ed2k file hash");
    };
    Ed2kHash::from_str(value).with_context(|| format!("invalid Ed2k hash {value}"))
}

fn search_file_size(job: &SearchJob) -> Result<u64> {
    job.file_size
        .filter(|size| *size > 0)
        .ok_or_else(|| anyhow::anyhow!("search job is missing file size"))
}

fn map_source_result(result: &SourceResult, file_size: u64) -> FileRecord {
    FileRecord {
        hashes: vec![HashType::Ed2k(result.file_hash.to_string())],
        names: Vec::new(),
        size: Some(file_size),
        content_type: None,
        tags: Vec::new(),
        sources: vec![Source {
            protocol: Protocol::Kad2,
            address: format!("{}:{}", result.ip, result.tcp_port),
            extra: serde_json::json!({
                "udp_port": result.udp_port,
                "search_mode": "source"
            }),
        }],
    }
}

async fn do_active_keyword_search(
    dht: &DhtNode,
    indexer_id: Uuid,
    job: &SearchJob,
    config: Arc<RwLock<EmuleAgentConfig>>,
    cancel: CancellationToken,
) -> Result<SearchRunStats> {
    let target = keyword_target(search_query(job)?);
    let mut stream = dht.search_keywords_with_cancel(target, cancel.clone());
    let callback_client = CoordinatorClient::new(&job.callback_url)?;
    let mut files = Vec::new();
    let mut seen = 0usize;
    let mut stats = SearchRunStats::default();

    while let Some(result) = stream.next().await {
        seen += 1;
        files.push(map_search_result_for(dht, &result)?);
        if files.len() >= ACTIVE_BATCH_SIZE {
            post_search_batch(
                &callback_client,
                job.job_id,
                indexer_id,
                std::mem::take(&mut files),
                &mut stats,
            )
            .await?;
        }
    }

    post_search_batch(&callback_client, job.job_id, indexer_id, files, &mut stats).await?;

    if seen == 0 && !cancel.is_cancelled() && config.read().await.p2p.kad.enable_mock_results {
        post_search_batch(
            &callback_client,
            job.job_id,
            indexer_id,
            vec![mock_file_record(
                search_query(job)?,
                dht.bind_addr()?.to_string(),
            )],
            &mut stats,
        )
        .await?;
    }

    Ok(stats)
}

async fn do_active_source_search(
    dht: &DhtNode,
    indexer_id: Uuid,
    job: &SearchJob,
    cancel: CancellationToken,
) -> Result<SearchRunStats> {
    let file_hash = search_file_hash(job)?;
    let file_size = search_file_size(job)?;
    let callback_client = CoordinatorClient::new(&job.callback_url)?;
    let mut stream = dht.search_sources_with_cancel(file_hash, file_size, cancel);
    let mut files = Vec::new();
    let mut stats = SearchRunStats::default();

    while let Some(result) = stream.next().await {
        files.push(map_source_result(&result, file_size));
        if files.len() >= ACTIVE_BATCH_SIZE {
            post_search_batch(
                &callback_client,
                job.job_id,
                indexer_id,
                std::mem::take(&mut files),
                &mut stats,
            )
            .await?;
        }
    }

    post_search_batch(&callback_client, job.job_id, indexer_id, files, &mut stats).await?;
    Ok(stats)
}

async fn seed_popular_impl(dht: &DhtNode, hashes: Vec<PopularHash>) -> Result<()> {
    if !dht.is_bootstrapped() {
        anyhow::bail!("kad node is not bootstrapped yet");
    }

    let bind_addr = dht.bind_addr()?;
    for hash in hashes {
        let HashType::Ed2k(raw_hash) = hash.hash;
        let file_hash = Ed2kHash::from_str(&raw_hash)
            .with_context(|| format!("invalid Ed2k hash {raw_hash}"))?;
        let keyword_hash = keyword_target(&hash.canonical_name);
        let keyword_tags = vec![
            Tag::filename(hash.canonical_name.clone()),
            Tag::filesize(hash.size),
            Tag::sources(hash.source_count),
        ];
        let _ = dht
            .publish_keyword(keyword_hash, file_hash, keyword_tags)
            .await;
        let source_tags = vec![
            Tag::new_short(tag_name::SOURCEPORT, TagValue::U16(bind_addr.port())),
            Tag::new_short(tag_name::SOURCEUPORT, TagValue::U16(bind_addr.port())),
            Tag::new_short(tag_name::SOURCETYPE, TagValue::U8(1)),
        ];
        let _ = dht.publish_source(file_hash, source_tags).await;
    }

    Ok(())
}

async fn restore_snoop_queue(
    coordinator: &CoordinatorClient,
    indexer_id: Uuid,
    snoop_queue: &Arc<Mutex<SnoopQueue>>,
) {
    match coordinator.restore_snoop(indexer_id).await {
        Ok(entries) => {
            let mut queue = snoop_queue.lock().await;
            queue.merge_snapshot(entries);
        }
        Err(error) => warn!("failed to restore snoop queue: {error}"),
    }
}

async fn flush_snoop_queue(
    coordinator: &CoordinatorClient,
    indexer_id: Uuid,
    snoop_queue: &Arc<Mutex<SnoopQueue>>,
) -> Result<()> {
    let entries = { snoop_queue.lock().await.snapshot() };
    coordinator.flush_snoop(indexer_id, &entries).await
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn read_optional_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(fs::read(path).with_context(|| {
        format!("failed to read {}", path.display())
    })?))
}

fn resolved_socket_addr(listen_port: u16, bind_ip: Option<&str>) -> Result<SocketAddr> {
    let ip = match bind_ip {
        Some(bind_ip) => bind_ip
            .parse::<IpAddr>()
            .with_context(|| format!("invalid bind ip {bind_ip}"))?,
        None => IpAddr::from([0, 0, 0, 0]),
    };
    Ok(SocketAddr::new(ip, listen_port))
}

fn load_or_create_indexer_id(path: &str) -> Result<Uuid> {
    let path = Path::new(path);
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return Uuid::parse_str(contents.trim())
            .with_context(|| format!("invalid uuid in {}", path.display()));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let generated = Uuid::new_v4();
    fs::write(path, generated.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(generated)
}

fn load_or_create_node_id(path: &Path) -> Result<NodeId> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return NodeId::from_str(contents.trim())
            .with_context(|| format!("invalid node id in {}", path.display()));
    }
    let node_id = NodeId::from_bytes(rand::random());
    fs::write(path, node_id.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(node_id)
}

fn load_or_create_udp_key(path: &Path) -> Result<u32> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        return contents
            .trim()
            .parse::<u32>()
            .with_context(|| format!("invalid udp key in {}", path.display()));
    }
    let udp_key: u32 = rand::random();
    fs::write(path, udp_key.to_string())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(udp_key)
}

fn significant_keyword_words(query: &str) -> Vec<String> {
    let words: Vec<String> = query
        .split(|char: char| !char.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .filter(|word| word.len() >= 3)
        .collect();
    if words.is_empty() {
        vec![query.to_lowercase()]
    } else {
        words
    }
}

fn keyword_target(query: &str) -> NodeId {
    let first_word = significant_keyword_words(query)
        .into_iter()
        .next()
        .unwrap_or_else(|| query.to_lowercase());
    let mut hasher = Md4::new();
    hasher.update(first_word.as_bytes());
    let digest: [u8; 16] = hasher.finalize().into();
    let mut wire = [0u8; 16];
    for chunk in 0..4 {
        let base = chunk * 4;
        wire[base] = digest[base + 3];
        wire[base + 1] = digest[base + 2];
        wire[base + 2] = digest[base + 1];
        wire[base + 3] = digest[base];
    }
    NodeId::from_bytes(wire)
}

fn guess_content_type(name: Option<&String>) -> Option<ContentType> {
    let Some(name) = name else {
        return Some(ContentType::Unknown);
    };
    let lower = name.to_lowercase();
    let value = if [".mkv", ".mp4", ".avi", ".mov"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Video
    } else if [".mp3", ".flac", ".wav", ".ogg"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Audio
    } else if [".pdf", ".epub", ".txt", ".doc", ".docx"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Document
    } else if [".zip", ".rar", ".7z", ".tar"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Archive
    } else if [".exe", ".msi", ".iso"]
        .iter()
        .any(|ext| lower.ends_with(ext))
    {
        ContentType::Software
    } else {
        ContentType::Unknown
    };
    Some(value)
}

fn mock_file_record(query: &str, bind_addr: String) -> FileRecord {
    FileRecord {
        hashes: vec![HashType::Ed2k(hex::encode(keyword_target(query).0))],
        names: vec![format!(
            "{}.bin",
            query.trim().replace(' ', "_").to_lowercase()
        )],
        size: Some(1_048_576),
        content_type: Some(ContentType::Unknown),
        tags: vec![TagEntry {
            key: "origin".into(),
            value: serde_json::json!("mock_fallback"),
        }],
        sources: vec![Source {
            protocol: Protocol::Kad2,
            address: bind_addr,
            extra: serde_json::json!({ "search_mode": "mock" }),
        }],
    }
}

fn tag_to_entry(tag: &Tag) -> TagEntry {
    let key = match &tag.name {
        TagName::Short(value) => format!("tag_{value:02x}"),
        TagName::Long(value) => value.clone(),
    };
    let value = match &tag.value {
        TagValue::Hash(value) => serde_json::json!(value.to_string()),
        TagValue::String(value) => serde_json::json!(value),
        TagValue::U64(value) => serde_json::json!(value),
        TagValue::U32(value) => serde_json::json!(value),
        TagValue::U16(value) => serde_json::json!(value),
        TagValue::U8(value) => serde_json::json!(value),
        TagValue::Float(value) => serde_json::json!(value),
        TagValue::Bool(value) => serde_json::json!(value),
        TagValue::Blob(value) => serde_json::json!(hex::encode(value)),
    };
    TagEntry { key, value }
}

fn map_search_result_for(dht: &DhtNode, result: &SearchResult) -> Result<FileRecord> {
    Ok(FileRecord {
        hashes: vec![HashType::Ed2k(result.hash.to_string())],
        names: result.names.clone(),
        size: result.size,
        content_type: guess_content_type(result.names.first()),
        tags: result.tags.iter().map(tag_to_entry).collect(),
        sources: vec![Source {
            protocol: Protocol::Kad2,
            address: dht.bind_addr()?.to_string(),
            extra: serde_json::json!({
                "search_mode": "network",
                "availability": result.availability,
            }),
        }],
    })
}

async fn record_snoop_entry(
    snoop_queue: &Arc<Mutex<SnoopQueue>>,
    query: String,
    hash: Option<HashType>,
) {
    let mut queue = snoop_queue.lock().await;
    queue.record(query, hash, Utc::now());
}

async fn next_passive_keyword_target(snoop_queue: &Arc<Mutex<SnoopQueue>>) -> Option<NodeId> {
    snoop_queue
        .lock()
        .await
        .select_next_keyword_target(Utc::now())
}

async fn persist_nodes_dat_for(dht: &DhtNode, state_paths: &AgentStatePaths) -> Result<()> {
    let contacts = dht
        .routing_contacts()
        .await
        .into_iter()
        .map(|contact| BootstrapContact {
            node_id: contact.id,
            ip: contact.ip,
            udp_port: contact.udp_port,
            tcp_port: contact.tcp_port,
            version: contact.kad_version,
        })
        .collect::<Vec<_>>();
    let bytes = encode_nodes_dat(&contacts)?;
    fs::write(&state_paths.nodes_dat_path, bytes)
        .with_context(|| format!("failed to write {}", state_paths.nodes_dat_path.display()))?;
    Ok(())
}

async fn handle_unsolicited_packet(
    dht: &DhtNode,
    snoop_queue: &Arc<Mutex<SnoopQueue>>,
    packet: KadPacket,
    from: SocketAddr,
) -> Result<()> {
    match packet {
        KadPacket::Ping => dht.send_packet(from, &KadPacket::Pong).await?,
        KadPacket::HelloReq(req) => {
            if let Some(udp_key) = req.udp_key {
                dht.register_peer_key(from, udp_key);
            }
            if let std::net::IpAddr::V4(ip) = from.ip() {
                let _ = dht
                    .add_contact(Contact::new(
                        req.node_id,
                        ip,
                        from.port(),
                        req.tcp_port,
                        req.version,
                    ))
                    .await;
            }
            let bind_addr = dht.bind_addr()?;
            let tcp_ip = match bind_addr.ip() {
                std::net::IpAddr::V4(ip) => u32::from_be_bytes(ip.octets()),
                std::net::IpAddr::V6(_) => 0,
            };
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::HelloRes(overlord_kad_proto::HelloRes {
                        node_id: dht.own_id(),
                        tcp_ip,
                        tcp_port: bind_addr.port(),
                        version: overlord_kad_proto::KAD_VERSION,
                        udp_key: Some(dht.udp_key()),
                        tags: Vec::new(),
                    }),
                )
                .await;
        }
        KadPacket::HelloRes(res) => {
            if let Some(udp_key) = res.udp_key {
                dht.register_peer_key(from, udp_key);
            }
            if let std::net::IpAddr::V4(ip) = from.ip() {
                let _ = dht
                    .add_contact(Contact::new(
                        res.node_id,
                        ip,
                        from.port(),
                        res.tcp_port,
                        res.version,
                    ))
                    .await;
            }
            let _ = dht.send_packet(from, &KadPacket::HelloResAck).await;
        }
        KadPacket::BootstrapReq => {
            let bind_addr = dht.bind_addr()?;
            let contacts = dht
                .closest_contacts(&dht.own_id(), K)
                .await
                .into_iter()
                .map(contact_to_entry)
                .collect();
            dht.send_packet(
                from,
                &KadPacket::BootstrapRes(overlord_kad_proto::BootstrapRes {
                    sender_id: dht.own_id(),
                    sender_tcp_port: bind_addr.port(),
                    sender_version: overlord_kad_proto::KAD_VERSION,
                    contacts,
                }),
            )
            .await?;
        }
        KadPacket::Req(req) => {
            let contacts = dht
                .closest_contacts(&req.target, req.count as usize)
                .await
                .into_iter()
                .map(contact_to_entry)
                .collect();
            dht.send_packet(
                from,
                &KadPacket::Res(overlord_kad_proto::Res {
                    target: req.target,
                    contacts,
                }),
            )
            .await?;
        }
        KadPacket::SearchKeyReq(req) => {
            record_snoop_entry(snoop_queue, format!("keyword:{}", req.target), None).await
        }
        KadPacket::SearchSourceReq(req) => {
            record_snoop_entry(
                snoop_queue,
                format!("source:{}", req.target),
                Some(HashType::Ed2k(hex::encode(req.target.0))),
            )
            .await
        }
        KadPacket::SearchNotesReq(req) => {
            record_snoop_entry(
                snoop_queue,
                format!("notes:{}", req.target),
                Some(HashType::Ed2k(hex::encode(req.target.0))),
            )
            .await
        }
        KadPacket::PublishKeyReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        KadPacket::PublishSourceReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        KadPacket::PublishNotesReq(req) => {
            let _ = dht
                .send_packet(
                    from,
                    &KadPacket::PublishRes(overlord_kad_proto::PublishRes {
                        target: req.target,
                        load: 0,
                    }),
                )
                .await;
        }
        _ => {}
    }
    Ok(())
}

fn contact_to_entry(contact: Contact) -> ContactEntry {
    ContactEntry {
        node_id: contact.id,
        ip: u32::from_be_bytes(contact.ip.octets()),
        udp_port: contact.udp_port,
        tcp_port: contact.tcp_port,
        version: contact.kad_version,
    }
}

impl AgentStatePaths {
    fn from_config(config: &EmuleAgentConfig) -> Self {
        let state_dir = PathBuf::from(&config.agent.state_dir);
        let nodes_dat_path = if config.p2p.kad.nodes_dat_path.trim().is_empty() {
            state_dir.join("overlord-kad.nodes.dat")
        } else {
            PathBuf::from(&config.p2p.kad.nodes_dat_path)
        };
        Self {
            node_id_path: state_dir.join("overlord-kad.node-id"),
            udp_key_path: state_dir.join("overlord-kad.udp-key"),
            nodes_dat_path,
            networking_config_path: state_dir.join("overlord-agent.networking.json"),
        }
    }
}

fn empty_networking_config() -> AgentNetworkingConfig {
    AgentNetworkingConfig {
        control: AgentControlConfig {
            bind_iface: None,
            bind_ip: None,
            selection_confirmed: false,
            listen_port: 13_301,
        },
        p2p: AgentP2pConfig {
            bind_iface: None,
            bind_ip: None,
            selection_confirmed: false,
            kad: AgentKadConfig {
                listen_port: 41_000,
            },
            ed2k: AgentEd2kConfig {
                listen_port: 41_001,
            },
        },
        nat: AgentNatConfig {
            p2p: AgentNatP2pConfig {
                enabled: false,
                backend_order: default_upnp_backend_order(),
                igd_ip: None,
                minissdpd_socket: None,
                ssdp_local_port: None,
                discovery_timeout_secs: 5,
                lease_duration_secs: 3_600,
                renew_margin_secs: 300,
                external_ip_override: None,
            },
        },
    }
}

fn apply_networking_config(config: &mut EmuleAgentConfig, desired: &AgentNetworkingConfig) {
    config.control.bind_iface = desired.control.bind_iface.clone();
    config.control.bind_ip = desired.control.bind_ip.clone();
    config.control.selection_confirmed = desired.control.selection_confirmed;
    config.control.listen_port = desired.control.listen_port;
    config.p2p.bind_iface = desired.p2p.bind_iface.clone();
    config.p2p.bind_ip = desired.p2p.bind_ip.clone();
    config.p2p.selection_confirmed = desired.p2p.selection_confirmed;
    config.p2p.kad.listen_port = desired.p2p.kad.listen_port;
    config.p2p.ed2k.listen_port = desired.p2p.ed2k.listen_port;
    config.nat.p2p.enabled = desired.nat.p2p.enabled;
    config.nat.p2p.backend_order = if desired.nat.p2p.backend_order.is_empty() {
        default_upnp_backend_order()
    } else {
        desired.nat.p2p.backend_order.clone()
    };
    config.nat.p2p.igd_ip = desired.nat.p2p.igd_ip.clone();
    config.nat.p2p.minissdpd_socket = desired.nat.p2p.minissdpd_socket.clone();
    config.nat.p2p.ssdp_local_port = desired.nat.p2p.ssdp_local_port;
    config.nat.p2p.discovery_timeout_secs = desired.nat.p2p.discovery_timeout_secs;
    config.nat.p2p.lease_duration_secs = desired.nat.p2p.lease_duration_secs;
    config.nat.p2p.renew_margin_secs = desired.nat.p2p.renew_margin_secs;
    config.nat.p2p.external_ip_override = desired.nat.p2p.external_ip_override.clone();
}

fn load_persisted_networking_config(config: &mut EmuleAgentConfig) -> Result<()> {
    let state_paths = AgentStatePaths::from_config(config);
    if !state_paths.networking_config_path.exists() {
        return Ok(());
    }

    let contents = fs::read_to_string(&state_paths.networking_config_path).with_context(|| {
        format!(
            "failed to read networking state from {}",
            state_paths.networking_config_path.display()
        )
    })?;
    let desired: AgentNetworkingConfig = serde_json::from_str(&contents).with_context(|| {
        format!(
            "failed to parse networking state from {}",
            state_paths.networking_config_path.display()
        )
    })?;
    apply_networking_config(config, &desired);
    Ok(())
}

fn persist_networking_config(
    state_paths: &AgentStatePaths,
    desired: &AgentNetworkingConfig,
) -> Result<()> {
    ensure_parent_dir(&state_paths.networking_config_path)?;
    let payload =
        serde_json::to_vec_pretty(desired).context("failed to serialize networking state")?;
    fs::write(&state_paths.networking_config_path, payload).with_context(|| {
        format!(
            "failed to persist networking state to {}",
            state_paths.networking_config_path.display()
        )
    })?;
    Ok(())
}

#[async_trait]
impl NatCapableAgent for OverlordAgentEmule {
    fn nat_config(&self) -> overlord_agent_nat::NatConfig {
        self.config
            .try_read()
            .map(|config| overlord_agent_nat::NatConfig {
                enabled: config.nat.p2p.enabled,
                backend_order: if config.nat.p2p.backend_order.is_empty() {
                    default_upnp_backend_order()
                } else {
                    config.nat.p2p.backend_order.clone()
                },
                bind_ip: config.p2p.bind_ip.clone(),
                igd_ip: config.nat.p2p.igd_ip.clone(),
                minissdpd_socket: config.nat.p2p.minissdpd_socket.clone(),
                ssdp_local_port: config.nat.p2p.ssdp_local_port,
                discovery_timeout_secs: config.nat.p2p.discovery_timeout_secs,
                lease_duration_secs: config.nat.p2p.lease_duration_secs,
                renew_margin_secs: config.nat.p2p.renew_margin_secs,
                external_ip_override: config.nat.p2p.external_ip_override.clone(),
            })
            .unwrap_or_default()
    }

    fn nat_mappings(&self) -> Vec<MappingSpec> {
        self.config
            .try_read()
            .ok()
            .and_then(|config| {
                Self::nat_mappings_from_config(&config, config.p2p.bind_ip.as_deref()).ok()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl IndexerService for OverlordAgentEmule {
    fn protocol(&self) -> Protocol {
        Protocol::Kad2
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn indexer_id(&self) -> Uuid {
        self.indexer_id
    }

    async fn start(&self) -> Result<()> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        restore_snoop_queue(&self.coordinator, self.indexer_id, &self.snoop_queue).await;
        self.reconcile_runtime().await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.stop_runtime().await?;
        flush_snoop_queue(&self.coordinator, self.indexer_id, &self.snoop_queue).await?;
        Ok(())
    }

    async fn search(&self, job: SearchJob) -> Result<()> {
        let runtime = self.runtime.lock().await.clone();
        let Some(runtime) = runtime else {
            anyhow::bail!("agent networking is waiting for interface selection");
        };
        let dht = runtime.dht.clone();
        let indexer_id = self.indexer_id;
        let config = self.config.clone();
        let callback_client = self.coordinator.clone();
        let active_searches = Arc::clone(&self.active_searches);
        let cancel = CancellationToken::new();
        {
            let mut active = active_searches.lock().await;
            if active.contains_key(&job.job_id) {
                anyhow::bail!("search {} is already active", job.job_id);
            }
            active.insert(
                job.job_id,
                ActiveSearchHandle {
                    cancel: cancel.clone(),
                },
            );
        }
        tokio::spawn(async move {
            let started_stats = SearchRunStats::default();
            if let Err(error) = emit_search_event(
                &callback_client,
                job.job_id,
                indexer_id,
                SearchEventStatus::Started,
                &started_stats,
                None,
            )
            .await
            {
                warn!("failed to report search start: {error}");
            }

            let outcome = match job.kind {
                SearchKind::Keyword => {
                    do_active_keyword_search(&dht, indexer_id, &job, config, cancel.clone()).await
                }
                SearchKind::Source => {
                    do_active_source_search(&dht, indexer_id, &job, cancel.clone()).await
                }
                SearchKind::Notes => Err(anyhow::anyhow!("notes search is not wired yet")),
            };

            let final_event = match outcome {
                Ok(stats) if cancel.is_cancelled() => (SearchEventStatus::Cancelled, stats, None),
                Ok(stats) => (SearchEventStatus::Completed, stats, None),
                Err(_error) if cancel.is_cancelled() => (
                    SearchEventStatus::Cancelled,
                    SearchRunStats::default(),
                    None,
                ),
                Err(error) => (
                    SearchEventStatus::Failed,
                    SearchRunStats::default(),
                    Some(error.to_string()),
                ),
            };

            if let Err(error) = emit_search_event(
                &callback_client,
                job.job_id,
                indexer_id,
                final_event.0,
                &final_event.1,
                final_event.2,
            )
            .await
            {
                warn!("failed to report search completion: {error}");
            }

            active_searches.lock().await.remove(&job.job_id);
        });
        Ok(())
    }

    async fn cancel_search(&self, job_id: Uuid) -> Result<()> {
        let Some(handle) = self.active_searches.lock().await.get(&job_id).cloned() else {
            anyhow::bail!("search {job_id} is not active");
        };
        handle.cancel.cancel();
        Ok(())
    }

    async fn stats(&self) -> Result<IndexerStats> {
        let queue_depth = self.snoop_queue.lock().await.len() as u32;
        let uptime_secs = self.started_at.elapsed().as_secs();
        let runtime = self.runtime.lock().await.clone();
        let crawl_rate = if uptime_secs == 0 {
            0.0
        } else {
            runtime
                .as_ref()
                .map(|runtime| runtime.passive_result_count.load(Ordering::Relaxed) as f32)
                .unwrap_or(0.0)
                / uptime_secs as f32
        };
        let interface_report = self.interface_report().await;

        Ok(IndexerStats {
            indexer_id: self.indexer_id,
            protocol: Protocol::Kad2,
            peers_connected: runtime
                .as_ref()
                .map(|runtime| runtime.dht.routing_table_size() as u32)
                .unwrap_or(0),
            crawl_rate,
            snoop_queue_depth: queue_depth,
            staging_queue_depth: 0,
            uptime_secs,
            nat: match runtime {
                Some(runtime) => Some(runtime.nat.status().await.snapshot()),
                None => None,
            },
            interface_report: Some(interface_report),
        })
    }

    async fn apply_config(&self, config: ConfigUpdate) -> Result<()> {
        let next: AgentNetworkingConfig = serde_json::from_value(config.config)
            .context("invalid config payload for overlord-agent-emule")?;
        let mut guard = self.config.write().await;
        let old_networking = Self::networking_config(&guard);
        apply_networking_config(&mut guard, &next);
        let new_networking = Self::networking_config(&guard);
        drop(guard);
        if old_networking != new_networking {
            persist_networking_config(&self.state_paths, &new_networking)?;
            self.request_restart();
        }
        Ok(())
    }

    async fn seed_popular(&self, hashes: Vec<PopularHash>) -> Result<()> {
        let runtime = self.runtime.lock().await.clone();
        let Some(runtime) = runtime else {
            anyhow::bail!("agent networking is waiting for interface selection");
        };
        seed_popular_impl(&runtime.dht, hashes).await
    }

    async fn flush_snoop(&self) -> Result<Vec<SnoopEntry>> {
        Ok(self.snoop_queue.lock().await.snapshot())
    }

    async fn interfaces(&self) -> Result<AgentNetworkReport> {
        Ok(self.interface_report().await)
    }
}

impl OverlordAgentEmule {
    async fn spawn_background_tasks(
        &self,
        runtime: &AgentNetworkRuntime,
        config: &EmuleAgentConfig,
    ) {
        let dht = runtime.dht.clone();
        let shutdown = Arc::clone(&runtime.shutdown);
        let state_paths = self.state_paths.clone();
        runtime.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) && !dht.is_bootstrapped() {
                match dht.bootstrap().await {
                    Ok(()) => {
                        if let Err(error) = persist_nodes_dat_for(&dht, &state_paths).await {
                            warn!("failed to persist nodes.dat after bootstrap: {error}");
                        }
                        break;
                    }
                    Err(error) => debug!("bootstrap retry failed: {error}"),
                }
                tokio::time::sleep(Duration::from_secs(BOOTSTRAP_RETRY_SECS)).await;
            }
        }));

        let dht = runtime.dht.clone();
        let shutdown = Arc::clone(&runtime.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        runtime.tasks.lock().await.push(tokio::spawn(async move {
            let mut packets = dht.subscribe_packets();
            while !shutdown.load(Ordering::Relaxed) {
                match packets.recv().await {
                    Ok((packet, from)) => {
                        if let Err(error) =
                            handle_unsolicited_packet(&dht, &snoop_queue, packet, from).await
                        {
                            debug!("unsolicited packet handling failed: {error}");
                        }
                    }
                    Err(error) => {
                        debug!("packet subscription closed: {error}");
                        break;
                    }
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let dht = runtime.dht.clone();
        let shutdown = Arc::clone(&runtime.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        let indexer_id = self.indexer_id;
        let passive_result_count = Arc::clone(&runtime.passive_result_count);
        runtime.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(PASSIVE_CRAWL_SECS)).await;
                if shutdown.load(Ordering::Relaxed) || !dht.is_bootstrapped() {
                    continue;
                }
                let Some(target) = next_passive_keyword_target(&snoop_queue).await else {
                    continue;
                };
                let mut stream = dht.search_keywords(target);
                let mut files = Vec::new();
                while let Some(result) = stream.next().await {
                    if let Ok(file) = map_search_result_for(&dht, &result) {
                        passive_result_count.fetch_add(1, Ordering::Relaxed);
                        files.push(file);
                        if files.len() >= PASSIVE_BATCH_SIZE {
                            let payload = ResultBatch {
                                job_id: None,
                                indexer_id,
                                protocol: Protocol::Kad2,
                                files: std::mem::take(&mut files),
                            };
                            if let Err(error) = coordinator.post_results(&payload).await {
                                warn!("failed to post passive result batch: {error}");
                            }
                        }
                    }
                }
                if !files.is_empty() {
                    let payload = ResultBatch {
                        job_id: None,
                        indexer_id,
                        protocol: Protocol::Kad2,
                        files,
                    };
                    if let Err(error) = coordinator.post_results(&payload).await {
                        warn!("failed to post passive result batch: {error}");
                    }
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let shutdown = Arc::clone(&runtime.shutdown);
        let snoop_queue = Arc::clone(&self.snoop_queue);
        let indexer_id = self.indexer_id;
        runtime.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(SNOOP_FLUSH_SECS)).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(error) = flush_snoop_queue(&coordinator, indexer_id, &snoop_queue).await
                {
                    debug!("snoop flush failed: {error}");
                }
            }
        }));

        let coordinator = self.coordinator.clone();
        let dht = runtime.dht.clone();
        let shutdown = Arc::clone(&runtime.shutdown);
        let republish_secs = config.p2p.kad.republish_interval_secs;
        runtime.tasks.lock().await.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(republish_secs)).await;
                if shutdown.load(Ordering::Relaxed) || !dht.is_bootstrapped() {
                    continue;
                }
                match coordinator.popular_hashes().await {
                    Ok(hashes) => {
                        if let Err(error) = seed_popular_impl(&dht, hashes).await {
                            debug!("republish cycle failed: {error}");
                        }
                    }
                    Err(error) => debug!("popular hash refresh failed: {error}"),
                }
            }
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EmuleAgentConfig, apply_networking_config, empty_networking_config, keyword_target,
        significant_keyword_words,
    };
    use overlord_agent_nat::{UPNP_MINIUPNPC_BACKEND, UPNP_RUPNP_BACKEND};

    #[test]
    fn significant_words_ignore_short_tokens() {
        assert_eq!(
            significant_keyword_words("A torino x train"),
            vec!["torino".to_string(), "train".to_string()]
        );
    }

    #[test]
    fn keyword_target_is_stable() {
        assert_eq!(
            hex::encode(keyword_target("Torino Train").0),
            "b2bc3aa39f375069e7c27eb83ce6baf3"
        );
    }

    #[test]
    fn empty_networking_config_prefers_miniupnpc_then_rupnp() {
        assert_eq!(
            empty_networking_config().nat.p2p.backend_order,
            vec![
                UPNP_MINIUPNPC_BACKEND.to_string(),
                UPNP_RUPNP_BACKEND.to_string()
            ]
        );
    }

    #[test]
    fn apply_networking_config_preserves_explicit_backend_order() {
        let mut config = EmuleAgentConfig::default();
        let mut desired = empty_networking_config();
        desired.nat.p2p.backend_order = vec![UPNP_RUPNP_BACKEND.to_string()];

        apply_networking_config(&mut config, &desired);

        assert_eq!(
            config.nat.p2p.backend_order,
            vec![UPNP_RUPNP_BACKEND.to_string()]
        );
    }
}
