use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use overlord_agent_nat::{
    MappedEndpoint, MappingExposure, MappingSpec, NatConfig, NatStatus, NatStatusSnapshot,
    PortMappingProvider, TransportProtocol, built_in_upnp_port_mapping_providers,
    default_upnp_backend_order,
};
use tokio::{sync::RwLock, time::sleep};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "minirupnpc")]
#[command(about = "NAT/UPnP mapping diagnostic tool for Overlord")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Map(MapArgs),
    Cleanup(CleanupArgs),
}

#[derive(Debug, Args, Clone)]
struct SharedArgs {
    #[arg(long, default_value_t = default_backend_arg())]
    backend: String,
    #[arg(long)]
    bind_ip: Option<String>,
    #[arg(long)]
    igd_ip: Option<String>,
    #[arg(long)]
    external_ip_override: Option<String>,
    #[arg(long, default_value_t = 5)]
    discovery_timeout_secs: u64,
    #[arg(long, default_value_t = 3_600)]
    lease_duration_secs: u32,
    #[arg(long, default_value_t = 300)]
    renew_margin_secs: u64,
    #[arg(long, default_value_t = 41_000)]
    udp_port: u16,
    #[arg(long, default_value_t = 41_001)]
    tcp_port: u16,
    #[arg(long)]
    ssdp_bind_ip: Option<String>,
}

#[derive(Debug, Args)]
struct MapArgs {
    #[command(flatten)]
    shared: SharedArgs,
    #[arg(long, default_value_t = 0)]
    hold_secs: u64,
    #[arg(long)]
    skip_pre_cleanup: bool,
    #[arg(long)]
    leave_mapped: bool,
}

#[derive(Debug, Args)]
struct CleanupArgs {
    #[command(flatten)]
    shared: SharedArgs,
}

pub async fn run() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    match cli.command {
        Command::Map(args) => run_map(args).await,
        Command::Cleanup(args) => run_cleanup(args).await,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
}

async fn run_map(args: MapArgs) -> Result<()> {
    apply_ssdp_bind_override(args.shared.ssdp_bind_ip.as_deref());

    let provider = resolve_provider(&args.shared.backend)?;
    let config = build_config(&args.shared);
    let mappings = build_mappings(args.shared.udp_port, args.shared.tcp_port);
    let status = Arc::new(RwLock::new(NatStatus::default()));

    if !args.skip_pre_cleanup {
        cleanup_standard_ports(
            provider.as_ref(),
            &config,
            args.shared.udp_port,
            args.shared.tcp_port,
            &status,
        )
        .await?;
        print_status("after_pre_cleanup", &status).await?;
    }

    info!(
        "reconciling backend={} bind_ip={:?} igd_ip={:?} ssdp_bind_ip={:?}",
        args.shared.backend, args.shared.bind_ip, args.shared.igd_ip, args.shared.ssdp_bind_ip
    );
    provider
        .reconcile(&config, &mappings, Arc::clone(&status))
        .await
        .with_context(|| format!("backend {} reconcile failed", provider.name()))?;
    print_status("after_reconcile", &status).await?;

    if args.hold_secs > 0 {
        info!("holding mappings for {}s", args.hold_secs);
        sleep(Duration::from_secs(args.hold_secs)).await;
        print_status("during_hold", &status).await?;
    }

    if !args.leave_mapped {
        let mapped = status.read().await.mappings.clone();
        provider
            .release(&config, &mapped, Arc::clone(&status))
            .await
            .with_context(|| format!("backend {} release failed", provider.name()))?;
        print_status("after_release", &status).await?;
    }

    Ok(())
}

async fn run_cleanup(args: CleanupArgs) -> Result<()> {
    apply_ssdp_bind_override(args.shared.ssdp_bind_ip.as_deref());

    let provider = resolve_provider(&args.shared.backend)?;
    let config = build_config(&args.shared);
    let status = Arc::new(RwLock::new(NatStatus::default()));

    cleanup_standard_ports(
        provider.as_ref(),
        &config,
        args.shared.udp_port,
        args.shared.tcp_port,
        &status,
    )
    .await?;
    print_status("after_cleanup", &status).await?;
    Ok(())
}

fn default_backend_arg() -> String {
    default_upnp_backend_order()
        .into_iter()
        .next()
        .unwrap_or_else(|| "upnp_rupnp".to_string())
}

fn apply_ssdp_bind_override(ssdp_bind_ip: Option<&str>) {
    if let Some(ip) = ssdp_bind_ip {
        let ssdp_bind_addr = format!("{ip}:0");
        unsafe {
            std::env::set_var("SSDP_CLIENT_BIND_ADDR", &ssdp_bind_addr);
        }
        info!("set SSDP_CLIENT_BIND_ADDR={ssdp_bind_addr}");
    }
}

fn resolve_provider(backend: &str) -> Result<Arc<dyn PortMappingProvider>> {
    built_in_upnp_port_mapping_providers()
        .into_iter()
        .find(|provider| provider.name() == backend)
        .ok_or_else(|| anyhow!("unknown backend {backend}"))
}

fn build_config(args: &SharedArgs) -> NatConfig {
    NatConfig {
        enabled: true,
        backend_order: vec![args.backend.clone()],
        bind_ip: args.bind_ip.clone(),
        igd_ip: args.igd_ip.clone(),
        discovery_timeout_secs: args.discovery_timeout_secs,
        lease_duration_secs: args.lease_duration_secs,
        renew_margin_secs: args.renew_margin_secs,
        external_ip_override: args.external_ip_override.clone(),
    }
}

fn build_mappings(udp_port: u16, tcp_port: u16) -> Vec<MappingSpec> {
    vec![
        MappingSpec {
            name: "kad".to_string(),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), udp_port),
            protocol: TransportProtocol::Udp,
            exposure: MappingExposure::Required,
            preferred_external_port: Some(udp_port),
        },
        MappingSpec {
            name: "ed2k".to_string(),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), tcp_port),
            protocol: TransportProtocol::Tcp,
            exposure: MappingExposure::Required,
            preferred_external_port: Some(tcp_port),
        },
    ]
}

async fn cleanup_standard_ports(
    provider: &dyn PortMappingProvider,
    config: &NatConfig,
    udp_port: u16,
    tcp_port: u16,
    status: &Arc<RwLock<NatStatus>>,
) -> Result<()> {
    let dummy = vec![
        dummy_mapping("kad", TransportProtocol::Udp, udp_port),
        dummy_mapping("ed2k", TransportProtocol::Tcp, tcp_port),
    ];
    let _ = provider.release(config, &dummy, Arc::clone(status)).await;
    Ok(())
}

fn dummy_mapping(name: &str, protocol: TransportProtocol, port: u16) -> MappedEndpoint {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    MappedEndpoint {
        name: name.to_string(),
        protocol,
        local_addr: addr,
        external_addr: addr,
        lease_expires_in_secs: 0,
        backend: String::new(),
    }
}

async fn print_status(label: &str, status: &Arc<RwLock<NatStatus>>) -> Result<()> {
    let snapshot = status.read().await.snapshot();
    print_snapshot(label, &snapshot)
}

fn print_snapshot(label: &str, snapshot: &NatStatusSnapshot) -> Result<()> {
    println!("=== {label} ===");
    println!("{}", serde_json::to_string_pretty(snapshot)?);
    Ok(())
}
