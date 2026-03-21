use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use overlord_agent_nat::{
    MappedEndpoint, MappingExposure, MappingSpec, NatConfig, NatStatus, PortMappingProvider,
    TransportProtocol, built_in_upnp_port_mapping_providers, default_upnp_backend_order,
};
use tokio::{sync::RwLock, time::sleep};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct HarnessOptions {
    backend: String,
    bind_ip: Option<String>,
    igd_ip: Option<String>,
    external_ip_override: Option<String>,
    discovery_timeout_secs: u64,
    lease_duration_secs: u32,
    renew_margin_secs: u64,
    udp_port: u16,
    tcp_port: u16,
    hold_secs: u64,
    cleanup_only: bool,
    skip_cleanup: bool,
    ssdp_bind_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let options = HarnessOptions::parse_from_env()?;

    if let Some(ssdp_bind_addr) = options.ssdp_bind_addr.as_deref() {
        unsafe {
            std::env::set_var("SSDP_CLIENT_BIND_ADDR", ssdp_bind_addr);
        }
        info!("set SSDP_CLIENT_BIND_ADDR={ssdp_bind_addr}");
    }

    let providers = built_in_upnp_port_mapping_providers();
    let provider = providers
        .into_iter()
        .find(|provider| provider.name() == options.backend)
        .ok_or_else(|| anyhow!("unknown backend {}", options.backend))?;

    let config = NatConfig {
        enabled: true,
        backend_order: vec![options.backend.clone()],
        bind_ip: options.bind_ip.clone(),
        igd_ip: options.igd_ip.clone(),
        discovery_timeout_secs: options.discovery_timeout_secs,
        lease_duration_secs: options.lease_duration_secs,
        renew_margin_secs: options.renew_margin_secs,
        external_ip_override: options.external_ip_override.clone(),
    };
    let mappings = build_mappings(options.udp_port, options.tcp_port);
    let status = Arc::new(RwLock::new(NatStatus::default()));

    if !options.skip_cleanup {
        cleanup_test_ports(provider.as_ref(), &config, options.udp_port, options.tcp_port, &status)
            .await?;
    }

    if options.cleanup_only {
        print_status("cleanup_only", &status).await?;
        return Ok(());
    }

    info!(
        "reconciling backend={} bind_ip={:?} igd_ip={:?} ssdp_bind_addr={:?}",
        options.backend, options.bind_ip, options.igd_ip, options.ssdp_bind_addr
    );
    provider
        .reconcile(&config, &mappings, Arc::clone(&status))
        .await
        .with_context(|| format!("backend {} reconcile failed", provider.name()))?;
    print_status("after_reconcile", &status).await?;

    if options.hold_secs > 0 {
        info!("holding mappings for {}s", options.hold_secs);
        sleep(Duration::from_secs(options.hold_secs)).await;
        print_status("during_hold", &status).await?;
    }

    if !options.skip_cleanup {
        let mapped = status.read().await.mappings.clone();
        provider
            .release(&config, &mapped, Arc::clone(&status))
            .await
            .with_context(|| format!("backend {} release failed", provider.name()))?;
        print_status("after_release", &status).await?;
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
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

async fn cleanup_test_ports(
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
    println!("=== {label} ===");
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

impl HarnessOptions {
    fn parse_from_env() -> Result<Self> {
        let mut raw = std::env::args().skip(1).collect::<Vec<_>>();
        if raw.iter().any(|arg| arg == "--help" || arg == "-h") {
            print_help();
            std::process::exit(0);
        }

        let mut values = HashMap::new();
        let mut flags = Vec::new();
        while let Some(arg) = raw.first().cloned() {
            raw.remove(0);
            if let Some(name) = arg.strip_prefix("--") {
                if matches!(name, "cleanup-only" | "skip-cleanup") {
                    flags.push(name.to_string());
                    continue;
                }
                let value = raw
                    .first()
                    .cloned()
                    .ok_or_else(|| anyhow!("missing value for --{name}"))?;
                raw.remove(0);
                values.insert(name.to_string(), value);
            } else {
                bail!("unexpected argument {arg}");
            }
        }

        Ok(Self {
            backend: values
                .remove("backend")
                .unwrap_or_else(|| default_upnp_backend_order()[0].clone()),
            bind_ip: values.remove("bind-ip"),
            igd_ip: values.remove("igd-ip"),
            external_ip_override: values.remove("external-ip-override"),
            discovery_timeout_secs: parse_u64(&values, "discovery-timeout-secs", 5)?,
            lease_duration_secs: parse_u32(&values, "lease-duration-secs", 3600)?,
            renew_margin_secs: parse_u64(&values, "renew-margin-secs", 300)?,
            udp_port: parse_u16(&values, "udp-port", 41000)?,
            tcp_port: parse_u16(&values, "tcp-port", 41001)?,
            hold_secs: parse_u64(&values, "hold-secs", 0)?,
            cleanup_only: flags.iter().any(|flag| flag == "cleanup-only"),
            skip_cleanup: flags.iter().any(|flag| flag == "skip-cleanup"),
            ssdp_bind_addr: values.remove("ssdp-bind-addr"),
        })
    }
}

fn parse_u16(values: &HashMap<String, String>, key: &str, default: u16) -> Result<u16> {
    values
        .get(key)
        .map(|value| value.parse::<u16>().with_context(|| format!("invalid --{key} value")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u32(values: &HashMap<String, String>, key: &str, default: u32) -> Result<u32> {
    values
        .get(key)
        .map(|value| value.parse::<u32>().with_context(|| format!("invalid --{key} value")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u64(values: &HashMap<String, String>, key: &str, default: u64) -> Result<u64> {
    values
        .get(key)
        .map(|value| value.parse::<u64>().with_context(|| format!("invalid --{key} value")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn print_help() {
    println!("upnp_backend_harness");
    println!("  --backend <id>                 Backend id, default upnp_rupnp");
    println!("  --bind-ip <ipv4>              nat.bind_ip override");
    println!("  --igd-ip <ipv4>               nat.igd_ip override");
    println!("  --ssdp-bind-addr <addr:port>  Override SSDP client bind address");
    println!("  --udp-port <port>             UDP mapping port, default 41000");
    println!("  --tcp-port <port>             TCP mapping port, default 41001");
    println!("  --discovery-timeout-secs <n>  Discovery timeout, default 5");
    println!("  --lease-duration-secs <n>     Lease duration, default 3600");
    println!("  --renew-margin-secs <n>       Renew margin, default 300");
    println!("  --external-ip-override <ip>   Override external IP");
    println!("  --hold-secs <n>               Keep mappings alive before release");
    println!("  --cleanup-only                Delete the test ports and exit");
    println!("  --skip-cleanup                Skip pre/post release");
}
