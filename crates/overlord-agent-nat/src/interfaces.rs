use std::collections::HashMap;
use std::net::IpAddr;

use anyhow::Result;
use if_addrs::{IfAddr, get_if_addrs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceAddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInterfaceAddress {
    pub family: InterfaceAddressFamily,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInterface {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub addresses: Vec<AgentInterfaceAddress>,
    pub is_loopback: bool,
    pub is_vpn_candidate: bool,
    pub has_default_route: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceSelectionState {
    Pending,
    Confirmed,
    Applied,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceBindingSelection {
    pub selected_interface_name: Option<String>,
    pub bind_ip: Option<String>,
    pub selection_confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceBindingReport {
    pub recommended_interface_name: Option<String>,
    pub selected_interface_name: Option<String>,
    pub resolved_bind_ip: Option<String>,
    pub selection_confirmed: bool,
    pub ready: bool,
    pub state: InterfaceSelectionState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedInterfaceBindingReport {
    pub selected_interface_name: Option<String>,
    pub bind_ip: Option<String>,
    pub recommended_interface_name: Option<String>,
    pub selection_confirmed: bool,
    pub ready: bool,
    pub state: InterfaceSelectionState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentNetworkReport {
    #[serde(default)]
    pub interfaces: Vec<AgentInterface>,
    pub control: InterfaceBindingReport,
    pub p2p: InterfaceBindingReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentNatConfig {
    pub enabled: bool,
    #[serde(default)]
    pub backend_order: Vec<String>,
    pub igd_ip: Option<String>,
    pub external_ip_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentNetworkingConfig {
    pub control: InterfaceBindingSelection,
    pub p2p: InterfaceBindingSelection,
    pub nat: AgentNatConfig,
}

pub fn detect_interfaces() -> Result<Vec<AgentInterface>> {
    let mut by_name = HashMap::<String, AgentInterface>::new();
    for iface in get_if_addrs()? {
        let description = platform_description(&iface.name);
        let entry = by_name.entry(iface.name.clone()).or_insert_with(|| AgentInterface {
            name: iface.name.clone(),
            description,
            addresses: Vec::new(),
            is_loopback: iface.is_loopback(),
            is_vpn_candidate: is_vpn_like(&iface.name),
            has_default_route: false,
        });
        entry.is_vpn_candidate =
            entry.is_vpn_candidate || entry.description.as_deref().is_some_and(is_vpn_like);
        entry.has_default_route = entry.has_default_route || platform_has_default_route(&iface.name);
        match iface.addr {
            IfAddr::V4(v4) => entry.addresses.push(AgentInterfaceAddress {
                family: InterfaceAddressFamily::Ipv4,
                address: v4.ip.to_string(),
            }),
            IfAddr::V6(v6) => entry.addresses.push(AgentInterfaceAddress {
                family: InterfaceAddressFamily::Ipv6,
                address: v6.ip.to_string(),
            }),
        }
    }

    let mut interfaces = by_name.into_values().collect::<Vec<_>>();
    interfaces.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(interfaces)
}

pub fn recommend_interface(interfaces: &[AgentInterface]) -> Option<String> {
    interfaces
        .iter()
        .find(|iface| iface.is_vpn_candidate && iface.addresses.iter().any(is_ipv4_address))
        .or_else(|| {
            interfaces.iter().find(|iface| {
                iface.has_default_route && !iface.is_loopback && iface.addresses.iter().any(is_ipv4_address)
            })
        })
        .or_else(|| {
            interfaces
                .iter()
                .find(|iface| !iface.is_loopback && iface.addresses.iter().any(is_ipv4_address))
        })
        .map(|iface| iface.name.clone())
}

pub fn resolve_bind_ip(
    interfaces: &[AgentInterface],
    selected_interface_name: Option<&str>,
    bind_ip_override: Option<&str>,
) -> Option<String> {
    if let Some(bind_ip) = bind_ip_override.filter(|ip| !ip.trim().is_empty()) {
        return Some(bind_ip.to_string());
    }
    let selected_name = selected_interface_name?;
    interfaces
        .iter()
        .find(|iface| iface.name == selected_name)
        .and_then(|iface| {
            iface
                .addresses
                .iter()
                .find(|address| matches!(address.family, InterfaceAddressFamily::Ipv4))
        })
        .map(|address| address.address.clone())
}

pub fn build_interface_binding_report(
    binding: &ResolvedInterfaceBindingReport,
) -> InterfaceBindingReport {
    InterfaceBindingReport {
        recommended_interface_name: binding.recommended_interface_name.clone(),
        selected_interface_name: binding.selected_interface_name.clone(),
        resolved_bind_ip: binding.bind_ip.clone(),
        selection_confirmed: binding.selection_confirmed,
        ready: binding.ready,
        state: binding.state,
        last_error: binding.last_error.clone(),
    }
}

fn is_ipv4_address(address: &AgentInterfaceAddress) -> bool {
    matches!(address.family, InterfaceAddressFamily::Ipv4)
}

fn is_vpn_like(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "tun",
        "tap",
        "wireguard",
        "wg",
        "openvpn",
        "hide.me",
        "nord",
        "proton",
        "tailscale",
        "zerotier",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
}

#[cfg(windows)]
fn platform_description(interface_name: &str) -> Option<String> {
    ipconfig::get_adapters()
        .ok()?
        .into_iter()
        .find(|adapter| adapter.adapter_name() == interface_name)
        .map(|adapter| {
            let friendly = adapter.friendly_name().to_string();
            let description = adapter.description().to_string();
            if friendly.eq_ignore_ascii_case(&description) {
                friendly
            } else {
                format!("{friendly} ({description})")
            }
        })
}

#[cfg(not(windows))]
fn platform_description(_interface_name: &str) -> Option<String> {
    None
}

#[cfg(windows)]
fn platform_has_default_route(interface_name: &str) -> bool {
    ipconfig::get_adapters()
        .ok()
        .into_iter()
        .flatten()
        .find(|adapter| adapter.adapter_name() == interface_name)
        .is_some_and(|adapter| adapter.gateways().iter().any(|gateway| *gateway != IpAddr::from([0, 0, 0, 0])))
}

#[cfg(not(windows))]
fn platform_has_default_route(_interface_name: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::{
        AgentInterface, AgentInterfaceAddress, InterfaceAddressFamily, ResolvedInterfaceBindingReport,
        build_interface_binding_report, recommend_interface, resolve_bind_ip,
    };

    fn iface(name: &str, vpn: bool, default_route: bool, ip: &str) -> AgentInterface {
        AgentInterface {
            name: name.to_string(),
            description: None,
            addresses: vec![AgentInterfaceAddress {
                family: InterfaceAddressFamily::Ipv4,
                address: ip.to_string(),
            }],
            is_loopback: false,
            is_vpn_candidate: vpn,
            has_default_route: default_route,
        }
    }

    #[test]
    fn recommend_interface_prefers_vpn() {
        let interfaces = vec![
            iface("Ethernet", false, true, "192.168.1.10"),
            iface("hide.me", true, false, "10.10.10.2"),
        ];
        assert_eq!(recommend_interface(&interfaces).as_deref(), Some("hide.me"));
    }

    #[test]
    fn resolve_bind_ip_prefers_override() {
        let interfaces = vec![iface("hide.me", true, false, "10.10.10.2")];
        assert_eq!(
            resolve_bind_ip(&interfaces, Some("hide.me"), Some("10.99.99.2")).as_deref(),
            Some("10.99.99.2")
        );
    }

    #[test]
    fn resolve_bind_ip_allows_any_override_without_interface_selection() {
        assert_eq!(
            resolve_bind_ip(&[], None, Some("0.0.0.0")).as_deref(),
            Some("0.0.0.0")
        );
    }

    #[test]
    fn build_interface_binding_report_preserves_selection_state() {
        let binding = ResolvedInterfaceBindingReport {
            selected_interface_name: Some("hide.me".to_string()),
            bind_ip: Some("10.10.10.2".to_string()),
            recommended_interface_name: Some("hide.me".to_string()),
            selection_confirmed: true,
            ready: true,
            state: super::InterfaceSelectionState::Applied,
            last_error: None,
        };
        let report = build_interface_binding_report(&binding);
        assert!(report.ready);
        assert_eq!(report.selected_interface_name.as_deref(), Some("hide.me"));
        assert_eq!(report.resolved_bind_ip.as_deref(), Some("10.10.10.2"));
    }
}
