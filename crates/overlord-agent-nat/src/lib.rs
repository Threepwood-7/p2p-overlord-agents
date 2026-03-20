mod config;
mod interfaces;
mod manager;
mod provider;
mod reachability;
mod types;

pub use config::NatConfig;
pub use interfaces::{
    AgentInterface, AgentInterfaceAddress, AgentInterfaceReport, InterfaceAddressFamily,
    InterfaceSelectionState, ResolvedInterfaceBinding, build_interface_report,
    detect_interfaces, recommend_interface, resolve_bind_ip,
};
pub use manager::{NatManager, NatManagerBuilder};
pub use provider::{NatCapableAgent, PortMappingProvider, RupnpPortMappingProvider};
pub use reachability::{NoopReachabilityStrategy, ReachabilityStrategy};
pub use types::{
    MappedEndpoint, MappingExposure, MappingSpec, NatStatus, NatStatusSnapshot, SelectedGateway,
    TransportProtocol,
};
