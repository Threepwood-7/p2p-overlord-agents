mod config;
mod interfaces;
mod manager;
mod provider;
mod reachability;
mod types;

pub use config::NatConfig;
pub use interfaces::{
    AgentControlConfig, AgentEd2kConfig, AgentInterface, AgentInterfaceAddress, AgentKadConfig,
    AgentNatConfig, AgentNatP2pConfig, AgentNetworkReport, AgentNetworkingConfig, AgentP2pConfig,
    InterfaceAddressFamily, InterfaceBindingReport, InterfaceBindingSelection,
    InterfaceSelectionState, ResolvedInterfaceBindingReport, build_interface_binding_report,
    detect_interfaces, recommend_interface, resolve_bind_ip,
};
pub use manager::{NatManager, NatManagerBuilder};
pub use provider::{
    IgdPortMappingProvider, MiniupnpcPortMappingProvider, NatCapableAgent, PortMappingProvider,
    RupnpPortMappingProvider, UPNP_IGD_BACKEND, UPNP_MINIUPNPC_BACKEND, UPNP_RUPNP_BACKEND,
    built_in_upnp_port_mapping_providers, default_upnp_backend_order,
};
pub use reachability::{NoopReachabilityStrategy, ReachabilityStrategy};
pub use types::{
    MappedEndpoint, MappingExposure, MappingSpec, NatStatus, NatStatusSnapshot, SelectedGateway,
    TransportProtocol,
};
