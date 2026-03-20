pub mod client;
pub mod server;
pub mod service;
pub mod types;

pub use client::CoordinatorClient;
pub use overlord_agent_nat::{
    AgentInterface, AgentInterfaceAddress, AgentNetworkReport, AgentNetworkSelections,
    InterfaceAddressFamily, InterfaceBindingReport, InterfaceBindingSelection,
    InterfaceSelectionState, MappedEndpoint, MappingExposure, MappingSpec, NatConfig, NatStatus,
    NatStatusSnapshot, ResolvedInterfaceBindingReport, SelectedGateway, TransportProtocol,
};
pub use server::{IndexerServer, RunningIndexerServer};
pub use service::IndexerService;
pub use types::{
    ConfigUpdate, ContentType, FileRecord, HashType, IndexerRegistration, IndexerStats,
    PopularHash, Protocol, RegisterRequest, RegistrationResponse, ResultBatch, SearchJob,
    SnoopEntry, Source, TagEntry,
};
