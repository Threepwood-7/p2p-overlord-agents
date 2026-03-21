use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{
    config::NatConfig,
    types::{MappedEndpoint, MappingSpec, NatStatus},
};

mod igd;
mod rupnp;

pub use igd::IgdPortMappingProvider;
pub use rupnp::RupnpPortMappingProvider;

pub const UPNP_RUPNP_BACKEND: &str = "upnp_rupnp";
pub const UPNP_IGD_BACKEND: &str = "upnp_igd";

#[async_trait]
pub trait NatCapableAgent: Send + Sync + 'static {
    fn nat_config(&self) -> NatConfig;
    fn nat_mappings(&self) -> Vec<MappingSpec>;
}

#[async_trait]
pub trait PortMappingProvider: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn reconcile(
        &self,
        config: &NatConfig,
        mappings: &[MappingSpec],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()>;

    async fn release(
        &self,
        config: &NatConfig,
        mappings: &[MappedEndpoint],
        status: Arc<RwLock<NatStatus>>,
    ) -> Result<()>;
}

pub fn default_upnp_backend_order() -> Vec<String> {
    vec![UPNP_RUPNP_BACKEND.to_string()]
}

pub fn built_in_upnp_port_mapping_providers() -> Vec<Arc<dyn PortMappingProvider>> {
    vec![
        Arc::new(RupnpPortMappingProvider),
        Arc::new(IgdPortMappingProvider),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        UPNP_IGD_BACKEND, UPNP_RUPNP_BACKEND, built_in_upnp_port_mapping_providers,
        default_upnp_backend_order,
    };

    #[test]
    fn default_upnp_backend_order_prefers_rupnp() {
        assert_eq!(
            default_upnp_backend_order(),
            vec![UPNP_RUPNP_BACKEND.to_string()]
        );
    }

    #[test]
    fn built_in_upnp_port_mapping_providers_use_explicit_backend_ids() {
        let provider_names = built_in_upnp_port_mapping_providers()
            .into_iter()
            .map(|provider| provider.name().to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            provider_names,
            vec![UPNP_RUPNP_BACKEND.to_string(), UPNP_IGD_BACKEND.to_string(),]
        );
    }
}
