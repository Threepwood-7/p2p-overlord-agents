use async_trait::async_trait;

use crate::types::NatStatus;

#[async_trait]
pub trait ReachabilityStrategy: Send + Sync + 'static {
    async fn on_nat_status_changed(&self, _status: NatStatus) {}
}

#[derive(Debug, Default)]
pub struct NoopReachabilityStrategy;

#[async_trait]
impl ReachabilityStrategy for NoopReachabilityStrategy {}
