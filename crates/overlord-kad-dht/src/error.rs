#[derive(Debug, thiserror::Error)]
pub enum DhtError {
    #[error("network error: {0}")]
    Net(#[from] overlord_kad_net::NetError),
    #[error("no bootstrap nodes available")]
    NoBootstrapNodes,
    #[error("bootstrap failed — no node responded")]
    BootstrapFailed,
    #[error("search timed out")]
    SearchTimeout,
    #[error("publish failed — no node accepted")]
    PublishFailed,
    #[error("routing error: {0}")]
    Routing(#[from] overlord_kad_routing::RoutingError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("nodes.dat parse error")]
    NodesDatParse,
    #[error("search semaphore closed")]
    SemaphoreClosed,
    #[error("unexpected packet type")]
    UnexpectedPacket,
}
