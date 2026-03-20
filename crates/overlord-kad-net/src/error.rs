use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Proto(#[from] overlord_kad_proto::ProtoError),
    #[error("request timed out after {secs}s to {addr}")]
    Timeout { addr: SocketAddr, secs: u64 },
    #[error("channel closed")]
    ChannelClosed,
    #[error("rate limited")]
    RateLimited,
    #[error("packet too short ({len} bytes)")]
    PacketTooShort { len: usize },
    #[error("peer {0} is flood-blocked")]
    FloodBlocked(std::net::IpAddr),
}
