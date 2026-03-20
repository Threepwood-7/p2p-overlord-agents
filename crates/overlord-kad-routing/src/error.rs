#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("routing table is full (max {max} contacts)")]
    TableFull { max: usize },
    #[error("duplicate IP: {ip}")]
    IpLimitExceeded { ip: std::net::Ipv4Addr },
    #[error("subnet /{prefix} limit exceeded")]
    SubnetLimitExceeded { prefix: u8 },
}
