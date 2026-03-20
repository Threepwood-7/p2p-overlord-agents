use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tokio::net::UdpSocket;

#[derive(Clone)]
pub struct KadUdpEndpoint {
    socket: Arc<UdpSocket>,
}

impl KadUdpEndpoint {
    pub async fn bind(bind_addr: &str) -> Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
}
