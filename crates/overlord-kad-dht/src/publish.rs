use crate::error::DhtError;
use crate::traversal::{TraversalConfig, TraversalContact, TraversalKind, run_traversal};
use overlord_kad_net::RpcManager;
use overlord_kad_proto::constants::STORE_TIMEOUT_SECS;
use overlord_kad_proto::{
    Ed2kHash, KadPacket, NodeId, Tag,
    constants::K,
    opcode,
    packet::{PublishEntry, PublishKeyReq, PublishNotesReq, PublishSourceReq},
};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(STORE_TIMEOUT_SECS);
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const PUBLISH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Publish a keyword→file mapping.
/// Returns the number of nodes that acknowledged.
pub async fn publish_keyword(
    rpc: &RpcManager,
    routing_table: &tokio::sync::Mutex<overlord_kad_routing::RoutingTable>,
    keyword_hash: NodeId,
    file_hash: Ed2kHash,
    tags: Vec<Tag>,
) -> Result<usize, DhtError> {
    let target = keyword_hash;
    let initial = get_initial(routing_table, &target).await;

    let traversal = run_traversal(
        rpc,
        initial,
        TraversalConfig {
            target,
            search_kind: TraversalKind::FindNode,
            timeout: PUBLISH_TIMEOUT,
            query_timeout: QUERY_TIMEOUT,
            phase2_fanout: K,
            result_tx: None,
        },
    )
    .await;

    if traversal.closest.is_empty() {
        return Err(DhtError::PublishFailed);
    }

    let entry = PublishEntry {
        hash: file_hash,
        tags,
    };
    let packet = KadPacket::PublishKeyReq(PublishKeyReq {
        target,
        entries: vec![entry],
    });

    let mut acks = 0usize;
    for contact in traversal.closest.iter().take(K) {
        match rpc
            .request(
                contact.addr,
                &packet,
                opcode::PUBLISH_RES,
                PUBLISH_RESPONSE_TIMEOUT,
            )
            .await
        {
            Ok(_) => acks += 1,
            Err(e) => tracing::debug!("publish_keyword ack failed from {}: {}", contact.addr, e),
        }
    }

    Ok(acks)
}

/// Publish source availability for a file.
pub async fn publish_source(
    rpc: &RpcManager,
    routing_table: &tokio::sync::Mutex<overlord_kad_routing::RoutingTable>,
    file_hash: Ed2kHash,
    tags: Vec<Tag>,
) -> Result<usize, DhtError> {
    let target = NodeId::from_bytes(file_hash.0);
    let initial = get_initial(routing_table, &target).await;

    let traversal = run_traversal(
        rpc,
        initial,
        TraversalConfig {
            target,
            search_kind: TraversalKind::FindNode,
            timeout: PUBLISH_TIMEOUT,
            query_timeout: QUERY_TIMEOUT,
            phase2_fanout: K,
            result_tx: None,
        },
    )
    .await;

    if traversal.closest.is_empty() {
        return Err(DhtError::PublishFailed);
    }

    let packet = KadPacket::PublishSourceReq(PublishSourceReq {
        target,
        source_hash: file_hash,
        tags,
    });

    let mut acks = 0usize;
    for contact in traversal.closest.iter().take(K) {
        match rpc
            .request(
                contact.addr,
                &packet,
                opcode::PUBLISH_RES,
                PUBLISH_RESPONSE_TIMEOUT,
            )
            .await
        {
            Ok(_) => acks += 1,
            Err(e) => tracing::debug!("publish_source ack failed from {}: {}", contact.addr, e),
        }
    }

    Ok(acks)
}

/// Publish a note/rating for a file.
pub async fn publish_notes(
    rpc: &RpcManager,
    routing_table: &tokio::sync::Mutex<overlord_kad_routing::RoutingTable>,
    file_hash: Ed2kHash,
    note_hash: Ed2kHash,
    tags: Vec<Tag>,
) -> Result<usize, DhtError> {
    let target = NodeId::from_bytes(file_hash.0);
    let initial = get_initial(routing_table, &target).await;

    let traversal = run_traversal(
        rpc,
        initial,
        TraversalConfig {
            target,
            search_kind: TraversalKind::FindNode,
            timeout: PUBLISH_TIMEOUT,
            query_timeout: QUERY_TIMEOUT,
            phase2_fanout: K,
            result_tx: None,
        },
    )
    .await;

    if traversal.closest.is_empty() {
        return Err(DhtError::PublishFailed);
    }

    let packet = KadPacket::PublishNotesReq(PublishNotesReq {
        target,
        note_hash,
        tags,
    });

    let mut acks = 0usize;
    for contact in traversal.closest.iter().take(K) {
        match rpc
            .request(
                contact.addr,
                &packet,
                opcode::PUBLISH_RES,
                PUBLISH_RESPONSE_TIMEOUT,
            )
            .await
        {
            Ok(_) => acks += 1,
            Err(e) => tracing::debug!("publish_notes ack failed from {}: {}", contact.addr, e),
        }
    }

    Ok(acks)
}

async fn get_initial(
    routing_table: &tokio::sync::Mutex<overlord_kad_routing::RoutingTable>,
    target: &NodeId,
) -> Vec<TraversalContact> {
    let rt = routing_table.lock().await;
    rt.get_closest(target, K)
        .into_iter()
        .map(|c| TraversalContact {
            id: c.id,
            addr: SocketAddr::new(IpAddr::V4(c.ip), c.udp_port),
            version: c.kad_version,
        })
        .collect()
}
