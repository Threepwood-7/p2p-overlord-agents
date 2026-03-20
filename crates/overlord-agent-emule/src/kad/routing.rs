use std::collections::BTreeMap;

use super::proto::NodeId;

#[derive(Debug, Clone)]
pub struct Contact {
    pub id: NodeId,
    pub address: String,
}

#[derive(Debug, Default)]
pub struct RoutingTable {
    contacts: BTreeMap<String, Contact>,
}

impl RoutingTable {
    pub fn add_contact(&mut self, address: String, id: NodeId) {
        self.contacts
            .insert(address.clone(), Contact { id, address });
    }

    pub fn len(&self) -> usize {
        self.contacts.len()
    }

    pub fn closest(&self, target: NodeId, limit: usize) -> Vec<Contact> {
        let mut contacts: Vec<Contact> = self.contacts.values().cloned().collect();
        contacts.sort_by_key(|contact| contact.id.distance(&target));
        contacts.truncate(limit);
        contacts
    }
}

#[cfg(test)]
mod tests {
    use super::RoutingTable;
    use crate::kad::proto::NodeId;

    #[test]
    fn routing_table_orders_by_distance() {
        let mut table = RoutingTable::default();
        table.add_contact("127.0.0.1:1".to_string(), NodeId::from_bytes([2; 16]));
        table.add_contact("127.0.0.1:2".to_string(), NodeId::from_bytes([1; 16]));
        let closest = table.closest(NodeId::ZERO, 1);
        assert_eq!(closest[0].address, "127.0.0.1:2");
    }
}
