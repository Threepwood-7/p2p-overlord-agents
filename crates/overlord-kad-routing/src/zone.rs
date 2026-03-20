use overlord_kad_proto::{KBASE, NodeId};

use crate::bin::RoutingBin;
use crate::contact::Contact;
use crate::error::RoutingError;

// ── ZoneContent ───────────────────────────────────────────────────────────────

enum ZoneContent {
    Leaf(RoutingBin),
    Branch {
        /// Owns nodes where bit[depth] == 0
        left: Box<RoutingZone>,
        /// Owns nodes where bit[depth] == 1
        right: Box<RoutingZone>,
    },
}

// ── RoutingZone ───────────────────────────────────────────────────────────────

pub struct RoutingZone {
    depth: u32,
    content: ZoneContent,
}

impl RoutingZone {
    /// Create a new root zone.
    pub fn new_root() -> Self {
        RoutingZone {
            depth: 0,
            content: ZoneContent::Leaf(RoutingBin::new()),
        }
    }

    fn new_leaf(depth: u32) -> Self {
        RoutingZone {
            depth,
            content: ZoneContent::Leaf(RoutingBin::new()),
        }
    }

    /// Try to add a contact.
    ///
    /// - `own_id`: our node ID (needed for split decision)
    /// - `total_contacts`: current total in the whole table
    /// - `max_table_size`: configured maximum
    /// - `on_own_side`: whether own_id would be routed into this zone
    ///
    /// Returns `Ok(true)` = added new, `Ok(false)` = updated existing, `Err` = rejected.
    pub fn add(
        &mut self,
        contact: Contact,
        own_id: &NodeId,
        total_contacts: usize,
        max_table_size: usize,
        on_own_side: bool,
    ) -> Result<bool, RoutingError> {
        match &mut self.content {
            ZoneContent::Leaf(_) => {
                // Try adding to the leaf bin.
                let result = {
                    let ZoneContent::Leaf(bin) = &mut self.content else {
                        unreachable!()
                    };
                    bin.try_add(contact.clone())
                };

                match result {
                    Ok(added) => Ok(added),
                    Err(RoutingError::TableFull { .. }) => {
                        // Attempt to split.
                        if self.can_split(on_own_side, total_contacts, max_table_size) {
                            self.split(own_id);
                            // Retry after split.
                            self.add(contact, own_id, total_contacts, max_table_size, on_own_side)
                        } else {
                            Err(RoutingError::TableFull {
                                max: max_table_size,
                            })
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            ZoneContent::Branch { left, right } => {
                let bit = contact.id.bit(self.depth);
                let own_bit = own_id.bit(self.depth);
                if bit {
                    // contact goes right
                    let child_on_own_side = on_own_side && own_bit;
                    right.add(
                        contact,
                        own_id,
                        total_contacts,
                        max_table_size,
                        child_on_own_side,
                    )
                } else {
                    // contact goes left
                    let child_on_own_side = on_own_side && !own_bit;
                    left.add(
                        contact,
                        own_id,
                        total_contacts,
                        max_table_size,
                        child_on_own_side,
                    )
                }
            }
        }
    }

    /// Collect up to `n` contacts closest to `target` by XOR distance.
    ///
    /// For simplicity: recurse into all leaf bins, add contacts to result.
    /// The caller (RoutingTable) sorts by XOR distance.
    pub fn get_closest(&self, target: &NodeId, n: usize, result: &mut Vec<Contact>) {
        match &self.content {
            ZoneContent::Leaf(bin) => {
                for c in bin.iter() {
                    result.push(c.clone());
                }
            }
            ZoneContent::Branch { left, right } => {
                // Recurse into the side closer to target first (optimization, but correctness
                // doesn't depend on order since we collect all and sort).
                let bit = target.bit(self.depth);
                if bit {
                    right.get_closest(target, n, result);
                    if result.len() < n {
                        left.get_closest(target, n, result);
                    }
                } else {
                    left.get_closest(target, n, result);
                    if result.len() < n {
                        right.get_closest(target, n, result);
                    }
                }
            }
        }
    }

    /// Remove a contact by ID. Returns the removed contact if found.
    pub fn remove(&mut self, id: &NodeId) -> Option<Contact> {
        match &mut self.content {
            ZoneContent::Leaf(bin) => bin.remove(id),
            ZoneContent::Branch { left, right } => {
                let bit = id.bit(self.depth);
                if bit {
                    right.remove(id)
                } else {
                    left.remove(id)
                }
            }
        }
    }

    /// Find a contact by ID.
    pub fn get(&self, id: &NodeId) -> Option<&Contact> {
        match &self.content {
            ZoneContent::Leaf(bin) => bin.iter().find(|c| &c.id == id),
            ZoneContent::Branch { left, right } => {
                let bit = id.bit(self.depth);
                if bit { right.get(id) } else { left.get(id) }
            }
        }
    }

    /// Count total contacts in this zone and all children.
    pub fn count(&self) -> usize {
        match &self.content {
            ZoneContent::Leaf(bin) => bin.len(),
            ZoneContent::Branch { left, right } => left.count() + right.count(),
        }
    }

    /// Whether this zone may be split.
    fn can_split(&self, on_own_side: bool, total_contacts: usize, max_table_size: usize) -> bool {
        // Condition 1: depth < 127
        if self.depth >= 127 {
            return false;
        }
        // Condition 2: total < max
        if total_contacts >= max_table_size {
            return false;
        }
        // Condition 3: depth < KBASE OR on own side
        if self.depth < KBASE as u32 || on_own_side {
            return true;
        }
        false
    }

    /// Split a leaf into two child zones, redistributing contacts.
    fn split(&mut self, own_id: &NodeId) {
        let bin = match &mut self.content {
            ZoneContent::Leaf(b) => {
                let mut drained = RoutingBin::new();
                for c in b.drain() {
                    let _ = drained.try_add(c);
                }
                drained
            }
            ZoneContent::Branch { .. } => return, // already split
        };

        let depth = self.depth;
        let mut left = Box::new(RoutingZone::new_leaf(depth + 1));
        let mut right = Box::new(RoutingZone::new_leaf(depth + 1));

        for c in bin.iter() {
            let bit = c.id.bit(depth);
            let own_bit = own_id.bit(depth);
            if bit {
                let child_own_side = own_bit;
                // We use a large max_table_size here since we're just redistributing
                let _ = right.add(c.clone(), own_id, 0, usize::MAX, child_own_side);
            } else {
                let child_own_side = !own_bit;
                let _ = left.add(c.clone(), own_id, 0, usize::MAX, child_own_side);
            }
        }

        self.content = ZoneContent::Branch { left, right };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contact::Contact;
    use overlord_kad_proto::{K, NodeId};
    use std::net::Ipv4Addr;

    fn make_contact(id_bytes: [u8; 16], ip: &str) -> Contact {
        Contact::new(
            NodeId::from_bytes(id_bytes),
            ip.parse::<Ipv4Addr>().unwrap(),
            4672,
            4662,
            9,
        )
    }

    #[test]
    fn test_add_and_count() {
        let mut zone = RoutingZone::new_root();
        let own_id = NodeId::from_bytes([0x00; 16]);
        for i in 0..5u8 {
            let mut id = [0u8; 16];
            id[0] = i + 1;
            // Use distinct /24 subnets to avoid per-bin subnet limit
            let c = make_contact(id, &format!("1.{}.0.1", i + 1));
            zone.add(c, &own_id, i as usize, 1000, true).unwrap();
        }
        assert_eq!(zone.count(), 5);
    }

    #[test]
    fn test_get_closest_all_contacts() {
        let mut zone = RoutingZone::new_root();
        let own_id = NodeId::from_bytes([0x00; 16]);
        for i in 1..=5u8 {
            let mut id = [0u8; 16];
            id[0] = i;
            let c = make_contact(id, &format!("10.0.0.{}", i));
            zone.add(c, &own_id, i as usize, 1000, true).unwrap();
        }
        let mut result = Vec::new();
        let target = NodeId::from_bytes([0x00; 16]);
        zone.get_closest(&target, 10, &mut result);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_remove() {
        let mut zone = RoutingZone::new_root();
        let own_id = NodeId::from_bytes([0x00; 16]);
        let id = NodeId::from_bytes([0x01; 16]);
        let c = make_contact([0x01; 16], "1.1.1.1");
        zone.add(c, &own_id, 0, 1000, true).unwrap();
        assert_eq!(zone.count(), 1);
        let removed = zone.remove(&id);
        assert!(removed.is_some());
        assert_eq!(zone.count(), 0);
    }

    #[test]
    fn test_split_on_overflow() {
        // own_id starts with 0x00, so bit 0 = 0 → own side is left (bit=0)
        let own_id = NodeId::from_bytes([0x00; 16]);
        let mut zone = RoutingZone::new_root();

        // Add K contacts with bit 0 = 0 (same side as own_id)
        // Use distinct /24 subnets to avoid per-bin subnet limit
        for i in 0..K as u8 {
            let mut id = [0x00u8; 16];
            id[1] = i + 1; // all have bit 0 = 0
            let c = make_contact(id, &format!("1.{}.0.1", i + 1));
            zone.add(c, &own_id, i as usize, 10000, true).unwrap();
        }
        assert_eq!(zone.count(), K);

        // Adding one more on the same side should trigger a split
        let mut extra_id = [0x00u8; 16];
        extra_id[2] = 1;
        let extra = make_contact(extra_id, "1.99.0.1");
        let result = zone.add(extra, &own_id, K, 10000, true);
        // After split, it should succeed
        assert!(result.is_ok());
        assert_eq!(zone.count(), K + 1);
    }

    #[test]
    fn test_get_by_id() {
        let mut zone = RoutingZone::new_root();
        let own_id = NodeId::from_bytes([0x00; 16]);
        let id = NodeId::from_bytes([0xAB; 16]);
        let c = make_contact([0xAB; 16], "9.9.9.9");
        zone.add(c, &own_id, 0, 1000, true).unwrap();
        assert!(zone.get(&id).is_some());
        assert!(zone.get(&NodeId::ZERO).is_none());
    }
}
