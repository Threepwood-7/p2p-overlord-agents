use std::fmt;
use std::str::FromStr;

use binrw::{BinRead, BinWrite};

use crate::error::ProtoError;

/// A 128-bit Kademlia node identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BinRead, BinWrite)]
#[brw(little)]
pub struct NodeId(pub [u8; 16]);

impl NodeId {
    pub const ZERO: NodeId = NodeId([0u8; 16]);

    pub fn from_bytes(b: [u8; 16]) -> Self {
        NodeId(b)
    }

    /// XOR distance between two node IDs.
    pub fn distance(&self, other: &Self) -> NodeId {
        let mut result = [0u8; 16];
        for i in 0..16 {
            result[i] = self.0[i] ^ other.0[i];
        }
        NodeId(result)
    }

    /// Index of the highest set bit in the XOR distance (0 = MSB of byte 0).
    /// Returns None if XOR is all zeros (same node).
    pub fn distance_exp(&self, other: &Self) -> Option<u32> {
        let xor = self.distance(other);
        for byte_idx in 0..16usize {
            let b = xor.0[byte_idx];
            if b != 0 {
                // highest set bit in this byte
                let bit_in_byte = 7 - b.leading_zeros();
                return Some((byte_idx as u32) * 8 + (7 - bit_in_byte));
            }
        }
        None
    }

    /// Returns the bit at position `pos` (0 = MSB of byte 0).
    pub fn bit(&self, pos: u32) -> bool {
        let byte_idx = (pos / 8) as usize;
        let bit_idx = 7 - (pos % 8); // MSB = bit 7
        if byte_idx >= 16 {
            return false;
        }
        (self.0[byte_idx] >> bit_idx) & 1 == 1
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl FromStr for NodeId {
    type Err = ProtoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 32 {
            return Err(ProtoError::InvalidNodeId);
        }
        let mut bytes = [0u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| ProtoError::InvalidNodeId)?;
        }
        Ok(NodeId(bytes))
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_id(hex: &str) -> NodeId {
        hex.parse().unwrap()
    }

    #[test]
    fn test_zero() {
        assert_eq!(NodeId::ZERO.0, [0u8; 16]);
    }

    #[test]
    fn test_distance_zero() {
        let a = make_id("0102030405060708090a0b0c0d0e0f10");
        let dist = a.distance(&a);
        assert_eq!(dist, NodeId::ZERO);
    }

    #[test]
    fn test_distance_xor() {
        let a = NodeId::from_bytes([0xFF; 16]);
        let b = NodeId::from_bytes([0x0F; 16]);
        let d = a.distance(&b);
        assert_eq!(d.0[0], 0xF0);
    }

    #[test]
    fn test_distance_exp_none_for_equal() {
        let a = make_id("aabbccddeeff00112233445566778899");
        assert_eq!(a.distance_exp(&a), None);
    }

    #[test]
    fn test_distance_exp_msb() {
        // XOR = 0x80 00 ... => highest bit at position 0
        let a = NodeId::from_bytes([0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let b = NodeId::ZERO;
        assert_eq!(a.distance_exp(&b), Some(0));
    }

    #[test]
    fn test_distance_exp_second_byte() {
        // XOR = 0x00 0x80 ... => highest bit at position 8
        let a = NodeId::from_bytes([0x00, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let b = NodeId::ZERO;
        assert_eq!(a.distance_exp(&b), Some(8));
    }

    #[test]
    fn test_distance_exp_lsb() {
        // XOR = 0x00...01 => bit at position 127
        let a = NodeId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01]);
        let b = NodeId::ZERO;
        assert_eq!(a.distance_exp(&b), Some(127));
    }

    #[test]
    fn test_bit() {
        // byte 0 = 0x80 => bit 0 = 1, bit 1 = 0
        let a = NodeId::from_bytes([0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(a.bit(0));
        assert!(!a.bit(1));
        assert!(!a.bit(8));
    }

    #[test]
    fn test_bit_second_byte() {
        // byte 1 = 0x01 => bit 15 = 1
        let a = NodeId::from_bytes([0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(!a.bit(8));
        assert!(a.bit(15));
    }

    #[test]
    fn test_display() {
        let a = NodeId::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        assert_eq!(format!("{}", a), "0102030405060708090a0b0c0d0e0f10");
    }

    #[test]
    fn test_from_str_roundtrip() {
        let hex = "0102030405060708090a0b0c0d0e0f10";
        let id: NodeId = hex.parse().unwrap();
        assert_eq!(format!("{}", id), hex);
    }

    #[test]
    fn test_from_str_invalid() {
        assert!("short".parse::<NodeId>().is_err());
        assert!(
            "gggggggggggggggggggggggggggggggg"
                .parse::<NodeId>()
                .is_err()
        );
    }

    #[test]
    fn test_ord() {
        let a = NodeId::from_bytes([0x00; 16]);
        let b = NodeId::from_bytes([0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn test_binrw_roundtrip() {
        use binrw::{BinRead, BinWrite};
        use std::io::Cursor;

        let id = NodeId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let mut buf = Cursor::new(Vec::new());
        id.write_le(&mut buf).unwrap();
        buf.set_position(0);
        let id2 = NodeId::read_le(&mut buf).unwrap();
        assert_eq!(id, id2);
    }
}
