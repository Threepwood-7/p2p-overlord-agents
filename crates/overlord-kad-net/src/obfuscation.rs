use overlord_kad_proto::constants::OP_KADEMLIAHEADER;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;

// NOTE: eMule uses MD5 for key derivation. Using SHA256 here for now.
// Verify against live network in Phase 3 and update if needed.
// See KADKAD.md §9 Obfuscation for details.

fn rc4(key: &[u8], data: &mut [u8]) {
    if key.is_empty() || data.is_empty() {
        return;
    }
    let klen = key.len();
    let mut s = [0u8; 256];
    for (i, value) in s.iter_mut().enumerate() {
        *value = i as u8;
    }
    let mut j = 0usize;
    for i in 0..256usize {
        j = (j + s[i] as usize + key[i % klen] as usize) & 0xFF;
        s.swap(i, j);
    }
    let mut i = 0usize;
    let mut j = 0usize;
    for byte in data.iter_mut() {
        i = (i + 1) & 0xFF;
        j = (j + s[i] as usize) & 0xFF;
        s.swap(i, j);
        *byte ^= s[(s[i] as usize + s[j] as usize) & 0xFF];
    }
}

fn derive_key(seed0: u8, seed1: u8, udp_key: u32) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update([seed0, seed1]);
    hasher.update(udp_key.to_le_bytes());
    let hash = hasher.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&hash[..16]);
    key
}

pub struct ObfuscationLayer {
    our_key: u32,
    enabled: bool,
    peer_keys: Mutex<HashMap<SocketAddr, u32>>,
}

impl ObfuscationLayer {
    pub fn new(our_key: u32, enabled: bool) -> Self {
        Self {
            our_key,
            enabled,
            peer_keys: Mutex::new(HashMap::new()),
        }
    }

    /// Register a peer's UDP key (learned from their HELLO packet).
    pub fn register_peer_key(&self, addr: SocketAddr, key: u32) {
        let mut guard = self.peer_keys.lock().unwrap();
        guard.insert(addr, key);
    }

    /// Get a peer's known UDP key.
    pub fn peer_key(&self, addr: SocketAddr) -> Option<u32> {
        let guard = self.peer_keys.lock().unwrap();
        guard.get(&addr).copied()
    }

    /// Our own UDP key (sent to peers in HELLO).
    pub fn our_key(&self) -> u32 {
        self.our_key
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Encrypt `plaintext` for sending to `addr`.
    /// If obfuscation is disabled or peer key is unknown: return plaintext as-is.
    /// Otherwise: prepend obfuscation header and RC4-encrypt the payload.
    pub fn encrypt(&self, addr: SocketAddr, plaintext: &[u8]) -> Vec<u8> {
        if !self.enabled {
            return plaintext.to_vec();
        }

        let peer_key = match self.peer_key(addr) {
            Some(k) => k,
            None => return plaintext.to_vec(),
        };

        let mut rng = rand::thread_rng();

        // Generate r0, r1 — neither can be 0xE4 (OP_KADEMLIAHEADER)
        let r0 = loop {
            let b: u8 = rng.r#gen();
            if b != OP_KADEMLIAHEADER {
                break b;
            }
        };
        let r1 = loop {
            let b: u8 = rng.r#gen();
            if b != OP_KADEMLIAHEADER {
                break b;
            }
        };

        // Build header: [r0, r1, 0xD1, padding_len=0x00]
        let header = [r0, r1, 0xD1u8, 0x00u8];

        // Derive key and encrypt
        let key = derive_key(r0, r1, peer_key);
        let mut payload = plaintext.to_vec();
        rc4(&key, &mut payload);

        let mut result = Vec::with_capacity(header.len() + payload.len());
        result.extend_from_slice(&header);
        result.extend_from_slice(&payload);
        result
    }

    /// Attempt to decrypt an incoming packet.
    /// Returns (data, was_obfuscated).
    /// If buf[2] != 0xD1 or decryption yields invalid header: returns (buf.to_vec(), false).
    pub fn decrypt(&self, buf: &[u8]) -> (Vec<u8>, bool) {
        if buf.len() < 4 {
            return (buf.to_vec(), false);
        }

        if buf[2] != 0xD1 {
            return (buf.to_vec(), false);
        }

        let padding = buf[3] as usize;
        let payload_start = 4 + padding;

        if buf.len() <= payload_start {
            return (buf.to_vec(), false);
        }

        let key = derive_key(buf[0], buf[1], self.our_key);
        let mut decrypted = buf[payload_start..].to_vec();
        rc4(&key, &mut decrypted);

        if !decrypted.is_empty() && decrypted[0] == OP_KADEMLIAHEADER {
            (decrypted, true)
        } else {
            (buf.to_vec(), false)
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn peer_addr() -> SocketAddr {
        "127.0.0.1:4672".parse().unwrap()
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let peer_key = 0xCAFE_BABEu32;
        let peer_addr = peer_addr();

        // sender_layer: obfuscation for the sender (doesn't need our_key, it encrypts outbound).
        // It registers the peer's UDP key so it can encrypt toward peer_addr.
        // The receiver decrypts using our_key = peer_key (matching what the sender encrypted with).
        let sender_layer = ObfuscationLayer::new(0, true);
        sender_layer.register_peer_key(peer_addr, peer_key);

        // receiver: uses our_key = peer_key so it can derive the same RC4 key.
        let receiver = ObfuscationLayer::new(peer_key, true);

        // A plaintext Kad2 packet (starts with OP_KADEMLIAHEADER 0xE4)
        let plaintext = vec![OP_KADEMLIAHEADER, 0x60]; // Ping packet

        let encrypted = sender_layer.encrypt(peer_addr, &plaintext);
        assert_ne!(encrypted, plaintext);
        assert!(encrypted.len() > 4);
        assert_eq!(encrypted[2], 0xD1);

        let (decrypted, was_obfuscated) = receiver.decrypt(&encrypted);
        assert!(was_obfuscated);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_plaintext_unchanged() {
        let layer = ObfuscationLayer::new(0xABCD_1234, true);
        // Plaintext packet (no 0xD1 at byte 2)
        let plain = vec![OP_KADEMLIAHEADER, 0x60, 0x00, 0x00];
        let (out, was_obfuscated) = layer.decrypt(&plain);
        assert!(!was_obfuscated);
        assert_eq!(out, plain);
    }

    #[test]
    fn test_decrypt_garbage_unchanged() {
        let layer = ObfuscationLayer::new(0x1234, true);
        // Has 0xD1 at byte 2 but decryption won't yield OP_KADEMLIAHEADER
        let garbage = vec![0x01u8, 0x02, 0xD1, 0x00, 0xFF, 0xFE, 0xFD];
        let (out, was_obfuscated) = layer.decrypt(&garbage);
        assert!(!was_obfuscated);
        assert_eq!(out, garbage);
    }

    #[test]
    fn test_disabled_encrypt_returns_plaintext() {
        let layer = ObfuscationLayer::new(0xDEAD, false);
        let peer_addr = peer_addr();
        layer.register_peer_key(peer_addr, 0xBEEF);
        let plaintext = vec![OP_KADEMLIAHEADER, 0x60];
        let out = layer.encrypt(peer_addr, &plaintext);
        assert_eq!(out, plaintext);
    }

    #[test]
    fn test_encrypt_no_peer_key_returns_plaintext() {
        let layer = ObfuscationLayer::new(0xDEAD, true);
        let peer_addr = peer_addr();
        // No peer key registered
        let plaintext = vec![OP_KADEMLIAHEADER, 0x61];
        let out = layer.encrypt(peer_addr, &plaintext);
        assert_eq!(out, plaintext);
    }
}
