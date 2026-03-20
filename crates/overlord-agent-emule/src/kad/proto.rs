use md4::{Digest, Md4};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId([u8; 16]);

impl NodeId {
    pub const ZERO: Self = Self([0; 16]);

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub fn distance(&self, other: &Self) -> [u8; 16] {
        let mut out = [0u8; 16];
        for (index, slot) in out.iter_mut().enumerate() {
            *slot = self.0[index] ^ other.0[index];
        }
        out
    }
}

pub fn significant_keyword_words(query: &str) -> Vec<String> {
    let words: Vec<String> = query
        .split(|char: char| !char.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| word.to_lowercase())
        .filter(|word| word.len() >= 3)
        .collect();

    if words.is_empty() {
        vec![query.to_lowercase()]
    } else {
        words
    }
}

pub fn keyword_target(query: &str) -> NodeId {
    let first_word = significant_keyword_words(query)
        .into_iter()
        .next()
        .unwrap_or_else(|| query.to_lowercase());

    let mut hasher = Md4::new();
    hasher.update(first_word.as_bytes());
    let digest: [u8; 16] = hasher.finalize().into();

    let mut wire = [0u8; 16];
    for chunk in 0..4 {
        let base = chunk * 4;
        wire[base] = digest[base + 3];
        wire[base + 1] = digest[base + 2];
        wire[base + 2] = digest[base + 1];
        wire[base + 3] = digest[base];
    }
    NodeId::from_bytes(wire)
}

#[cfg(test)]
mod tests {
    use super::{keyword_target, significant_keyword_words};

    #[test]
    fn significant_words_ignore_short_tokens() {
        assert_eq!(
            significant_keyword_words("A torino x train"),
            vec!["torino".to_string(), "train".to_string()]
        );
    }

    #[test]
    fn keyword_target_is_stable() {
        assert_eq!(
            hex::encode(keyword_target("Torino Train").into_bytes()),
            "b2bc3aa39f375069e7c27eb83ce6baf3"
        );
    }
}
