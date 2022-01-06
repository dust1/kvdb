use std::vec::Vec;

/// Encodes a u64. Simply uses the big-endian form, which preserves order. Does not attempt to
/// compress it, for now.
pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// Encodes a string. Simply converts to a byte vector and encodes that.
pub fn encode_string(str: &str) -> Vec<u8> {
    encode_bytes(str.as_bytes())
}

/// Encodes a byte vector. 0x00 is escaped as 0x00 0xff, and 0x00 0x00 is used as a terminator.
/// See: https://activesphere.com/blog/2018/08/17/order-preserving-serialization
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);

    /// if bytes has 0x00, replace it use [0x00, 0xff]
    /// in the end, append [0x00, 0x00]
    encoded.extend(
        bytes
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]),
    );
    encoded
}
