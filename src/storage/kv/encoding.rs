use crate::error::{Error, Result};
use std::vec::Vec;

/// Encodes a u64. Simply uses the big-endian form, which preserves order. Does not attempt to
/// compress it, for now.
pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// table the bytes first byte, and remove it in original array
pub fn take_byte(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(Error::Internal("unexpected end of bytes".into()));
    }
    let b = bytes[0];
    *bytes = &bytes[1..];
    Ok(b)
}

/// decode a byte vector from a slice and shortens the slice.
/// link encode_bytes()
pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoded = Vec::new();
    let mut l = 0;
    while l < bytes.len() {
        match &bytes[l] {
            0x00 if l < bytes.len() - 1 && 0xff.eq(&bytes[l + 1]) => {
                decoded.push(0x00);
                l += 2;
            }
            0x00 if l < bytes.len() - 1 && 0x00.eq(&bytes[l + 1]) => {
                *bytes = &bytes[l + 2..];
                break;
            }
            b => {
                decoded.push(*b);
                l += 1;
            }
        }
    }

    Ok(decoded)
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
