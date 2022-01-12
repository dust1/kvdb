use crate::{
    error::{Error, Result},
    sql::types::Value,
};
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

/// encode f64, this preserves the natural numerical ordering,
/// with NaN at the end
pub fn encode_f64(f: f64) -> [u8; 8] {
    let mut bytes = f.to_be_bytes();
    if bytes[0] >> 7 & 1 == 0 {
        // if f > 0, flip sign bit to 1
        bytes[0] ^= 1 << 7;
    } else {
        // if f < 0, flip all bit
        bytes.iter_mut().for_each(|b| *b = !*b);
    }
    bytes
}

pub fn encode_boolean(b: bool) -> u8 {
    if b {
        0x01
    } else {
        0x00
    }
}

pub fn encode_i64(i: i64) -> [u8; 8] {
    let mut bytes = i.to_be_bytes();
    // flip sign bit
    bytes[0] ^= 1 << 7;
    bytes
}

/// encode value
pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00],
        Value::Boolean(b) => vec![0x01, encode_boolean(*b)],
        Value::Float(f) => [&[0x02][..], &encode_f64(*f)].concat(),
        Value::Integer(i) => [&[0x03][..], &encode_i64(*i)].concat(),
        Value::String(s) => [&[0x04][..], &encode_string(s)].concat(),
    }
}

/// Encodes a byte vector. 0x00 is escaped as 0x00 0xff, and 0x00 0x00 is used as a terminator.
/// See: https://activesphere.com/blog/2018/08/17/order-preserving-serialization
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);

    // if bytes has 0x00, replace it use [0x00, 0xff]
    // in the end, append [0x00, 0x00]
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
