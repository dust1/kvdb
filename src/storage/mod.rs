mod btree;
pub mod kv;
pub mod memory;
pub mod range;

#[cfg(test)]
mod test;

use crate::error::Result;
use crate::storage::range::{Range, Scan};
use std::fmt::Display;

/// A key/value store.
pub trait Store: Display + Send + Sync {
    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&self, range: Range) -> Scan;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}
