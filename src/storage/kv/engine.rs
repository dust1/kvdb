use crate::error::{Error, Result};
use crate::storage::kv::encoding::encode_bytes;
use crate::storage::Store;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::Read;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock};
use crate::storage::range::Range;

/// a key-value store
pub struct KVStoreEngine {
    store: Arc<RwLock<Box<dyn Store>>>,
}

/// a key range
pub struct Scan {
    /// augmented KV store iterator
    scan: Peekable<super::super::range::Scan>,
}

/// store engine key
enum Key<'a> {
    /// A record for a key/version pair.
    Record(Cow<'a, [u8]>),
}

impl Clone for KVStoreEngine {
    fn clone(&self) -> Self {
        Self { store: self.store.clone() }
    }
}

impl KVStoreEngine {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self { store: Arc::new(RwLock::new(store)) }
    }

    /// fetch a key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        let value = session.get(&Key::Record(key.into()).encode())?;
        if let Some(vec) = value {
            return deserialize(&vec);
        }
        Ok(None)
    }

    /// sets a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// delete a key
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// scan keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::super::range::Scan> {
        if prefix.is_empty() {
            return Err(Error::Value(format!(
                "Scan prefix cannot be empty".into()
            )));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        // the reason for this should be in:
        // https://activesphere.com/blog/2018/08/17/order-preserving-serialization
        // or in Catalog::create
        // e.g. AB00DD
        // start : AB00FFDD00
        // end   : AC0100DE01
        for i in (0..end.len()).rev() {
            match end[i] {
                // it won't happen
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                },
                v => {
                    end[i] = v + 1;
                }
            }
        }

        self.scan(start..end)
    }

    /// scan a key range
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::super::range::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into()).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into()).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into()))
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into()).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into()).encode()),
            Bound::Unbounded => Bound::Unbounded
        };
        let scan = self.store.read()?.scan(Range::from((start, end)));
        Ok(Box::new(Scan::new(scan)))
    }

    /// write a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        let mut session = self.store.write()?;
        let key = Key::Record(key.into()).encode();
        session.set(&key, serialize(&value)?)
    }
}

impl<'a> Key<'a> {
    fn encode(self) -> Vec<u8> {
        match self {
            Self::Record(key) => [&[0xff][..], &encode_bytes(&key)].concat(),
        }
    }
}

impl Scan {
    fn new(mut scan: super::super::range::Scan) -> Self {
        todo!()
    }
}

/// Same with kv.rs
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
