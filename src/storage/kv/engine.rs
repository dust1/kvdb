use std::borrow::Cow;
use std::iter::Peekable;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::RwLock;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::storage::kv::encoding::encode_bytes;
use crate::storage::kv::encoding::take_byte;
use crate::storage::kv::encoding::take_bytes;
use crate::storage::range::Range;
use crate::storage::Store;

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
#[derive(Debug)]
enum Key<'a> {
    /// A record for a key/version pair.
    Record(Cow<'a, [u8]>),
}

impl Clone for KVStoreEngine {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl KVStoreEngine {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
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
            return Err(Error::Value("Scan prefix cannot be empty".into()));
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
                }
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
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into()).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into()).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into()).encode()),
            Bound::Unbounded => Bound::Unbounded,
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

    fn decode(mut bytes: &[u8]) -> Result<Self> {
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0xff => Self::Record(take_bytes(bytes)?.into()),
            b => return Err(Error::Internal(format!("Unknown key prefix byte:{:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "Unexpceted data remaining at end of key".into(),
            ));
        }
        Ok(key)
    }
}

impl Scan {
    fn new(mut scan: super::super::range::Scan) -> Self {
        scan = Box::new(scan.flat_map(|r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(key) => Ok(Some((key.into_owned(), v))),
            })
            .transpose()
        }));
        Self {
            scan: scan.peekable(),
        }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return non-deleted items.
            if let Some(value) = deserialize(&value)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

/// Same with kv.rs
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
