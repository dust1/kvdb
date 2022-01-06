use crate::error::Result;
use crate::storage::kv::encoding::encode_bytes;
use crate::storage::Store;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::Read;
use std::sync::{Arc, RwLock};

/// a key-value store
pub struct KVStoreEngine {
    store: Arc<RwLock<Box<dyn Store>>>,
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

/// Same with kv.rs
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
