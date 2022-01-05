use std::sync::{Arc, RwLock};
use crate::storage::Store;
use crate::error::Result;

/// a key-value store
pub struct KVStoreEngine {
    store: Arc<RwLock<Box<dyn Store>>>
}

impl Clone for KVStoreEngine {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone()
        }
    }
}

impl KVStoreEngine {

    pub fn new(store: Box<dyn Store>) -> Self {
        Self { store: Arc::new(RwLock::new(store)) }
    }

    /// sets a key
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// write a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        todo!()
    }

}