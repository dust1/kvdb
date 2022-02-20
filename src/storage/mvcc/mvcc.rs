use std::sync::{Arc, RwLock};

use serde::{Serialize, Deserialize};
use serde_derive::{Serialize, Deserialize};

use crate::{storage::{Store, range::Range}, error::Result};

use super::{transaction::{Transaction, TransactionMode}, keys::Key};


/// MVCC Status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

pub struct MVCC {
    store: Arc<RwLock<Box<dyn Store>>>
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self { store: self.store.clone() }
    }
}

impl MVCC {

    /// Create a new MVCC K/V Store with given K/V store for storage
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store))
        }
    }

    /// begin a new transaction in read-write mode
    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), TransactionMode::ReadWrite)
    }

    /// begin a new transaction in the given mode
    pub fn begin_with_mode(&self, mode: TransactionMode) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), mode)
    }

    /// resume a transaction with the given ID
    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id)
    }

    /// return engine status
    pub fn status(&self) -> Result<Status> {
        let store = self.store.read()?;

        // get the latest used transaction ID
        let txns:u64 = match store.get(&Key::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        } - 1;

        // get the count with active transaction
        let txns_active:u64 = store.scan(
            Range::from(
                Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
            )
        ).try_fold(0, |count, r| r.map(|_| count + 1))?;

        Ok(Status {
            txns,
            txns_active,
            storage: store.to_string()
        })
    }

    /// fetch an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        todo!()
    }

    /// set an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }

}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
