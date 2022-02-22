use std::borrow::Cow;
use std::sync::Arc;
use std::sync::RwLock;

use bincode::deserialize;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::transaction::MVCCTransaction;
use super::transaction::TransactionMode;
use crate::common::keys::TransactionKey;
use crate::common::range::Range;
use crate::error::Result;
use crate::storage::Store;

/// MVCC Status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

pub struct MVCC {
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl MVCC {
    /// Create a new MVCC K/V Store with given K/V store for storage
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
    }

    /// begin a new transaction in read-write mode
    pub fn begin(&self) -> Result<MVCCTransaction> {
        MVCCTransaction::begin(self.store.clone(), TransactionMode::ReadWrite)
    }

    /// begin a new transaction in the given mode
    pub fn begin_with_mode(&self, mode: TransactionMode) -> Result<MVCCTransaction> {
        MVCCTransaction::begin(self.store.clone(), mode)
    }

    /// resume a transaction with the given ID
    pub fn resume(&self, id: u64) -> Result<MVCCTransaction> {
        MVCCTransaction::resume(self.store.clone(), id)
    }

    /// return engine status
    pub fn status(&self) -> Result<Status> {
        let store = self.store.read()?;

        // get the latest used transaction ID
        let txns: u64 = match store.get(&TransactionKey::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        } - 1;

        // get the count with active transaction
        let txns_active: u64 = store
            .scan(Range::from(
                TransactionKey::TxnActive(0).encode()
                    ..TransactionKey::TxnActive(std::u64::MAX).encode(),
            ))
            .try_fold(0, |count, r| r.map(|_| count + 1))?;

        Ok(Status {
            txns,
            txns_active,
            storage: store.to_string(),
        })
    }

    /// fetch an unversioned metadata value
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        session.get(&TransactionKey::Metadata(Cow::from(key)).encode())
    }

    /// set an unversioned metadata value
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let session = self.store.read()?;
        let k = TransactionKey::Metadata(Cow::from(key)).encode();
        session.set(key, value)
    }
}
