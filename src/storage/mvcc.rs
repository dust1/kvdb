use std::sync::{Arc, Mutex};
use crate::storage::Store;

/// an mvcc-base transactional key-value store.
pub struct MVCC {
    /// the underlying KV store.
    store: Arc<Mutex<dyn Store>>,
}

/// an mvcc transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// A read-write transaction.
    ReadWrite,
    /// A read-only transaction.
    ReadOnly,
    /// A read-only transaction running in a snapshot of a given version.
    ///
    /// The version must refer to a committed transaction ID. Any changes visible to the original
    /// transaction will be visible in the snapshot (i.e. transactions that had not committed before
    /// the snapshot transaction started will not be visible, even though they have a lower version).
    Snapshot {
        version: u64
    }
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone()
        }
    }
}

