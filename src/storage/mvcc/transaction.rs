use std::{sync::{Arc, RwLock}, collections::HashSet};

use crate::{storage::Store, error::Result};


/// An MVCC Transaction Mode
pub enum TransactionMode {
    /// A read-write transaction
    ReadWrite,
    /// A read-only tranasction
    ReadOnly,
    /// A read-only transaction running in a snapshot of a given version
    /// 
    /// The version must refer to a committed transaction ID. Any changes visible to the original
    /// transaction will be visible in the snapshot (i.e. transactions that had not committed before
    /// the snapshot transaction started will not be visible, even though they have a lower version).
    Snapshot {
        version: u64
    }
}

/// A versioned snapshot, containing visiblity information about concurrent transaction
struct Snapshot {
    /// the version(i.e. Transaction ID) that the snapshot belongs to
    version: u64,
    /// the set of transaction IDs that were active at the start of the transactions,
    /// and thus should be invisible to the snapshot
    invisible: HashSet<u64>
}

pub struct Transaction {
    store: Arc<RwLock<Box<dyn Store>>>,
    id: u64,
    mode: TransactionMode,
}

impl Transaction {

    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: TransactionMode) -> Result<Self> {
        todo!()
    }

    pub fn resume(store:Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        todo!()
    }
    
}