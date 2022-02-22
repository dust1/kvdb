use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use bincode::deserialize;
use bincode::serialize;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::common::keys::TransactionKey;
use crate::common::range::Range;
use crate::error::Error;
use crate::error::Result;
use crate::storage::Store;

/// An MVCC Transaction Mode
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    Snapshot { version: u64 },
}

/// A versioned snapshot, containing visiblity information about concurrent transaction
struct Snapshot {
    /// the version(i.e. Transaction ID) that the snapshot belongs to
    version: u64,
    /// the set of transaction IDs that were active at the start of the transactions,
    /// and thus should be invisible to the snapshot
    invisible: HashSet<u64>,
}

/// An MVCC Transaction
pub struct MVCCTransaction {
    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<RwLock<Box<dyn Store>>>,
    /// tansaction id
    id: u64,
    /// transaction mode
    mode: TransactionMode,
    /// the shapshot that the transaction is running in
    snapshot: Snapshot,
}

impl Snapshot {
    // take a new snapshot with given version
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut snapshot = Self {
            version,
            invisible: HashSet::new(),
        };
        // scan all active transaction with transaction id less than version
        let mut scan = session.scan(Range::from(
            &TransactionKey::TxnActive(0).encode()..&TransactionKey::TxnActive(version).encode(),
        ));

        // save all active transaction id
        while let Some((key, _)) = scan.next().transpose()? {
            match TransactionKey::decode(&key)? {
                TransactionKey::TxnActive(id) => snapshot.invisible.insert(id),
                k => return Err(Error::Internal(format!("Expexted TxnActive, got {}", k))),
            };
        }
        std::mem::drop(scan);
        // just save active transaction ids
        session.set(
            &TransactionKey::TxnSnapshot(version).encode(),
            serialize(&snapshot.invisible)?,
        )?;
        Ok(snapshot)
    }

    /// read a snapshot with given version
    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&TransactionKey::TxnSnapshot(version).encode())? {
            Some(ref v) => {
                let invisible = deserialize(v)?;
                Ok(Self { version, invisible })
            }
            None => {
                return Err(Error::Value(format!(
                    "snapshot version not found {}",
                    version
                )))
            }
        }
    }
}

impl MVCCTransaction {
    /// begin a new transaction in the given mode
    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: TransactionMode) -> Result<Self> {
        let session = store.write()?;
        let id: u64 = match session.get(&TransactionKey::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        session.set(&TransactionKey::TxnNext.encode(), serialize(&(id + 1))?)?;
        session.set(&TransactionKey::TxnActive(id).encode(), serialize(&mode)?)?;

        let mut snapshot = Snapshot::take(&mut session, id)?;
        std::mem::drop(session);
        if let TransactionMode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?;
        }

        Ok(Self {
            store,
            id,
            mode,
            snapshot,
        })
    }

    /// resume a transaction with given id
    pub fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read()?;
        let mode = match session.get(&TransactionKey::TxnActive(id).encode())? {
            Some(ref v) => deserialize(v)?,
            None => {
                return Err(Error::Value(format!(
                    "No active transaction with Id: {}",
                    id
                )))
            }
        };
        // transaction id is not necessarily equal to snapshot version
        let snapshot = match &mode {
            TransactionMode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };
        std::mem::drop(session);
        Ok(Self {
            store,
            id,
            mode,
            snapshot,
        })
    }
}
