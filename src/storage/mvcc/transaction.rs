use std::collections::HashSet;
use std::ops::Bound;
use std::ops::RangeBounds;
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
use crate::common::scan::KVScan;
use crate::common::scan::KeyRangeScan;
use crate::error::Error;
use crate::error::Result;
use crate::storage::Store;

/// An MVCC Transaction Mode
#[derive(Clone, Debug, Copy, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone)]
pub struct Snapshot {
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

    /// check whether the given version is visible in this snapshot
    fn is_visable(&self, version: u64) -> bool {
        version < self.version && !self.invisible.contains(&version)
    }
}

impl MVCCTransaction {
    /// begin a new transaction in the given mode
    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: TransactionMode) -> Result<Self> {
        let mut session = store.write()?;
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

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn mode(&self) -> TransactionMode {
        self.mode
    }

    pub fn commit(&self) -> Result<()> {
        let mut session = self.store.write()?;
        // remove Txnactive flag with transaction id
        session.delete(&TransactionKey::TxnActive(self.id).encode())?;
        session.flush()
    }

    pub fn rollback(&self) -> Result<()> {
        let mut session = self.store.write()?;
        if self.mode.mutable() {
            let mut rollback = Vec::new();
            let mut scan = session.scan(Range::from(
                TransactionKey::TxnUpdate(self.id, vec![].into()).encode()
                    ..TransactionKey::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match TransactionKey::decode(&key)? {
                    TransactionKey::TxnUpdate(_, updated_key) => {
                        rollback.push(updated_key.into_owned())
                    }
                    k => return Err(Error::Internal(format!("Excepted TxnUpdate, got {}", k))),
                }
                rollback.push(key);
            }
            std::mem::drop(scan);
            for key in rollback.into_iter() {
                session.delete(&key)?;
            }
        }
        session.delete(&TransactionKey::TxnActive(self.id).encode())
    }

    /// sacn a key range
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<KVScan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => {
                Bound::Excluded(TransactionKey::Record(k.into(), std::u64::MAX).encode())
            }
            Bound::Included(k) => Bound::Included(TransactionKey::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(TransactionKey::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(TransactionKey::Record(k.into(), 0).encode()),
            Bound::Included(k) => {
                Bound::Included(TransactionKey::Record(k.into(), std::u64::MAX).encode())
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read()?.scan(Range::from((start, end)));
        Ok(Box::new(KeyRangeScan::new(scan, self.snapshot.clone())))
    }

    /// scan key under a given prefix
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KVScan> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    continue;
                }
            }
        }
        self.scan(start..end)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        let mut scan = session
            .scan(Range::from(
                TransactionKey::Record(key.into(), 0).encode()
                    ..=TransactionKey::Record(key.into(), self.id).encode(),
            ))
            .rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match TransactionKey::decode(&k)? {
                TransactionKey::Record(_, id) => {
                    if self.snapshot.is_visable(id) {
                        return Ok(Some(v));
                    }
                }
                k => return Err(Error::Internal(format!("Excepted Txn::Record {}", k))),
            }
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// delete. set the value to None
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    pub fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::ReadOnly);
        }
        let mut session = self.store.write()?;

        // find the min tranaction ID with TxnActive
        let min = self
            .snapshot
            .invisible
            .iter()
            .min()
            .cloned()
            .unwrap_or(self.id + 1);
        // scan other transaction records with the same key
        let mut scan = session
            .scan(Range::from(
                TransactionKey::Record(key.into(), min).encode()
                    ..=TransactionKey::Record(key.into(), std::u64::MAX).encode(),
            ))
            .rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match TransactionKey::decode(&k)? {
                TransactionKey::Record(_, version) => {
                    // if this transaction can visable it
                    // transaction conflict
                    if !self.snapshot.is_visable(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {}", k))),
            }
        }
        std::mem::drop(scan);

        // write the key and update record
        let key = TransactionKey::Record(key.into(), self.id).encode();
        let update = TransactionKey::TxnUpdate(self.id, (&key).into()).encode();
        session.set(&update, vec![])?;
        session.set(&key, serialize(&value)?)
    }
}

impl TransactionMode {
    pub fn mutable(&self) -> bool {
        match self {
            TransactionMode::ReadWrite => true,
            _ => false,
        }
    }
}
