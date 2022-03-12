use std::iter::Peekable;

use bincode::deserialize;

use crate::common::keys::TransactionKey;
use crate::error::Error;
use crate::error::Result;
use crate::storage::mvcc::Snapshot;

/// a Key/Value iterator
pub type KVScan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

/// a key range scan
pub struct KeyRangeScan {
    scan: Peekable<KVScan>,
    next_back_seen: Option<Vec<u8>>,
}

impl KeyRangeScan {
    pub fn new(mut scan: KVScan, snapshot: Snapshot) -> Self {
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match TransactionKey::decode(&k)? {
                TransactionKey::Record(_, version) if !snapshot.is_visable(version) => Ok(None),
                TransactionKey::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self {
            scan: scan.peekable(),
            next_back_seen: None,
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // we usually only need the last version key
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // we also can be got the last version key
        // the next_back_seen init None, it alway the last version
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for KeyRangeScan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for KeyRangeScan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}
