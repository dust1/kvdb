use std::iter::Peekable;

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
    pub fn new(_scan: KVScan, _snapshot: Snapshot) -> Self {
        todo!()
    }
}

impl Iterator for KeyRangeScan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for KeyRangeScan {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
