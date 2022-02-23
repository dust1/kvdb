use std::borrow::Cow;
use std::fmt::Display;

use crate::error::Result;

/// MVCC keys
#[derive(Debug)]
pub enum TransactionKey<'a> {
    /// the next avaliable transaction ID, Used when starting new Transaction
    TxnNext,
    /// active transaction markers, containing the mode. Used to detect concurrent txns, and to resume
    TxnActive(u64),
    /// transaction snapshot, containing concurrent active transactions at start of transaction
    TxnSnapshot(u64),
    /// update marker for a transaction ID and key, used for rollback
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// A record for a key/version pair
    Record(Cow<'a, [u8]>, u64),
    /// arbitray unversioned metadata
    Metadata(Cow<'a, [u8]>),
    /// use decode
    Decoder,
}

/// Data key
#[derive(Debug)]
pub enum DataKey<'a> {
    ONE(Cow<'a, [u8]>),
    Decode,
}

impl<'a> TransactionKey<'a> {
    pub fn encode(self) -> Vec<u8> {
        todo!()
    }

    pub fn decode(_bytes: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl<'a> Display for TransactionKey<'a> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'a> DataKey<'a> {}
