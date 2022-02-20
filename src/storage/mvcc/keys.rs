use std::borrow::Cow;

use crate::error::Result;


/// MVCC keys
#[derive(Debug)]
pub enum Key<'a> {
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
}

impl<'a> Key<'a>  {

    pub fn encode(self) -> Vec<u8> {
        todo!()
    }

    pub fn decode(byte: &mut [u8]) -> Result<Self> {
        todo!()
    }
    
}