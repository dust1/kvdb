use std::borrow::Cow;
use std::fmt::Display;

use crate::error::Error;
use crate::error::Result;
use crate::sql::schema::data_value::DataValue;

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
}

/// Data key
#[derive(Debug)]
pub enum SQLKey<'a> {
    /// A table schema key for the given table name
    Table(Option<Cow<'a, str>>),
    /// A key for an index entry
    Index(Cow<'a, str>, Cow<'a, str>, Option<Cow<'a, DataValue>>),
    /// A key for a row identified by table name and row primary key
    Row(Cow<'a, str>, Option<Cow<'a, DataValue>>),
}

impl<'a> TransactionKey<'a> {
    pub fn encode(self) -> Vec<u8> {
        use super::encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(txn_id) => [&[0x02][..], &encode_u64(txn_id)].concat(),
            Self::TxnSnapshot(txn_id) => [&[0x03][..], &encode_u64(txn_id)].concat(),
            Self::TxnUpdate(txn_id, key) => {
                [&[0x04][..], &encode_u64(txn_id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, txn_id) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(txn_id)].concat()
            }
        }
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        use super::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => return Err(Error::Value(format!("Unknow SQL key prefix {}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "Unexcepted data remaining at end of key".into(),
            ));
        }
        Ok(key)
    }
}

impl<'a> Display for TransactionKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxnNext => write!(f, "TransactionKey::TxnNext"),
            Self::TxnActive(txn_id) => write!(f, "TransactionKey::TxnActive({})", txn_id),
            Self::TxnSnapshot(txn_id) => write!(f, "TransactionKey::TxnSnapshot({})", txn_id),
            Self::TxnUpdate(txn_id, key) => {
                write!(f, "TransactionKey::TxnUpdate({}, {:?})", txn_id, key)
            }
            Self::Metadata(key) => write!(f, "TransactionKey::Metadata({:?})", key),
            Self::Record(key, txn_id) => write!(f, "TransactionKey::Record({:?}, {})", key, txn_id),
        }
    }
}

/// Table: 0x01
/// Index: 0x02
/// Row  : 0x03
impl<'a> SQLKey<'a> {
    pub fn encode(self) -> Vec<u8> {
        use super::encoding::*;
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
            Self::Index(table, column, None) => {
                [&[0x02][..], &encode_string(&table), &encode_string(&column)].concat()
            }
            Self::Index(table, column, Some(value)) => [
                &[0x02][..],
                &encode_string(&table),
                &encode_string(&column),
                &encode_data_value(&value),
            ]
            .concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(&table)].concat(),
            Self::Row(table, Some(pk)) => {
                [&[0x03][..], &encode_string(&table), &encode_data_value(&pk)].concat()
            }
        }
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        use super::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::Table(Some(take_string(bytes)?.into())),
            0x02 => Self::Index(
                take_string(bytes)?.into(),
                take_string(bytes)?.into(),
                Some(take_data_value(bytes)?.into()),
            ),
            0x03 => Self::Row(
                take_string(bytes)?.into(),
                Some(take_data_value(bytes)?.into()),
            ),
            b => return Err(Error::Value(format!("Unknow SQL key prefix {}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "Unexcepted data remaining at end of key".into(),
            ));
        }
        Ok(key)
    }
}

impl<'a> Display for SQLKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table(None) => write!(f, "SQLKey::Table(None)"),
            Self::Table(Some(table)) => write!(f, "SQLKey::Table({})", table),
            Self::Index(table, row, None) => write!(f, "SQLKey::Index({}, {}, None)", table, row),
            Self::Index(table, row, Some(value)) => {
                write!(f, "SQLKey:Index({}, {}, {})", table, row, value)
            }
            Self::Row(table, None) => write!(f, "SQLKey:Row({}, None)", table),
            Self::Row(table, Some(pk)) => write!(f, "SQLKey:Row({}, {})", table, pk),
        }
    }
}
