use std::borrow::Cow;

use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::Scan;
use crate::sql::plan::planners::Expression;
use crate::sql::schema::Catalog;
use crate::sql::schema::Table;
use crate::sql::schema::Tables;
use crate::sql::types::Row;
use crate::sql::types::Value;
use crate::storage::kv::encoding::encode_string;
use crate::storage::kv::encoding::encode_value;
use crate::storage::kv::engine::KVStoreEngine;

/// A SQL engine based KVStoreEngine
pub struct KV {
    /// access control, only allow the super-level directory to be
    /// able to access, avoid the user to call outside the program
    pub(super) kv: KVStoreEngine,
}

impl Clone for KV {
    fn clone(&self) -> Self {
        KV::new(self.kv.clone())
    }
}

enum Key<'a> {
    /// a table schema key for the given table name
    Table(Option<Cow<'a, str>>),
    /// a key of a row identified by table name any row primary key
    Row(Cow<'a, str>, Option<Cow<'a, Value>>),
}

impl KV {
    pub fn new(kv: KVStoreEngine) -> Self {
        Self { kv }
    }

    /// read a table row by table name and primary id
    pub fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        let key = Key::Row(table.into(), Some(id.into())).encode();
        let value = self.kv.get(&key)?;
        value.map(|v| deserialize(&v)).transpose()
    }
}

impl Catalog for KV {
    fn create_table(&mut self, table: Table) -> crate::error::Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }

        table.validate(self)?;
        self.kv.set(
            &Key::Table(Some((&table.name).into())).encode(),
            serialize(&table)?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        let table = self.must_read_table(table)?;
        if let Some((name, columns)) = self.table_reference(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Table {}'s column {} was referenced this table {}",
                name, columns[0], &table.name
            )));
        }
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?;
        }
        self.kv
            .delete(&Key::Table(Some(table.name.into())).encode())
    }

    fn read_table(&self, table: &str) -> crate::error::Result<Option<Table>> {
        self.kv
            .get(&Key::Table(Some(table.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn scan_table(&self) -> Result<Tables> {
        Ok(Box::new(
            self.kv
                .scan_prefix(&Key::Table(None).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        ))
    }

    /// scan a table's row
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan> {
        // 1. read table
        let table = self.must_read_table(table)?;
        // Ok(Box::new(
        //     self.kv
        //         // scan by table name and no primary key
        //         .scan_prefix(&Key::Row((&table.name).into(), None).encode())?
        //         // deserialize value
        //         .map(|r| r.and_then(|(_, v)| deserialize(&v)))
        //         .filter_map(move |r| match r {
        //             Ok(row) => match &filter {
        //                 // filter value
        //                 Some(filter) => match filter.evaluate(Some(&row)) {
        //                     Ok(Value::Boolean(b)) if b => Some(Ok(row)),
        //                     Ok(Value::Boolean(_)) | Ok(Value::Null) => None,
        //                     Ok(v) => Some(Err(Error::Value(format!(
        //                         "Filter returnd {}, excepted boolean",
        //                         v
        //                     )))),
        //                     Err(err) => Some(Err(err)),
        //                 },
        //                 None => Some(Ok(row)),
        //             },
        //             err => Some(err),
        //         }),
        // ))
        todo!()
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        // TODO 1. validate_row
        // table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} alreary exists for table {}",
                id, table.name
            )));
        }

        // save data
        self.kv.set(
            &Key::Row(Cow::Borrowed(&table.name), Some(Cow::Borrowed(&id))).encode(),
            serialize(&row)?,
        )?;

        // TODO 2. update index

        Ok(())
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        // let table = self.must_read_table(table)?;
        // TODO 1. check reference
        // TODO 2. remove index
        self.kv
            .delete(&Key::Row(Cow::Borrowed(table), Some(Cow::Borrowed(&id))).encode())
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        self.kv.set(
            &Key::Row(Cow::Borrowed(table), Some(Cow::Borrowed(&id))).encode(),
            serialize(&row)?,
        )
    }
}

impl<'a> Key<'a> {
    /// encode the key as a byte vector
    fn encode(self) -> Vec<u8> {
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(&table)].concat(),
            Self::Row(table, Some(pk)) => {
                [&[0x03][..], &encode_string(&table), &encode_value(&pk)].concat()
            }
        }
    }
}

/// Serializes SQL metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes SQL metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
