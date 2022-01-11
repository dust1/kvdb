use crate::error::{Error, Result};
use crate::sql::engine::Scan;
use crate::sql::schema::{Catalog, Table, Tables};
use crate::sql::types::expression::Expression;
use crate::sql::types::{Row, Value};
use crate::storage::kv::encoding::encode_string;
use crate::storage::kv::engine::KVStoreEngine;
use crate::storage::Store;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

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
}

impl KV {
    pub fn new(kv: KVStoreEngine) -> Self {
        Self { kv }
    }
}

impl Catalog for KV {
    fn create_table(&mut self, table: Table) -> crate::error::Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }

        table.validate(self)?;
        self.kv.set(&Key::Table(Some((&table.name).into())).encode(), serialize(&table)?)
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
        self.kv.delete(&Key::Table(Some(table.name.into())).encode())
    }

    fn read_table(&self, table: &str) -> crate::error::Result<Option<Table>> {
        self.kv.get(&Key::Table(Some(table.into())).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan_table(&self) -> Result<Tables> {
        todo!()
    }

    fn scan(&self, _table: &str, _filter: Option<Expression>) -> Result<Scan> {
        todo!()
    }

    fn create(&mut self, _table: &str, _row: Row) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, _table: &str, _id: &Value) -> Result<()> {
        todo!()
    }
}

impl<'a> Key<'a> {
    /// encode the key as a byte vector
    fn encode(self) -> Vec<u8> {
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
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
