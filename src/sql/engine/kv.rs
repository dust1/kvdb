use std::borrow::Cow;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use crate::error::{Error, Result}:
use crate::sql::schema::{Catalog, Table};
use crate::storage::kv::KVStoreEngine;
use crate::storage::Store;

/// A SQL engine based KVStoreEngine
pub struct KV {
    /// access control, only allow the super-level directory to be
    /// able to access, avoid the user to call outside the program
    pub(super) kv: KVStoreEngine
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
        Self {
            kv
        }
    }

}

impl Catalog for KV {
    fn create_table(&mut self, table: Table) -> crate::error::Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }

        table.validate()?;
        self.kv.set(&Key::Table(Some((&table.name).into())).encode(), serialize(&table)?)
    }

    fn read_table(&self, table: &str) -> crate::error::Result<Option<Table>> {
        todo!()
    }
}

impl<'a> Key<'a> {

    /// encode the key as a byte vector
    fn encode(self) -> Vec<u8> {
        todo!()
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