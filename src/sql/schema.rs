use sqlparser::ast::{ColumnDef, ObjectName};
use crate::error::{Error, Result};
use crate::sql::parser::translate::translate_object_name_to_string;

///TODO The catalog stores schema information
pub trait Catalog {

    /// Read a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;

    /// Read a table, and error if it does not exists
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist.", table)))
    }
}

/// a table schema
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl Table {
    pub fn new(name: ObjectName, columns: Vec<ColumnDef>) -> Result<Table> {
        let table_name = translate_object_name_to_string(&name)?.clone();
        Ok(Table {
            name: table_name,
            columns
        })
    }

}
