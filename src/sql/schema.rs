use crate::error::{Error, Result};
use crate::sql::parser::translate::translate_object_name_to_string;
use crate::sql::types::{DataType, Value};
use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ObjectName};

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is a primary key
    pub primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub index: bool,
}

impl Table {
    pub fn new(name: ObjectName, columns: Vec<ColumnDef>) -> Result<Table> {
        let table_name = translate_object_name_to_string(&name)?;
        let columns = columns.iter().map(Column::from_column_def).collect::<Vec<_>>();
        Ok(Table { name: table_name, columns })
    }
}

impl Column {
    pub fn from_column_def(column: &ColumnDef) -> Self {
        todo!()
    }
}
