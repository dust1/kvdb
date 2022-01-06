use crate::error::{Error, Result};
use crate::sql::types::{DataType, Value};
use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption, ObjectName};

///TODO The catalog stores schema information
pub trait Catalog {
    /// Create a new Tablej
    fn create_table(&mut self, table: Table) -> Result<()>;

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
        let table_name = name.to_string();
        let columns = columns.iter().map(Column::from_column_def).collect::<Vec<_>>();
        Ok(Table { name: table_name, columns })
    }

    /// Validates the table schema
    pub fn validate(&self) -> Result<()> {
        todo!()
    }
}

impl Column {
    pub fn from_column_def(column_def: &ColumnDef) -> Self {
        let mut column = Self {
            name: column_def.name.to_string(),
            datatype: DataType::new(&column_def.data_type),
            primary_key: false,
            nullable: false,
            default: None,
            unique: false,
            references: None,
            index: false,
        };

        for column_d in &column_def.options {
            match &column_d.option {
                ColumnOption::Null => column.nullable = true,
                ColumnOption::NotNull => column.nullable = false,
                ColumnOption::Default(expr) => column.default = Some(Value::from_expr(expr)),
                ColumnOption::Unique { is_primary: true } => {
                    column.unique = true;
                    column.primary_key = true;
                    column.index = true;
                }
                ColumnOption::Unique { .. } => column.unique = true,
                ColumnOption::ForeignKey { foreign_table, .. } => {
                    column.references = Some(foreign_table.to_string())
                }
                _ => {}
            }
        }
        column
    }
}
