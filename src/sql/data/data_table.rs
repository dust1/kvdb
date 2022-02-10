use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::{ObjectName, ColumnDef, ColumnOption};

use crate::{error::{Result, Error}, sql::engine::kv::KV};

use super::{data_type::DataType, data_value::Value};


pub type Row = Vec<Value>;

pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

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
    /// The table which is referenced by this foreign key, link other table's primary key
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
    pub fn validate(&self, kv: &KV) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("table {} has no columns", self.name)));
        }
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => return Err(Error::Value(format!("no primary key in table {}", self.name))),
            _ => return Err(Error::Value(format!("Multiple primary keys in table {}", self.name))),
        }
        for column in &self.columns {
            column.validate(self, kv)?;
        }

        Ok(())
    }

    /// validate a row in this table
    pub fn validate_row(&self, _row: &[Value], _kv: &mut KV) -> Result<()> {
        todo!()
    }

    // fetchs a column index by name
    pub fn get_column_index(&self, _name: &str) -> Result<usize> {
        todo!()
    }

    /// returns the primary key column of the table
    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    /// return the primary key value in data row
    pub fn get_row_key(&self, row: &Row) -> Result<Value> {
        let index = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in {}", self.name)))?;
        row.get(index)
            .cloned()
            .ok_or_else(|| Error::Value("Primary key value not found for row".into()))
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

    /// validates the column schema
    pub fn validate(&self, table: &Table, kv: &KV) -> Result<()> {
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!("Primary key {} can not be nullable", self.name)));
        }
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!("Primary key {} should be unique", self.name)));
        }

        // validate default value
        if let Some(default) = &self.default {
            if let Some(datatype) = default.datatype() {
                if datatype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, datatype, self.datatype
                    )));
                }
            } else if !self.nullable {
                return Err(Error::Value(format!(
                    "Can not use NULL as default value for nun-nullable column {}",
                    self.name
                )));
            }
        } else if self.nullable {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }

        // validate reference
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = kv.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table {} reference by column {} does not exist.",
                    reference, self.name
                )));
            };

            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can not reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }

        Ok(())
    }
}