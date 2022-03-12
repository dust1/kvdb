use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;

use super::data_value::DataValue;
use super::table_column::TableColumn;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;

pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;

/// a table schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<TableColumn>,
}

impl Table {
    pub fn new(name: ObjectName, columns: Vec<ColumnDef>) -> Result<Table> {
        let table_name = name.to_string();
        let columns = columns
            .iter()
            .map(TableColumn::try_form)
            .collect::<Vec<_>>();
        Ok(Table {
            name: table_name,
            columns,
        })
    }

    /// get index base on the column name
    pub fn get_column_index(&self, column_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == column_name)
            .map(|(i, _)| i)
            .ok_or_else(|| {
                Error::Value(format!(
                    "Column {} not found in table {}",
                    column_name, self.name
                ))
            })
    }

    /// return the table column with the column name
    pub fn get_column(&self, name: &str) -> Result<&TableColumn> {
        self.columns.iter().find(|c| c.name == name).ok_or_else(|| {
            Error::Value(format!("Column {} not found in table {}", name, self.name))
        })
    }

    /// return the primary key value of a row
    pub fn get_row_key(&self, row: &[DataValue]) -> Result<DataValue> {
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| Error::Value("Table can not found primary key".into()))?,
        )
        .cloned()
        .ok_or_else(|| Error::Value("primary key can not found for row".into()))
    }

    /// return the primaryt key of this table
    pub fn get_primary_key(&self) -> Result<&TableColumn> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in Table {}", self.name)))
    }

    /// validate the table row schema
    pub fn validate_row(&self, row: &[DataValue], txn: &mut dyn SQLTransaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!(
                "Invalid row size for table {}",
                self.name
            )));
        }
        let primary_key = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &primary_key, value, txn)?;
        }

        Ok(())
    }

    /// validate the table schema
    pub fn validate(&self, txn: &mut dyn SQLTransaction) -> Result<()> {
        // check columns count
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns.", self.name)));
        }

        // check primary key number, should 1
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Value(format!(
                    "Table {} has no primary_key.",
                    self.name
                )))
            }
            n => {
                return Err(Error::Value(format!(
                    "Table {} has {} primary key, should 1",
                    self.name, n
                )))
            }
        }

        for column in &self.columns {
            column.validate(self, txn)?;
        }
        Ok(())
    }
}
