use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;

use super::data_value::DataValue;
use super::table_column::TableColumn;
use crate::error::Result;

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

    /// return the table column with the column name
    pub fn get_column(&self, _name: &str) -> Result<&TableColumn> {
        todo!()
    }

    /// return the primary key value of a row
    pub fn get_row_key(&self, _row: &[DataValue]) -> Result<DataValue> {
        todo!()
    }
}
