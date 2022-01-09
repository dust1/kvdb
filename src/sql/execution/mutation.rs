use std::collections::HashMap;
use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::expression::Expression;
use crate::error::{Error, Result};
use crate::sql::types::{DataType, Row, Value};

/// An INSERT Executor
pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table,
            columns,
            rows
        })
    }

    /// build a row from a set of column names and values,
    /// padding it with default value
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(Error::Value("INSERT column size not equals VALUES size".into()));
        }
        let mut row = Row::new();
        let mut index = 0;
        for column in table.columns.iter() {
            match columns.get(index) {
                Some(col) if column.name.eq(col) => {
                    let push_value = &values[index];
                    row.push(push_value.clone());
                    index += 1;
                },
                _ if column.default.is_some() => {
                    if let Some(default) = &column.default {
                        row.push(default.clone());
                    }
                },
                _ => {
                    return Err(Error::Value(format!(
                        "Column {} not have default",
                        column.name
                    )));
                }
            }
        }
        Ok(row)
    }

    /// pads a row with default value where possible
    fn pad_row(table: &Table, mut row: Row) -> Result<Row> {
        let mut row_index = 0;
        for column in table.columns.iter() {
            match (&column.default, row.get(row_index)) {
                (None, None) => {
                    return Err(Error::Value(format!(
                        "The column {} not have default value",
                        column.name
                    )));
                },
                (Some(default), None) => {
                    // append in the end
                    row.push(default.clone());
                    row_index += 1;
                },
                (None, Some(value)) => {
                    if let Some(datatype) = value.datatype() {
                        if datatype != column.datatype {
                            return Err(Error::Value(format!(
                                "The column {} type is {}, but INSERT type is {}.",
                                column.name, column.datatype, datatype
                            )));
                        } else {
                            row_index += 1;
                        }
                    } else {
                        return Err(Error::Value(format!(
                            "Unknown type with INSERT value {}",
                            value
                        )));
                    }
                },
                (Some(default), Some(value)) => {
                    if let Some(datatype) = value.datatype() {
                        if datatype != column.datatype {
                            row.insert(row_index, default.clone());
                            row_index += 2;
                        } else {
                            row_index += 1;
                        }
                    } else {
                        return Err(Error::Value(format!(
                            "Unknown type with INSERT value {}",
                            value
                        )));
                    }
                }
            }
        }

        Ok(row)
    }
}

impl<C: Catalog> Executor<C> for Insert {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        let table = catalog.must_read_table(&self.table)?;
        let mut count = 0;
        for expressions in self.rows {
            // data source
            let mut row =
                expressions.into_iter().map(|expr| expr.evaluate(None)).collect::<Result<_>>()?;
            if self.columns.is_empty() {
                // e.g. INSERT INTO table VALUES (1, "name");
                // the column not specified
                row = Self::pad_row(&table, row)?;
            } else {
                // e.g. INSERT INTO table (id, name) VALUES (1, "name");
                row = Self::make_row(&table, &self.columns, row)?;
            }
            catalog.create(&table.name, row)?;
            count += 1;
        }
        Ok(ResultSet::Create {count})
    }
}