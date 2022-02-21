use std::collections::HashSet;

use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::Executor;
use crate::sql::execution::ResultSet;
use crate::sql::plan::planners::Expression;
use crate::sql::schema::Catalog;
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::sql::types::Value;

/// An INSERT Executor
pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

// A UPDATE executor
pub struct Update<C: Catalog> {
    table: String,
    source: Box<dyn Executor<C>>,
    expressions: Vec<(usize, Expression)>,
}

pub struct Delete<C: Catalog> {
    table: String,
    source: Box<dyn Executor<C>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table,
            columns,
            rows,
        })
    }

    /// build a row from a set of column names and values,
    /// padding it with default value
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(Error::Value(
                "INSERT column size not equals VALUES size".into(),
            ));
        }
        let mut row = Row::new();
        let mut index = 0;
        for column in table.columns.iter() {
            match columns.get(index) {
                Some(col) if column.name.eq(col) => {
                    let push_value = &values[index];
                    row.push(push_value.clone());
                    index += 1;
                }
                _ if column.default.is_some() => {
                    if let Some(default) = &column.default {
                        row.push(default.clone());
                    }
                }
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
                }
                (Some(default), None) => {
                    // append in the end
                    row.push(default.clone());
                    row_index += 1;
                }
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
                }
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
            // let mut row = expressions
            //     .into_iter()
            //     .map(|expr| expr.evaluate(None))
            //     .collect::<Result<_>>()?;
            let mut row = vec![];
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
        Ok(ResultSet::Create { count })
    }
}

impl<C: Catalog> Update<C> {
    pub fn new(
        table_name: String,
        source: Box<dyn Executor<C>>,
        expressions: Vec<(usize, Expression)>,
    ) -> Box<Self> {
        Box::new(Self {
            table: table_name,
            source,
            expressions,
        })
    }
}

impl<C: Catalog> Executor<C> for Update<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        match self.source.execute(catalog)? {
            ResultSet::Query { mut rows, .. } => {
                let table = catalog.must_read_table(&self.table)?;
                let mut update = HashSet::new();
                while let Some(row) = rows.next().transpose()? {
                    let id = table.get_row_key(&row)?;
                    if update.contains(&id) {
                        continue;
                    }
                    let mut new = row.clone();
                    for (index, expression) in &self.expressions {
                        // new[*index] = expression.evaluate(Some(&row))?;
                    }

                    catalog.update(&table.name, &id, new)?;
                    update.insert(id);
                }
                Ok(ResultSet::Update {
                    count: update.len() as u64,
                })
            }
            r => Err(Error::Internal(format!("Unexpected response: {:?}", r))),
        }
    }
}

impl<C: Catalog> Delete<C> {
    pub fn new(table_name: String, source: Box<dyn Executor<C>>) -> Box<Self> {
        Box::new(Self {
            table: table_name,
            source,
        })
    }
}

impl<C: Catalog> Executor<C> for Delete<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        match self.source.execute(catalog)? {
            ResultSet::Query { mut rows, .. } => {
                let table = catalog.must_read_table(&self.table)?;
                let mut deleted = HashSet::new();
                while let Some(row) = rows.next().transpose()? {
                    let id = table.get_row_key(&row)?;
                    if deleted.contains(&id) {
                        continue;
                    }
                    catalog.delete(&table.name, &id)?;
                    deleted.insert(id);
                }
                Ok(ResultSet::Delete {
                    count: deleted.len() as u64,
                })
            }
            r => Err(Error::Internal(format!("Unexpected response: {:?}", r))),
        }
    }
}
