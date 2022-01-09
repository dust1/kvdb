use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::expression::Expression;
use crate::error::Result;
use crate::sql::types::{Row, Value};

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
    pub fn make_row(table: &Table, column: &[String], values: Vec<Value>) -> Result<Row> {
        todo!()
    }

    /// pads a row with default value where possible
    fn pad_row(table: &Table, mut row: Row) -> Result<Row> {
        todo!()
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