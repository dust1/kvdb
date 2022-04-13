use std::collections::HashMap;

use crate::common::result::DataRow;
use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::InsertPlan;
use crate::sql::schema::table::Table;
use crate::sql::sql_executor::KVExecutor;

pub struct InsertExec {
    plan: InsertPlan,
}

impl InsertExec {
    pub fn new(plan: InsertPlan) -> Box<Self> {
        Box::new(Self { plan })
    }

    /// pad a row with default values where possible
    fn pad_row(table: &Table, mut rows_data: DataRow) -> Result<DataRow> {
        // just padding default value in the end
        for column in table.columns.iter().skip(rows_data.len()) {
            if let Some(default_value) = &column.default {
                rows_data.push(default_value.clone());
            } else {
                return Err(Error::Value(format!(
                    "No default value of column {}",
                    column.name
                )));
            }
        }
        Ok(rows_data)
    }

    // builds a row from a set of column names and values,
    // padding it with default value
    fn make_row(table: &Table, insert_columns: &[String], rows_data: DataRow) -> Result<DataRow> {
        if insert_columns.len() != rows_data.len() {
            return Err(Error::Value("Column and value counts do not match".into()));
        }
        let mut insert = HashMap::new();
        // iterator column and value with insert
        for (column, value) in insert_columns.iter().zip(rows_data.into_iter()) {
            table.get_column(column)?;
            if insert.insert(column.clone(), value).is_some() {
                // column was inserted
                return Err(Error::Value(format!(
                    "The column {} given multiple time",
                    column
                )));
            }
        }

        let mut rows = DataRow::new();
        for column in table.columns.iter() {
            if let Some(v) = insert.get(&column.name) {
                rows.push(v.clone());
            } else if let Some(default_v) = &column.default {
                rows.push(default_v.clone());
            } else {
                return Err(Error::Value(format!(
                    "No default value of column {}",
                    column.name
                )));
            }
        }
        Ok(rows)
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for InsertExec {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        let plan = self.plan;
        let table = txn.must_read_table(&plan.table_name)?;
        let mut count = 0;
        for expression in plan.rows {
            let mut row = expression
                .into_iter()
                .map(|expr| expr.evaluate(None))
                .collect::<Result<Vec<_>>>()?;
            if plan.columns.is_empty() {
                // INSERT INTO table (1, 'name');
                // should pad default row
                row = Self::pad_row(&table, row)?;
            } else {
                // INSERT INTO table (num, name) VALUES (1, 'name');
                row = Self::make_row(&table, &plan.columns, row)?;
            }
            txn.create(&plan.table_name, row)?;
            count += 1;
        }

        Ok(ResultSet::Create { count })
    }
}
