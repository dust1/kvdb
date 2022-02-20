use crate::sql::execution::Executor;
use crate::sql::execution::ResultSet;
use crate::sql::plan::planners::Expression;
use crate::sql::schema::Catalog;
use crate::sql::types::Column;
use crate::sql::types::Row;

/// An executor that produces a single empty row
pub struct Nothing;

/// a table scan executor
pub struct Scan {
    table: String,
    filter: Option<Expression>,
}

impl Nothing {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<C: Catalog> Executor<C> for Nothing {
    fn execute(self: Box<Self>, _: &mut C) -> crate::error::Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: Vec::new(),
            rows: Box::new(std::iter::once(Ok(Row::new()))),
        })
    }
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, filter })
    }
}

impl<C: Catalog> Executor<C> for Scan {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        let table = catalog.must_read_table(&self.table)?;
        Ok(ResultSet::Query {
            columns: table
                .columns
                .iter()
                .map(|c| Column {
                    name: Some(c.name.clone()),
                })
                .collect(),
            rows: Box::new(catalog.scan(&table.name, self.filter)?),
        })
    }
}
