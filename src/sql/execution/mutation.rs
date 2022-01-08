use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::Catalog;
use crate::sql::types::expression::Expression;

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
}

impl<C: Catalog> Executor<C> for Insert {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        todo!()
    }
}