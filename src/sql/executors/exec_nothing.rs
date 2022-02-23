use std::iter::once;

use crate::common::result::DataRow;
use crate::common::result::ResultSet;
use crate::sql::engine::SQLTransaction;
use crate::sql::sql_executor::KVExecutor;

pub struct NothingExec;

impl NothingExec {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<T: SQLTransaction> KVExecutor<T> for NothingExec {
    fn execute(self: Box<Self>, ctx: &mut T) -> crate::error::Result<ResultSet> {
        Ok(ResultSet::Query {
            columns: vec![],
            rows: Box::new(once(Ok(DataRow::new()))),
        })
    }
}
