use std::iter::once;

use crate::sql::data::DataResult;
use crate::sql::data::DataRow;
use crate::sql::session::Catalog;
use crate::sql::sql_executor::KVExecutor;

pub struct NothingExec;

impl NothingExec {
    pub fn new() -> Box<Self> {
        Box::new(Self)
    }
}

impl<C: Catalog> KVExecutor<C> for NothingExec {
    fn execute(self: Box<Self>, ctx: &mut C) -> crate::error::Result<crate::sql::data::DataResult> {
        Ok(DataResult::Query {
            columns: vec![],
            rows: Box::new(once(Ok(DataRow::new()))),
        })
    }
}
