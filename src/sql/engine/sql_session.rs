use super::sql_engine::SQLEngine;
use super::SQLTransaction;
use crate::common::result::ResultSet;
use crate::error::Result;
use crate::sql::plan_parser::PlanParser;
use crate::storage::mvcc::TransactionMode;

/// A SQL-Session
pub struct SQLSession<E: SQLEngine> {
    /// SQL-Engine
    pub engine: E,
    /// the current session transaction
    pub txn: Option<E::Transaction>,
}

impl<E: SQLEngine + 'static> SQLSession<E> {
    /// execute a query, managing transaction status for the session
    pub fn execute(&self, query: &str) -> Result<ResultSet> {
        let mut txn = self.engine.begin(TransactionMode::ReadWrite)?;
        match PlanParser::parser(query, &mut txn)?.execute(&mut txn) {
            Ok(result) => {
                txn.commit()?;
                Ok(result)
            }
            Err(e) => {
                txn.rollback()?;
                Err(e)
            }
        }
    }
}
