use std::sync::Arc;

use super::sql_engine::SQLEngine;
use super::SQLTransaction;
use crate::error::Result;
use crate::sql::data::DataResult;
use crate::sql::parser::KVParser;
use crate::sql::plan_parser::PlanParser;
use crate::storage::mvcc::TransactionMode;

/// A SQL-Session
pub struct SQLSession<E: SQLEngine> {
    /// SQL-Engine
    engine: E,
    /// the current session transaction
    txn: Option<E::Transaction>,
}

impl<E: SQLEngine> SQLSession<E> {
    /// execute a query, managing transaction status for the session
    pub fn execute(&self, query: &str) -> Result<DataResult> {
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
