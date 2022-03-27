use super::sql_engine::SQLEngine;
use super::SQLTransaction;
use crate::common::result::ResultSet;
use crate::error::Error;
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

    /// runs a closure in the session's transaction, or a new transaction if none is active
    pub fn with_txn<R, F>(&mut self, mode: TransactionMode, f: F) -> Result<R>
    where F: FnOnce(&mut E::Transaction) -> Result<R> {
        if let Some(ref mut txn) = self.txn {
            if !txn.mode().satisfies(&mode) {
                return Err(Error::Value(
                    "The operation cannot run in the current transaction".into(),
                ));
            }
            return f(txn);
        }

        // a new transaction
        let mut txn = self.engine.begin(mode)?;
        let result = f(&mut txn);
        txn.rollback()?;
        result
    }
}
