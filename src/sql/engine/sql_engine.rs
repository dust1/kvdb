use super::sql_session::SQLSession;
use super::sql_transaction::SQLTransaction;
use crate::error::Result;
use crate::storage::mvcc::TransactionMode;

/// A SQL-Engine
pub trait SQLEngine: Clone {
    /// defined a association type, this is a Transaction type
    type Transaction: SQLTransaction;

    /// begine a sql transcation
    fn begin(&self, mode: TransactionMode) -> Result<Self::Transaction>;

    /// resume a sql transaction with given txn id
    fn resume(&self, id: u64) -> Result<Option<Self::Transaction>>;

    /// begine a sql session for executing individual statements
    fn session(&self) -> Result<SQLSession<Self>> {
        Ok(SQLSession {
            engine: self.clone(),
            txn: None,
        })
    }
}
