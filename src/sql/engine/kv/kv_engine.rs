use super::kv_transaction::KVTransaction;
use crate::sql::engine::sql_engine::SQLEngine;
use crate::storage::mvcc::MVCC;

/// A SQL-Engine base KV Store
pub struct KVEngine {
    pub mvcc: MVCC,
}

impl Clone for KVEngine {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
        }
    }
}

impl SQLEngine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(
        &self,
        mode: crate::storage::mvcc::TransactionMode,
    ) -> crate::error::Result<Self::Transaction> {
        todo!()
    }

    fn resume(&self, id: u64) -> crate::error::Result<Option<Self::Transaction>> {
        todo!()
    }
}
