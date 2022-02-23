use crate::sql::engine::sql_transaction::SQLTransaction;
use crate::sql::engine::Catalog;
use crate::storage::mvcc::MVCCTransaction;

pub struct KVTransaction {
    txn: MVCCTransaction,
}

impl SQLTransaction for KVTransaction {
    fn id(&self) -> u64 {
        todo!()
    }

    fn mode(&self) -> crate::storage::mvcc::TransactionMode {
        todo!()
    }

    fn commit(self) -> crate::error::Result<()> {
        todo!()
    }

    fn rollback(self) -> crate::error::Result<()> {
        todo!()
    }

    fn create(
        &mut self,
        table: &str,
        row: crate::common::result::DataRow,
    ) -> crate::error::Result<()> {
        todo!()
    }

    fn delete(
        &mut self,
        table: &str,
        id: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<()> {
        todo!()
    }

    fn read(
        &self,
        table: &str,
        id: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<Option<crate::common::result::DataRow>> {
        todo!()
    }

    fn read_index(
        &self,
        table: &str,
        column: &str,
        value: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<std::collections::HashSet<crate::sql::schema::data_value::DataValue>>
    {
        todo!()
    }

    fn scan(
        &self,
        table: &str,
        filter: Option<crate::sql::plan::plan_expression::Expression>,
    ) -> crate::error::Result<crate::sql::engine::sql_transaction::Scan> {
        todo!()
    }

    fn scan_index(
        &self,
        table: &str,
        column: &str,
    ) -> crate::error::Result<crate::sql::engine::sql_transaction::IndexScan> {
        todo!()
    }

    fn update(
        &mut self,
        table: &str,
        id: &crate::sql::schema::data_value::DataValue,
        row: crate::common::result::DataRow,
    ) -> crate::error::Result<()> {
        todo!()
    }
}

impl Catalog for KVTransaction {
    fn read_table(
        &self,
        table: &str,
    ) -> crate::error::Result<Option<crate::sql::schema::table::Table>> {
        todo!()
    }
}
