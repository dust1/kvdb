use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::sql_transaction::SQLTransaction;
use crate::sql::engine::Catalog;
use crate::sql::schema::table::Table;
use crate::storage::mvcc::MVCCTransaction;
use crate::storage::mvcc::TransactionMode;

pub struct KVTransaction {
    txn: MVCCTransaction,
}

impl KVTransaction {
    pub fn new(txn: MVCCTransaction) -> Self {
        Self { txn }
    }
}

impl SQLTransaction for KVTransaction {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> crate::storage::mvcc::TransactionMode {
        self.txn.mode()
    }

    fn commit(self) -> crate::error::Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> crate::error::Result<()> {
        todo!()
    }

    fn create(
        &mut self,
        _table: &str,
        _row: crate::common::result::DataRow,
    ) -> crate::error::Result<()> {
        todo!()
    }

    fn delete(
        &mut self,
        _table: &str,
        _id: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<()> {
        todo!()
    }

    fn read(
        &self,
        _table: &str,
        _id: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<Option<crate::common::result::DataRow>> {
        todo!()
    }

    fn read_index(
        &self,
        _table: &str,
        _column: &str,
        _value: &crate::sql::schema::data_value::DataValue,
    ) -> crate::error::Result<std::collections::HashSet<crate::sql::schema::data_value::DataValue>>
    {
        todo!()
    }

    fn scan(
        &self,
        _table: &str,
        _filter: Option<crate::sql::plan::plan_expression::Expression>,
    ) -> crate::error::Result<crate::sql::engine::sql_transaction::Scan> {
        todo!()
    }

    fn scan_index(
        &self,
        _table: &str,
        _column: &str,
    ) -> crate::error::Result<crate::sql::engine::sql_transaction::IndexScan> {
        todo!()
    }

    fn update(
        &mut self,
        _table: &str,
        _id: &crate::sql::schema::data_value::DataValue,
        _row: crate::common::result::DataRow,
    ) -> crate::error::Result<()> {
        todo!()
    }
}

impl Catalog for KVTransaction {
    fn read_table(
        &self,
        _table: &str,
    ) -> crate::error::Result<Option<crate::sql::schema::table::Table>> {
        todo!()
    }

    fn create_table(&mut self, table: Table) -> crate::error::Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!(
                "Create table name {} already exists.",
                table.name
            )));
        }
        table.validate(self)?;

        todo!()
    }

    fn delete_table(&mut self, _table: &str) -> crate::error::Result<()> {
        todo!()
    }
}
