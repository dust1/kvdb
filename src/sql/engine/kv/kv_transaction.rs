use bincode::deserialize;
use bincode::serialize;


use crate::common::keys::SQLKey;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::sql_transaction::SQLTransaction;
use crate::sql::engine::Catalog;
use crate::sql::schema::table::Table;
use crate::sql::schema::table::Tables;
use crate::storage::mvcc::MVCCTransaction;


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
        table: &str,
    ) -> crate::error::Result<Option<crate::sql::schema::table::Table>> {
        let key = SQLKey::Table(Some(table.into()));
        if let Some(v) = self.txn.get(&key.encode())? {
            return Ok(Some(deserialize(&v)?));
        }
        Ok(None)
    }

    fn create_table(&mut self, table: Table) -> crate::error::Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!(
                "Create table name {} already exists.",
                table.name
            )));
        }
        table.validate(self)?;
        self.txn.set(
            &SQLKey::Table(Some((&table.name).into())).encode(),
            serialize(&table)?,
        )
    }

    fn delete_table(&mut self, table: &str) -> crate::error::Result<()> {
        let table = self.must_read_table(table)?;
        if let Some((t, cs)) = self.table_references(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Table {} is referenced by table {} column {}",
                table.name, t, cs[0]
            )));
        }
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?;
        }
        self.txn
            .delete(&SQLKey::Table(Some((&table.name).into())).encode())
    }

    fn scan_table(&self) -> Result<Tables> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&SQLKey::Table(None).encode())?
                .map(|r| {
                    r.and_then(|(_, v)| {
                        deserialize(&v).or(Err(Error::Internal("Excepted scan".into())))
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        ))
    }
}
