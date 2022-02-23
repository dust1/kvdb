use std::collections::HashSet;

use super::Catalog;
use crate::common::result::DataRow;
use crate::error::Result;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::schema::data_value::DataValue;
use crate::storage::mvcc::TransactionMode;

/// a row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<DataRow>> + Send>;

/// an index scan iterator
pub type IndexScan =
    Box<dyn DoubleEndedIterator<Item = Result<(DataValue, HashSet<DataValue>)>> + Send>;

/// A SQL-Transaction interface
/// All implementations of this trait need to implement Catalog at the same time
pub trait SQLTransaction: Catalog {
    /// The transaction ID
    fn id(&self) -> u64;
    /// The transaction mode
    fn mode(&self) -> TransactionMode;
    /// Commits the transaction
    fn commit(self) -> Result<()>;
    /// Rolls back the transaction
    fn rollback(self) -> Result<()>;

    /// Creates a new table row
    fn create(&mut self, table: &str, row: DataRow) -> Result<()>;
    /// Deletes a table row
    fn delete(&mut self, table: &str, id: &DataValue) -> Result<()>;
    /// Reads a table row, if it exists
    fn read(&self, table: &str, id: &DataValue) -> Result<Option<DataRow>>;
    /// Reads an index entry, if it exists
    fn read_index(
        &self,
        table: &str,
        column: &str,
        value: &DataValue,
    ) -> Result<HashSet<DataValue>>;
    /// Scans a table's rows
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan>;
    /// Scans a column's index entries
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
    /// Updates a table row
    fn update(&mut self, table: &str, id: &DataValue, row: DataRow) -> Result<()>;
}
