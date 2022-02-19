use super::schema::Table;
use crate::error::Error;
use crate::error::Result;

pub trait Catalog {
    /// Read a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;

    /// Read a table, and error if it does not exists
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist.", table)))
    }
}
