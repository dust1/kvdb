use crate::error::Error;
use crate::error::Result;
use crate::sql::schema::table::Table;

pub trait Catalog {
    /// create table
    fn create_table(&self, table: Table) -> Result<()>;

    /// delete table
    fn delete_table(&self, table: &str) -> Result<()>;

    /// Read a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;

    /// Read a table, and error if it does not exists
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist.", table)))
    }
}
