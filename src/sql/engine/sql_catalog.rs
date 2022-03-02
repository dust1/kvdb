use crate::error::Error;
use crate::error::Result;
use crate::sql::schema::table::Table;
use crate::sql::schema::table::Tables;

pub trait Catalog {
    /// create table
    fn create_table(&mut self, table: Table) -> Result<()>;

    /// delete table
    fn delete_table(&mut self, table: &str) -> Result<()>;

    /// Read a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;

    /// iterator over all tables
    fn scan_table(&self) -> Result<Tables>;

    /// Read a table, and error if it does not exists
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist.", table)))
    }

    /// return all references to a table, as table,column pairs
    fn table_references(&self, table: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self
            .scan_table()?
            .filter(|t| with_self || t.name != table)
            .map(|t| {
                (
                    t.name,
                    t.columns
                        .iter()
                        .filter(|c| c.references.as_deref() == Some(table))
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, cs)| !cs.is_empty())
            .collect::<Vec<_>>())
    }
}
