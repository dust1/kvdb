use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::{Catalog, Table};


pub struct CreateTable {
    table: Table
}

pub struct DropTable {
    table: String
}

impl CreateTable {
    pub fn new(table: Table) -> Box<Self> {
        Box::new(Self {
            table
        })
    }
}

impl<C: Catalog> Executor<C> for CreateTable {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        let name = self.table.name.clone();
        catalog.create_table(self.table)?;
        Ok(ResultSet::CreateTable {
            name
        })
    }
}

impl DropTable {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self {
            table
        })
    }
}

impl <C:Catalog> Executor<C> for DropTable {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        catalog.delete_table(&self.table)?;
        Ok(ResultSet::DropTable {
            name: self.table
        })
    }
}