mod planner;

use sqlparser::ast::{Statement, TableAlias};

use crate::error::Result;
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::expression::Expression;

/// a query plan
pub struct Plan(pub Node);

impl Plan {
    pub fn build<C: Catalog>(statement: Statement, catalog: &mut C) -> Result<Self> {
        todo!()
    }
}

/// Plan Node
pub enum Node {
    CreateTable{
        schema: Table
    },
    DropTable {
        table: String
    },
    Scan {
        table: String,
        alias: Option<TableAlias>,
        filter: Option<Expression>
    }
}
