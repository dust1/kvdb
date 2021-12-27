use sqlparser::ast::Statement;

use crate::error::Result;
use crate::sql::schema::Catalog;

mod planner;

/// a query plan
pub struct Plan(pub Node);

impl Plan {
    pub fn build<C: Catalog>(statement: Statement, catalog: &mut C) -> Result<Self> {

    }
}

/// Plan Node
pub enum Node {

}
