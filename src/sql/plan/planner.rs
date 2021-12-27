use sqlparser::ast::Statement;
use sqlparser::ast::ColumnDef;

use crate::error::Result;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::Catalog;

/// query plan builder
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C
}

impl <'a, C: Catalog> Planner<'a, C> {

    pub fn new(catalog: &'a mut C) -> Self {
        Self {catalog}
    }

    pub fn build(&mut self, statement: Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    pub fn build_statement(&mut self, statement: Statement) -> Result<Node> {
        Ok(match statement {
            Statement::CreateTable {name:ObjectName, columns, .. } => {
                todo!()
            },
            Statement::Query(query) => {
                todo!()
            },
            _ => {
                todo!()
            }
        })
    }

}