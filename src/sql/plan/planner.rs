use crate::error::Result;
use crate::sql::parser::ast::KVStatement;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::Catalog;

/// query plan builder
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    pub fn build(&mut self, statement: KVStatement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    pub fn build_statement(&mut self, statement: KVStatement) -> Result<Node> {
        todo!()
    }

}
