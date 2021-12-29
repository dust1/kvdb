use sqlparser::ast::SetExpr;
use crate::error::{Error, Result};
use crate::sql::parser::ast::KVStatement;
use crate::sql::parser::translate::translate_object_name_to_string;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::{Catalog, Table};

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
        match statement {
            KVStatement::CreateTable {
                name,
                columns
            } => {
                let table = Table::new(name, columns)?;
                Ok(Node::CreateTable {
                    schema: table
                })
            },
            KVStatement::DropTable {
                names
            } => {
                let name = &names[0];
                let table_name = translate_object_name_to_string(name)?;
                Ok(Node::DropTable {
                    table: table_name
                })
            },
            KVStatement::Query(query) => {
                let mut node = plan_query_body(&query.body)?;

            },
            _ => {
                todo!()
            }
        }
    }


}

/// plan by query.body
fn plan_query_body(set_expr: &SetExpr) -> Result<Node> {
    match set_expr {
        SetExpr::Select(select) => {
            todo!()
        },
        _ => Err(Error::Parse("can not support this select.".to_string()))
    }
}