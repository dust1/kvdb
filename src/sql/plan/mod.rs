pub mod planner;

use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::{Statement, TableAlias};
use std::fmt::{Display, Formatter};

use crate::error::Result;
use crate::sql::parser::ast::KVStatement;
use crate::sql::plan::planner::Planner;
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::expression::Expression;

/// a query plan
#[derive(Debug)]
pub struct Plan(pub Node);

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl Plan {
    pub fn build<C: Catalog>(statement: KVStatement, catalog: &mut C) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }
}

/// Plan Node
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Nothing,
    CreateTable { schema: Table },
    DropTable { table: String },
    Scan { table: String, alias: Option<String>, filter: Option<Expression> },
    Filter { source: Box<Node>, predicate: Expression },
    Projection { source: Box<Node>, expressions: Vec<(Expression, Option<String>)> },
    Insert { table: String, columns: Vec<String>, expressions: Vec<Vec<Expression>> },
}

impl Node {
    pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
        let mut s = indent.clone();
        if !last {
            s += "├─ ";
            indent += "│  "
        } else if !root {
            s += "└─ ";
            indent += "   ";
        }
        match self {
            Self::CreateTable { schema } => {
                s += &format!("CreateTable: {}\n", schema.name);
            }
            Self::DropTable { table } => {
                s += &format!("DropTable: {}\n", table);
            }
            Self::Filter { source, predicate } => {
                s += &format!("Filter: {}\n", predicate);
                s += &source.format(indent, false, true);
            }
            Self::Insert { table, columns: _, expressions } => {
                s += &format!("Insert: {} ({} rows)\n", table, expressions.len());
            }
            Self::Nothing {} => {
                s += "Nothing\n";
            }
            Self::Projection { source, expressions } => {
                s += &format!(
                    "Projection: {}\n",
                    expressions
                        .iter()
                        .map(|(expr, _)| expr.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                s += &source.format(indent, false, true);
            }
            Self::Scan { table, alias, filter } => {
                s += &format!("Scan: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if let Some(expr) = filter {
                    s += &format!(" ({})", expr);
                }
                s += "\n";
            }
            _ => todo!(),
        };
        if root {
            s = s.trim_end().to_string()
        }
        s
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}