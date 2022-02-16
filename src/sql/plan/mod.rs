mod optimizer;
mod plan_node;
pub mod planner;
pub mod planners;

use std::fmt::Display;
use std::fmt::Formatter;

pub use plan_node::PlanNode;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::error::Result;
use crate::sql::execution::Executor;
use crate::sql::execution::ResultSet;
use crate::sql::parser::ast::KVStatement;
use crate::sql::plan::planner::Planner;
use crate::sql::schema::Catalog;
use crate::sql::schema::Table;
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
    /// build plan with statement
    pub fn build<C: Catalog>(statement: KVStatement, catalog: &mut C) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// optimize the plan
    pub fn optimize<C: Catalog>(self, _catalog: &mut C) -> Result<Self> {
        // todo
        Ok(self)
    }

    /// execute the plan
    pub fn execute<C: Catalog + 'static>(self, kv: &mut C) -> Result<ResultSet> {
        <dyn Executor<C>>::build(self.0).execute(kv)
    }
}

/// a sort order direction
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

/// Plan Node
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Nothing,
    CreateTable {
        schema: Table,
    },
    DropTable {
        table: String,
    },
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    Projection {
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        expressions: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        source: Box<Node>,
        expressions: Vec<(usize, Option<String>, Expression)>,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
    GroupBy {
        source: Box<Node>,
        expression: Vec<Expression>,
    },
    OrderBy {
        source: Box<Node>,
        orders: Vec<(Expression, Direction)>,
    },
    Limit {
        source: Box<Node>,
        offset: usize,
        limit: Option<usize>,
    },
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
            Self::Insert {
                table,
                columns: _,
                expressions,
            } => {
                s += &format!("Insert: {} ({} rows)\n", table, expressions.len());
            }
            Self::Nothing {} => {
                s += "Nothing\n";
            }
            Self::Projection {
                source,
                expressions,
            } => {
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
            Self::Scan {
                table,
                alias,
                filter,
            } => {
                s += &format!("Scan: {}", table);
                if let Some(alias) = alias {
                    s += &format!(" as {}", alias);
                }
                if let Some(expr) = filter {
                    s += &format!(" ({})", expr);
                }
                s += "\n";
            }
            _ => {
                // do nothing
            }
        };
        if root {
            s = s.trim_end().to_string()
        }
        s
    }
}

impl Display for Node {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
