pub mod mutation;
pub mod query;
pub mod schema;
pub mod source;

use crate::error::Result;
use crate::sql::execution::mutation::Insert;
use crate::sql::execution::query::{Filter, Projection};
use crate::sql::execution::schema::{CreateTable, DropTable};
use crate::sql::execution::source::{Nothing, Scan};
use crate::sql::plan::Node;
use crate::sql::schema::Catalog;
use crate::sql::types::{Columns, Rows};
use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};

/// a plan executor
pub trait Executor<C: Catalog> {
    /// executes the executor, consuming it and return a result set
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet>;
}

/// an executor result set
#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    // rows created
    Create {
        count: u64,
    },
    // table created
    CreateTable {
        name: String,
    },
    // table drop
    DropTable {
        name: String,
    },
    // query result
    Query {
        columns: Columns,
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
        rows: Rows,
    },
    // Explain result
    Explain(Node),
}

impl<C: Catalog + 'static> dyn Executor<C> {
    /// builds an executor for a plan node, consuming it
    pub fn build(node: Node) -> Box<dyn Executor<C>> {
        match node {
            Node::Nothing => Nothing::new(),
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::DropTable { table } => DropTable::new(table),
            Node::Scan { table, alias: _, filter } => Scan::new(table, filter),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::Projection { source, expressions } => {
                Projection::new(Self::build(*source), expressions)
            }
            Node::Insert { table, columns, expressions } => {
                Insert::new(table, columns, expressions)
            }
        }
    }
}

impl ResultSet {
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }
}
