pub mod mutation;
pub mod query;
pub mod schema;
pub mod source;

use std::fmt::Display;

use derivative::Derivative;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use self::mutation::Delete;
use self::mutation::Update;
use self::query::GroupBy;
use self::query::Limit;
use self::query::Order;
use crate::error::Result;
use crate::sql::execution::mutation::Insert;
use crate::sql::execution::query::Filter;
use crate::sql::execution::query::Projection;
use crate::sql::execution::schema::CreateTable;
use crate::sql::execution::schema::DropTable;
use crate::sql::execution::source::Nothing;
use crate::sql::execution::source::Scan;
use crate::sql::plan::Node;
use crate::sql::schema::Catalog;
use crate::sql::types::Columns;
use crate::sql::types::Rows;

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
    Update {
        count: u64,
    },
    Delete {
        count: u64,
    },
    // Explain result
    Explain(Node),
}

impl Display for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultSet::Create { count } => write!(f, "create {}", count),
            ResultSet::CreateTable { name } => write!(f, "create table {}", name),
            ResultSet::DropTable { name } => write!(f, "drop table {}", name),
            ResultSet::Query { columns, .. } => write!(f, "query columns {:?}", columns),
            ResultSet::Update { count } => write!(f, "update count {}", count),
            ResultSet::Delete { count } => write!(f, "delete count {}", count),
            ResultSet::Explain(node) => write!(f, "explain node {}", node),
        }
    }
}

impl<C: Catalog + 'static> dyn Executor<C> {
    /// builds an executor for a plan node, consuming it
    pub fn build(node: Node) -> Box<dyn Executor<C>> {
        match node {
            Node::Nothing => Nothing::new(),
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::DropTable { table } => DropTable::new(table),
            Node::Scan {
                table,
                alias: _,
                filter,
            } => Scan::new(table, filter),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
            Node::Projection {
                source,
                expressions,
            } => Projection::new(Self::build(*source), expressions),
            Node::Insert {
                table,
                columns,
                expressions,
            } => Insert::new(table, columns, expressions),
            Node::Update {
                table,
                source,
                expressions,
            } => Update::new(
                table,
                Self::build(*source),
                expressions.into_iter().map(|(i, _, e)| (i, e)).collect(),
            ),
            Node::Delete { table, source } => Delete::new(table, Self::build(*source)),
            Node::GroupBy { source, expression } => GroupBy::new(Self::build(*source), expression),
            Node::OrderBy { source, orders } => Order::new(Self::build(*source), orders),
            Node::Limit {
                source,
                offset,
                limit,
            } => Limit::new(Self::build(*source), offset, limit),
        }
    }
}

impl ResultSet {
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }
}
