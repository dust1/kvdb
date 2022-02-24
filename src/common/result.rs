use std::fmt::Display;

use crate::error::Result;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::schema::data_value::DataValue;

/// A row of DataValue
pub type DataRow = Vec<DataValue>;

/// A row of iterator
pub type DataRows = Box<dyn Iterator<Item = Result<DataRow>> + Send>;

/// A column(in a result set)
#[derive(Debug)]
pub struct DataColumn {
    pub name: Option<String>,
}

/// a set of columns
pub type DataColumns = Vec<DataColumn>;

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
        columns: DataColumns,
        rows: DataRows,
    },
    Update {
        count: u64,
    },
    Delete {
        count: u64,
    },
    // Explain result
    Explain(PlanNode),
}

impl Display for ResultSet {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
