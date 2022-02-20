
use crate::sql::plan::PlanNode;

use super::{DataColumns, DataRows};


pub enum DataResult {
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