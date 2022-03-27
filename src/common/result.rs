use std::fmt::Display;

use derivative::Derivative;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::schema::data_value::DataValue;

/// A row of DataValue
pub type DataRow = Vec<DataValue>;

/// A row of iterator
pub type DataRows = Box<dyn Iterator<Item = Result<DataRow>> + Send>;

/// A column(in a result set)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataColumn {
    pub name: Option<String>,
}

/// a set of columns
pub type DataColumns = Vec<DataColumn>;

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
        columns: DataColumns,
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
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

impl ResultSet {
    pub fn empty_rows() -> DataRows {
        Box::new(std::iter::empty())
    }

    pub fn into_row(self) -> Result<DataRow> {
        if let ResultSet::Query { mut rows, .. } = self {
            rows.next()
                .transpose()?
                .ok_or_else(|| Error::Value("No rows returned".into()))
        } else {
            Err(Error::Value(format!("Not a query result: {}", self)))
        }
    }

    pub fn into_value(self) -> Result<DataValue> {
        self.into_row()?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Value("No value returned".into()))
    }
}

impl Display for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create { count } => write!(f, "ResultSet::Create{{count:{}}}", count),
            Self::CreateTable { name } => write!(f, "ResultSet::CreateTable{{name: {}}}", name),
            Self::DropTable { name } => write!(f, "ResultSet::DropTable{{name: {}}}", name),
            Self::Query { columns, rows: _ } => {
                write!(f, "ResultSet::Query:\r\n columns:{:?}", columns)
            }
            Self::Update { count } => write!(f, "ResultSet::Update{{count: {}}}", count),
            Self::Delete { count } => write!(f, "ResultSet::Delete{{count: {}}}", count),
            Self::Explain(plan) => write!(f, "ResultSet::Explain({})", plan),
        }
    }
}
