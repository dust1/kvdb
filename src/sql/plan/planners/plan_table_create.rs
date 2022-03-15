use std::fmt::Display;

use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::schema::table::Table;
use crate::sql::schema::table_column::TableColumn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<TableColumn>,
}

impl CreateTablePlan {
    pub fn to_table(self) -> Table {
        Table {
            name: self.name,
            columns: self.columns,
        }
    }
}

impl Display for CreateTablePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table_name: {}, columns: {:?}", self.name, self.columns)
    }
}
