use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::data::DataTable;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    CreateTable { schema: DataTable },
}
