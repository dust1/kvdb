use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DeletePlan {
    pub table_name: String,
    pub source: Box<PlanNode>,
}
