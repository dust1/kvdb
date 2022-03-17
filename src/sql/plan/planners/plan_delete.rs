use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_node::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq)]
pub struct DeletePlan {
    pub table_name: String,
    pub source: Box<PlanNode>,
}
