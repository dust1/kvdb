use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdatePlan {
    pub table_name: String,
    pub source: Box<PlanNode>,
    pub expressions: Vec<(usize, Option<String>, Expression)>,
}
