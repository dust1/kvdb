use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq)]
pub struct GroupByPlan {
    pub source: Box<PlanNode>,
    pub expressions: Vec<Expression>,
}
