use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionPlan {
    pub source: Box<PlanNode>,
    pub expressions: Vec<(Expression, Option<String>)>,
}
