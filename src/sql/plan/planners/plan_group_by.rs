use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::Expression;
use crate::sql::plan::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GroupByPlan {
    pub source: Box<PlanNode>,
    pub expressions: Vec<Expression>,
}
