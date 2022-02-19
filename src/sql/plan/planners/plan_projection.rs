use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::Expression;
use crate::sql::plan::PlanNode;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ProjectionPlan {
    pub source: Box<PlanNode>,
    pub expressions: Vec<(Expression, Option<String>)>,
}
