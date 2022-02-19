use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::Expression;
use crate::sql::plan::PlanNode;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterPlan {
    pub source: Box<PlanNode>,
    pub predicate: Expression,
}
