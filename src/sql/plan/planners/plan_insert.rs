use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::plan_expression::Expression;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InsertPlan {
    pub table_name: String,
    pub columns: Vec<String>,
    pub expressions: Vec<Vec<Expression>>,
}
