use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_expression::Expression;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InsertPlan {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expression>>,
}
