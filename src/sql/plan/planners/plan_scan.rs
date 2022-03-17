use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::plan::plan_expression::Expression;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanPlan {
    pub table_name: String,
    pub alias: Option<String>,
    pub filter: Option<Expression>,
}
