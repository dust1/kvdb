use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::Expression;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
    pub alias: Option<String>,
    pub filter: Option<Expression>,
}
