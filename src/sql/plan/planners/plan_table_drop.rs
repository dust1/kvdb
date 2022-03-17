use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropTablePlan {
    pub table_name: String,
}
