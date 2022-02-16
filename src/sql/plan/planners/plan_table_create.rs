use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::sql::data::DataColumn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<DataColumn>,
}
