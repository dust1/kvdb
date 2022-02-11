use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::data_column::DataColumn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataTable {
    pub name: String,
    pub columns: Vec<DataColumn>,
}
