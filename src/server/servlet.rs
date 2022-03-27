use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::common::result::DataRow;
use crate::common::result::ResultSet;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<DataRow>),
    ListTable(Vec<String>),
}
