use serde_derive::{Serialize, Deserialize};

use crate::sql::schema::Table;


#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    CreateTable {
        schema: Table
    }
}