use serde_derive::{Deserialize, Serialize};

use crate::sql::schema::Table;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    CreateTable { schema: Table },
}
