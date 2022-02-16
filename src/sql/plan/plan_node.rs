use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::planners::CreateTablePlan;
use super::planners::DropTablePlan;
use super::planners::InsertPlan;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    Insert(InsertPlan),
}
