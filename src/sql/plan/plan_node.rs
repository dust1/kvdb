use std::fmt::Display;

use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::planners::CreateTablePlan;
use super::planners::DeletePlan;
use super::planners::DropTablePlan;
use super::planners::FilterPlan;
use super::planners::GroupByPlan;
use super::planners::InsertPlan;
use super::planners::ProjectionPlan;
use super::planners::ScanPlan;
use super::planners::UpdatePlan;

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq)]
pub enum PlanNode {
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    Insert(InsertPlan),
    Scan(ScanPlan),
    Filter(FilterPlan),
    Projection(ProjectionPlan),
    GroupBy(GroupByPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Nothing,
}

impl Display for PlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable(plan) => write!(f, "PlanNode::CreateTable({})", plan),
            Self::DropTable(plan) => write!(f, "PlanNode::DropTable({:?})", plan),
            Self::Insert(plan) => write!(f, "PlanNode::Insert({:?})", plan),
            Self::Scan(plan) => write!(f, "PlanNode::Scan({:?})", plan),
            Self::Filter(plan) => write!(f, "PlanNode::Filter({:?})", plan),
            Self::Projection(plan) => write!(f, "PlanNode::Projection({:?})", plan),
            Self::GroupBy(plan) => write!(f, "PlanNode::GrouBy({:?})", plan),
            Self::Update(plan) => write!(f, "PlanNode::Update({:?})", plan),
            Self::Delete(plan) => write!(f, "PlanNode::Delete({:?})", plan),
            Self::Nothing => write!(f, "PlanNode::Nothin"),
        }
    }
}
