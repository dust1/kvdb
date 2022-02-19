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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
