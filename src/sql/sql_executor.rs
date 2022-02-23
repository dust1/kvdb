use super::engine::SQLTransaction;
use super::executors::CreateTableExec;
use super::executors::DeleteExec;
use super::executors::DropTableExec;
use super::executors::FilterExec;
use super::executors::GroupByExec;
use super::executors::InsertExec;
use super::executors::NothingExec;
use super::executors::ProjectionExec;
use super::executors::ScanExec;
use super::executors::UpdateExec;
use super::plan::plan_node::PlanNode;
use crate::common::result::ResultSet;
use crate::error::Result;

// plan executor
pub trait KVExecutor<T: SQLTransaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: SQLTransaction + 'static> dyn KVExecutor<T> {
    pub fn build(node: PlanNode) -> Box<dyn KVExecutor<T>> {
        match node {
            PlanNode::Nothing => NothingExec::new(),
            PlanNode::CreateTable(plan) => CreateTableExec::new(plan),
            PlanNode::DropTable(plan) => DropTableExec::new(plan),
            PlanNode::Insert(plan) => InsertExec::new(plan),
            PlanNode::Scan(plan) => ScanExec::new(plan),
            PlanNode::Filter(plan) => FilterExec::new(plan),
            PlanNode::Projection(plan) => ProjectionExec::new(plan),
            PlanNode::GroupBy(plan) => GroupByExec::new(plan),
            PlanNode::Update(plan) => UpdateExec::new(plan),
            PlanNode::Delete(plan) => DeleteExec::new(plan),
        }
    }
}
