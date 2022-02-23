use super::engine::SQLTransaction;
use super::executors::NothingExec;
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
            _ => todo!(),
        }
    }
}
