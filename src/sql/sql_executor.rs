use super::data::DataResult;
use super::executors::NothingExec;
use super::plan::PlanNode;
use super::session::Catalog;
use crate::error::Result;

// plan executor
pub trait KVExecutor<C: Catalog> {
    fn execute(self: Box<Self>, ctx: &mut C) -> Result<DataResult>;
}

impl<C: Catalog + 'static> dyn KVExecutor<C> {
    pub fn build(node: PlanNode) -> Box<dyn KVExecutor<C>> {
        match node {
            PlanNode::Nothing => NothingExec::new(),
            _ => todo!(),
        }
    }
}
