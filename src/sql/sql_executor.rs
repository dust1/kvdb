use crate::error::Result;

use super::{session::Catalog, data::DataResult, plan::PlanNode, executors::NothingExec};


// plan executor
pub trait KVExecutor<C: Catalog> {

    fn execute(self: Box<Self>, ctx: &mut C) -> Result<DataResult>;

}

impl<C: Catalog + 'static> dyn KVExecutor<C> {
    
    pub fn build(node: PlanNode) -> Box<dyn KVExecutor<C>> {
        match node {
            PlanNode::Nothing => NothingExec::new(),
            _ => todo!()
        }
    }

}