use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::ProjectionPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct ProjectionExec {
    plan: ProjectionPlan,
}

impl ProjectionExec {
    pub fn new(plan: ProjectionPlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for ProjectionExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
