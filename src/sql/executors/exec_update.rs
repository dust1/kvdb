use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::UpdatePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct UpdateExec {
    plan: UpdatePlan,
}

impl UpdateExec {
    pub fn new(plan: UpdatePlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for UpdateExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}