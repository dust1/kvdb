use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::DeletePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct DeleteExec {
    plan: DeletePlan,
}

impl DeleteExec {
    pub fn new(plan: DeletePlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for DeleteExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
