use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::GroupByPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct GroupByExec {
    plan: GroupByPlan,
}

impl GroupByExec {
    pub fn new(plan: GroupByPlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for GroupByExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
