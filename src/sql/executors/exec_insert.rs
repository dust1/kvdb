use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::InsertPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct InsertExec {
    plan: InsertPlan,
}

impl InsertExec {
    pub fn new(plan: InsertPlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for InsertExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
