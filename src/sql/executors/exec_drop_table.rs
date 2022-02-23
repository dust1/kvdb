use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::DropTablePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct DropTableExec {
    plan: DropTablePlan,
}

impl DropTableExec {
    pub fn new(plan: DropTablePlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for DropTableExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
