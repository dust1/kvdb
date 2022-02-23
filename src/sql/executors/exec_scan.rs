use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::ScanPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct ScanExec {
    plan: ScanPlan,
}

impl ScanExec {
    pub fn new(plan: ScanPlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for ScanExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
