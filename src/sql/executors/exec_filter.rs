use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::FilterPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct FilterExec {
    plan: FilterPlan,
}

impl FilterExec {
    pub fn new(plan: FilterPlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for FilterExec {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        todo!()
    }
}
