use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::CreateTablePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct CreateTableExec {
    plan: CreateTablePlan,
}

impl CreateTableExec {
    pub fn new(_plan: CreateTablePlan) -> Box<Self> {
        todo!()
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for CreateTableExec {
    fn execute(self: Box<Self>, _txn: &mut T) -> Result<crate::common::result::ResultSet> {
        todo!()
    }
}
