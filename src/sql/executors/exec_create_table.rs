use crate::common::result::ResultSet;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::CreateTablePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct CreateTableExec {
    plan: CreateTablePlan,
}

impl CreateTableExec {
    pub fn new(plan: CreateTablePlan) -> Box<Self> {
        Box::new(Self { plan })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for CreateTableExec {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = self.plan.to_table();
        let name = table.name.clone();
        txn.create_table(table)?;
        Ok(ResultSet::CreateTable { name })
    }
}
