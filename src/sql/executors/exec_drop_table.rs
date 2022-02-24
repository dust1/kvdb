use crate::common::result::ResultSet;
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
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        let table_name = self.plan.table_name;
        txn.delete_table(&table_name)?;
        Ok(ResultSet::DropTable { name: table_name })
    }
}
