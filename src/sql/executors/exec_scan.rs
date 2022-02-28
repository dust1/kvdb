use crate::common::result::DataColumn;
use crate::common::result::ResultSet;
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
        txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        let table = txn.must_read_table(&self.plan.table_name)?;
        Ok(ResultSet::Query {
            columns: table
                .columns
                .iter()
                .map(|c| DataColumn {
                    name: Some(c.name.clone()),
                })
                .collect(),
            rows: Box::new(txn.scan(&self.plan.table_name, self.plan.filter)?),
        })
    }
}
