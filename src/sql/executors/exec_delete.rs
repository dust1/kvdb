use crate::common::result::ResultSet;
use crate::error::Error;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::planners::DeletePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct DeleteExec<T: SQLTransaction> {
    table_name: String,
    source: Box<dyn KVExecutor<T>>,
}

impl<T: SQLTransaction + 'static> DeleteExec<T> {
    pub fn new(plan: DeletePlan) -> Box<Self> {
        Box::new(Self {
            table_name: plan.table_name,
            source: <dyn KVExecutor<T>>::build(*plan.source),
        })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for DeleteExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        let table = txn.must_read_table(&self.table_name)?;
        let mut count = 0;
        match self.source.execute(txn)? {
            ResultSet::Query { mut rows, .. } => {
                // iterator rows, get the primary key of the row
                // and delete it
                while let Some(row) = rows.next().transpose()? {
                    txn.delete(&self.table_name, &table.get_row_key(&row)?)?;
                    count += 1;
                }
                Ok(ResultSet::Delete { count })
            }
            r => Err(Error::Internal(format!("Unexpected result {}", r))),
        }
    }
}
