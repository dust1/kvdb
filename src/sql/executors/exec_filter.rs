use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::planners::FilterPlan;
use crate::sql::schema::data_value::DataValue;
use crate::sql::sql_executor::KVExecutor;

pub struct FilterExec<T: SQLTransaction> {
    source: Box<dyn KVExecutor<T>>,
    predicate: Expression,
}

impl<T: SQLTransaction + 'static> FilterExec<T> {
    pub fn new(plan: FilterPlan) -> Box<Self> {
        Box::new(Self {
            predicate: plan.predicate,
            source: <dyn KVExecutor<T>>::build(*plan.source),
        })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for FilterExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let predicate = self.predicate;

        match self.source.execute(txn)? {
            ResultSet::Query { rows, columns } => {
                let rows = rows.filter_map(move |row| {
                    // expression evaluate every row
                    // if return Boolean(true), will show it.
                    // other returned, filter it
                    row.and_then(|row| match predicate.evaluate(Some(&row))? {
                        DataValue::Boolean(true) => Ok(Some(row)),
                        DataValue::Boolean(false) => Ok(None),
                        DataValue::Null => Ok(None),
                        value => Err(Error::Value(format!(
                            "Filter returned {}, expected boolean",
                            value
                        ))),
                    })
                    .transpose()
                });
                Ok(ResultSet::Query {
                    columns,
                    rows: Box::new(rows),
                })
            }
            r => Err(Error::Internal(format!("Unexpexted result {}", r))),
        }
    }
}
