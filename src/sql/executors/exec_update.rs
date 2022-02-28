use std::collections::HashSet;

use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::planners::UpdatePlan;
use crate::sql::sql_executor::KVExecutor;

pub struct UpdateExec<T: SQLTransaction> {
    table_name: String,
    source: Box<dyn KVExecutor<T>>,
    expressions: Vec<(usize, Option<String>, Expression)>,
}

impl<T: SQLTransaction + 'static> UpdateExec<T> {
    pub fn new(plan: UpdatePlan) -> Box<Self> {
        Box::new(Self {
            table_name: plan.table_name,
            source: <dyn KVExecutor<T>>::build(*plan.source),
            expressions: plan.expressions,
        })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for UpdateExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Query { mut rows, .. } => {
                let table = txn.must_read_table(&self.table_name)?;
                let mut updated = HashSet::new();
                while let Some(row) = rows.next().transpose()? {
                    let id = table.get_row_key(&row)?;
                    if updated.contains(&id) {
                        continue;
                    }

                    let mut new = row.clone();
                    for (field, _, expr) in &self.expressions {
                        new[*field] = expr.evaluate(Some(&row))?;
                    }
                    txn.update(&self.table_name, &id, new)?;
                    updated.insert(id);
                }

                Ok(ResultSet::Update {
                    count: updated.len() as u64,
                })
            }
            e => Err(Error::Internal(format!("Unexpceted result{}", e))),
        }
    }
}
