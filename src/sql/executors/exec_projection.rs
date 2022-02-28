use crate::common::result::DataColumn;
use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::SQLTransaction;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::planners::ProjectionPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct ProjectionExec<T: SQLTransaction> {
    source: Box<dyn KVExecutor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T: SQLTransaction + 'static> ProjectionExec<T> {
    pub fn new(plan: ProjectionPlan) -> Box<Self> {
        Box::new(Self {
            source: <dyn KVExecutor<T>>::build(*plan.source),
            expressions: plan.expressions,
        })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for ProjectionExec<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<crate::common::result::ResultSet> {
        if let ResultSet::Query { rows, columns } = self.source.execute(txn)? {
            let (expressions, label): (Vec<Expression>, Vec<Option<String>>) =
                self.expressions.into_iter().unzip();
            let columns = expressions
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    if let Some(Some(name)) = label.get(i) {
                        DataColumn {
                            name: Some(name.clone()),
                        }
                    } else if let Expression::Field(i, _) = e {
                        columns
                            .get(*i)
                            .cloned()
                            .unwrap_or(DataColumn { name: None })
                    } else {
                        DataColumn { name: None }
                    }
                })
                .collect();

            let rows = Box::new(rows.map(move |r| {
                r.and_then(|row| {
                    expressions
                        .iter()
                        .map(|e| e.evaluate(Some(&row)))
                        .collect::<Result<_>>()
                })
            }));
            Ok(ResultSet::Query { rows, columns })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
