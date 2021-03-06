use crate::sql::engine::SQLTransaction;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::planners::GroupByPlan;
use crate::sql::sql_executor::KVExecutor;

pub struct GroupByExec<T: SQLTransaction> {
    _source: Box<dyn KVExecutor<T>>,
    _expressions: Vec<Expression>,
}

impl<T: SQLTransaction + 'static> GroupByExec<T> {
    pub fn new(plan: GroupByPlan) -> Box<Self> {
        Box::new(Self {
            _source: <dyn KVExecutor<T>>::build(*plan.source),
            _expressions: plan.expressions,
        })
    }
}

impl<T: SQLTransaction + 'static> KVExecutor<T> for GroupByExec<T> {
    fn execute(
        self: Box<Self>,
        _txn: &mut T,
    ) -> crate::error::Result<crate::common::result::ResultSet> {
        // FIXME should do it
        todo!()
    }
}
