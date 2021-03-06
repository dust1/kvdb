use sqlparser::ast::Assignment;
use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::common::scope::Scope;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::plan::planners::ScanPlan;
use crate::sql::plan::planners::UpdatePlan;

#[derive(Debug, PartialEq, Eq)]
pub struct KVUpdateStatement {
    pub table_name: ObjectName,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

impl AnalyzerStatement for KVUpdateStatement {
    fn analyze<C: Catalog>(&self, catalog: &mut C) -> Result<AnalyzerResult> {
        let table_name = self.table_name.to_string();
        let table = catalog.must_read_table(&table_name)?;
        let mut scope = Scope::from_table(table)?;
        let set = self.assignment_to_set(&self.assignments, &mut scope)?;
        let filter = self
            .selection
            .as_ref()
            .map(|expr| Expression::from_expr(expr, &mut scope))
            .transpose()?;

        Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::Update(
            UpdatePlan {
                table_name: table_name.clone(),
                source: Box::new(PlanNode::Scan(ScanPlan {
                    table_name,
                    alias: None,
                    filter,
                })),
                expressions: set,
            },
        ))))
    }
}

impl KVUpdateStatement {
    /// assignment to set
    fn assignment_to_set(
        &self,
        assignments: &[Assignment],
        scope: &mut Scope,
    ) -> Result<Vec<(usize, Option<String>, Expression)>> {
        Ok(assignments
            .iter()
            .map(|issignment| {
                let field = issignment.id.to_string();
                Ok((
                    scope.resolve(None, &field)?,
                    Some(field),
                    Expression::from_expr(&issignment.value, scope)?,
                ))
            })
            .collect::<Result<_>>()?)
    }
}
