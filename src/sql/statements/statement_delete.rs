use std::sync::Arc;

use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::common::scope::Scope;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::plan::planners::DeletePlan;
use crate::sql::plan::planners::ScanPlan;

pub struct KVDeleteStatement {
    pub table_name: ObjectName,
    pub selection: Option<Expr>,
}

impl AnalyzerStatement for KVDeleteStatement {
    fn analyze<C: Catalog>(&self, catalog: &mut C) -> Result<AnalyzerResult> {
        let table_name = self.table_name.to_string();
        let table = catalog.must_read_table(&table_name)?;
        let mut scope = Scope::from_table(table)?;
        let filter = self
            .selection
            .as_ref()
            .map(|expr| Expression::from_expr(&expr, &mut scope))
            .transpose()?;

        Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::Delete(
            DeletePlan {
                table_name: table_name.clone(),
                source: Box::new(PlanNode::Scan(ScanPlan {
                    table_name: table_name.clone(),
                    alias: None,
                    filter,
                })),
            },
        ))))
    }
}
