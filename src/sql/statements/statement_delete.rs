use std::sync::Arc;

use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Result;
use crate::sql::plan::planner::Scope;
use crate::sql::plan::planners::DeletePlan;
use crate::sql::plan::planners::Expression;
use crate::sql::plan::planners::ScanPlan;
use crate::sql::plan::PlanNode;
use crate::sql::session::Catalog;

pub struct KVDeleteStatement {
    pub table_name: ObjectName,
    pub selection: Option<Expr>,
}

impl AnalyzerStatement for KVDeleteStatement {
    fn analyze(&self, catalog: Arc<dyn Catalog>) -> Result<AnalyzerResult> {
        let ctx = catalog.clone();
        let table_name = self.table_name.to_string();
        let table = ctx.must_read_table(&table_name)?;
        let mut scope = Scope::from_table(table)?;
        let filter = self
            .selection
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
