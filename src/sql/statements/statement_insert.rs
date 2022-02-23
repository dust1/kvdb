use std::sync::Arc;

use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SqliteOnConflict;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::plan::planners::InsertPlan;

pub struct KVInsertStatement {
    /// Only for Sqlite
    pub or: Option<SqliteOnConflict>,
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: Box<Query>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub table: bool,
}

impl AnalyzerStatement for KVInsertStatement {
    fn analyze<C: Catalog>(&self, catalog: &mut C) -> Result<AnalyzerResult> {
        Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::Insert(
            InsertPlan {
                table_name: self.table_name.to_string(),
                columns: self
                    .columns
                    .iter()
                    .map(|ident| ident.to_string())
                    .collect::<Vec<String>>(),
                expressions: Expression::from_query(self.source.as_ref())?,
            },
        ))))
    }
}
