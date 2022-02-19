use std::sync::Arc;

use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SqlOption;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Result;
use crate::sql::data::DataColumn;
use crate::sql::plan::planners::CreateTablePlan;
use crate::sql::plan::PlanNode;
use crate::sql::session::Catalog;

pub struct KVCreateTableStatement {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub config: Vec<SqlOption>,
    pub query: Option<Box<Query>>,
    pub like: Option<ObjectName>,
}

impl AnalyzerStatement for KVCreateTableStatement {
    fn analyze(&self, _catalog: Arc<dyn Catalog>) -> Result<AnalyzerResult> {
        let columns = self
            .columns
            .iter()
            .map(DataColumn::try_form)
            .collect::<Vec<_>>();
        Ok(AnalyzerResult::SimpleQuery(Box::new(
            PlanNode::CreateTable(CreateTablePlan {
                name: self.name.to_string(),
                columns,
            }),
        )))
    }
}
