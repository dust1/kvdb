use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SqlOption;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::plan::planners::CreateTablePlan;
use crate::sql::schema::table_column::TableColumn;

pub struct KVCreateTableStatement {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub config: Vec<SqlOption>,
    pub query: Option<Box<Query>>,
    pub like: Option<ObjectName>,
}

impl AnalyzerStatement for KVCreateTableStatement {
    fn analyze<C: Catalog>(&self, _catalog: &mut C) -> Result<AnalyzerResult> {
        let columns = self
            .columns
            .iter()
            .map(TableColumn::try_form)
            .collect::<Vec<_>>();
        Ok(AnalyzerResult::SimpleQuery(Box::new(
            PlanNode::CreateTable(CreateTablePlan {
                name: self.name.to_string(),
                columns,
            }),
        )))
    }
}
