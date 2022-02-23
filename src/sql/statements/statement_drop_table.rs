use sqlparser::ast::ObjectName;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::plan::planners::DropTablePlan;

pub struct KVDropTableStatement {
    pub if_exists: bool,
    pub names: Vec<ObjectName>,
}

impl AnalyzerStatement for KVDropTableStatement {
    fn analyze<C: Catalog>(&self, _catalog: &mut C) -> Result<AnalyzerResult> {
        let name = &self.names[0];
        Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::DropTable(
            DropTablePlan {
                table_name: name.to_string(),
            },
        ))))
    }
}
