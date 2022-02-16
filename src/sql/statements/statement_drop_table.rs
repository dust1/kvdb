use sqlparser::ast::ObjectName;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::planners::DropTablePlan;
use crate::sql::plan::PlanNode;

pub struct KVDropTableStatement {
    pub if_exists: bool,
    pub names: Vec<ObjectName>,
}

impl AnalyzerStatement for KVDropTableStatement {
    fn analyze(&self) -> Result<AnalyzerResult> {
        let name = &self.names[0];
        Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::DropTable(
            DropTablePlan {
                table_name: name.to_string(),
            },
        ))))
    }
}
