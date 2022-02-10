use sqlparser::ast::ObjectName;

use super::AnalyzerStatement;

pub struct KVDropTableStatement {
    pub if_exists: bool,
    pub names: Vec<ObjectName>,
}

impl AnalyzerStatement for KVDropTableStatement {
    fn analyze(&self) -> crate::error::Result<super::analyzer_statement::AnalyzerResult> {
        todo!()
    }
}
