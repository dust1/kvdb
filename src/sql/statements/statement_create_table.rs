use sqlparser::ast::{ObjectName, ColumnDef, SqlOption, Query};

use super::AnalyzerStatement;


pub struct KVCreateTableStatement {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub config: Vec<SqlOption>,
    pub query: Option<Box<Query>>,
    pub like: Option<ObjectName>,
}

impl AnalyzerStatement for KVCreateTableStatement {
    fn analyze(&self) -> crate::error::Result<super::analyzer_statement::AnalyzerResult> {
        todo!()
    }
}