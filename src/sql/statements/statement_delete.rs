use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use super::AnalyzerStatement;

pub struct KVDeleteStatement {
    pub table_name: ObjectName,
    pub selection: Option<Expr>,
}

impl AnalyzerStatement for KVDeleteStatement {
    fn analyze(&self) -> crate::error::Result<super::analyzer_statement::AnalyzerResult> {
        todo!()
    }
}
