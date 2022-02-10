use sqlparser::ast::{Assignment, Expr, ObjectName};

use super::AnalyzerStatement;

pub struct KVUpdateStatement {
    pub table_name: ObjectName,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

impl AnalyzerStatement for KVUpdateStatement {
    fn analyze(&self) -> crate::error::Result<super::analyzer_statement::AnalyzerResult> {
        todo!()
    }
}
