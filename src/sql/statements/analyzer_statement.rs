use std::sync::Arc;

use crate::error::Result;
use crate::sql::plan::PlanNode;
use crate::sql::session::Catalog;
use crate::sql::sql_statement::KVStatement;

pub enum AnalyzerResult {
    SimpleQuery(Box<PlanNode>),
}

pub trait AnalyzerStatement {
    fn analyze(&self, catalog: Arc<dyn Catalog>) -> Result<AnalyzerResult>;
}

impl AnalyzerStatement for KVStatement {
    fn analyze(&self, catalog: Arc<dyn Catalog>) -> Result<AnalyzerResult> {
        match self {
            KVStatement::Query(v) => v.analyze(catalog),
            KVStatement::Insert(v) => v.analyze(catalog),
            KVStatement::DropTable(v) => v.analyze(catalog),
            KVStatement::CreateTable(v) => v.analyze(catalog),
            KVStatement::Delete(v) => v.analyze(catalog),
            KVStatement::Update(v) => v.analyze(catalog),
        }
    }
}
