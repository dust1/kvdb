use std::sync::Arc;

use crate::error::Result;
use crate::sql::engine::Catalog;
use crate::sql::plan::plan_node::PlanNode;
use crate::sql::sql_statement::KVStatement;

pub enum AnalyzerResult {
    SimpleQuery(Box<PlanNode>),
}

pub trait AnalyzerStatement {
    fn analyze<C: Catalog>(&self, catalog: &mut C) -> Result<AnalyzerResult>;
}

impl AnalyzerStatement for KVStatement {
    fn analyze<C: Catalog>(&self, catalog: &mut C) -> Result<AnalyzerResult> {
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
