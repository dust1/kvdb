use crate::error::Result;
use crate::sql::plan::PlanNode;
use crate::sql::sql_statement::KVStatement;

pub enum AnalyzerResult {
    SimpleQuery(Box<PlanNode>),
}

pub trait AnalyzerStatement {
    fn analyze(&self) -> Result<AnalyzerResult>;
}

impl AnalyzerStatement for KVStatement {
    fn analyze(&self) -> Result<AnalyzerResult> {
        match self {
            KVStatement::Query(v) => v.analyze(),
            KVStatement::Insert(v) => v.analyze(),
            KVStatement::DropTable(v) => v.analyze(),
            KVStatement::CreateTable(v) => v.analyze(),
            KVStatement::Delete(v) => v.analyze(),
            KVStatement::Update(v) => v.analyze(),
        }
    }
}
