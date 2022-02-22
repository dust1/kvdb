use super::data::DataResult;
use super::engine::SQLTransaction;
use super::plan::PlanNode;
use super::session::Catalog;
use super::sql_parser::KVParser;
use super::sql_statement::KVStatement;
use super::statements::AnalyzerResult;
use super::statements::AnalyzerStatement;
use crate::error::Error;
use crate::error::Result;

pub struct PlanParser {
    plan: PlanNode,
}

impl PlanParser {
    pub fn parser<C: Catalog>(sql: &str, catalog: &mut C) -> Result<Self> {
        let stmts = KVParser::parser_sql(sql)?;
        PlanParser::build_plan(stmts, catalog)
    }

    pub fn build_plan<C: Catalog>(stmts: Vec<KVStatement>, catalog: &mut C) -> Result<Self> {
        if stmts.len() != 1 {
            return Err(Error::Internal("Only support single query".into()));
        }

        match stmts[0].analyze(catalog)? {
            AnalyzerResult::SimpleQuery(plan) => Ok(Self { plan: *plan }),
        }
    }

    /// optimize the plan, consuming it.
    pub fn optimize<C: Catalog>(self, _catalog: &mut C) -> Result<Self> {
        /// just do it
        Ok(self)
    }

    pub fn execute<T: SQLTransaction>(self, txn: &mut T) -> Result<DataResult> {
        todo!()
    }
}
