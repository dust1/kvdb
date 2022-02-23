use super::engine::Catalog;
use super::engine::SQLTransaction;
use super::plan::plan_node::PlanNode;
use super::sql_parser::KVParser;
use super::sql_statement::KVStatement;
use super::statements::AnalyzerResult;
use super::statements::AnalyzerStatement;
use crate::common::result::ResultSet;
use crate::error::Error;
use crate::error::Result;
use crate::sql::sql_executor::KVExecutor;

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

    /// executes the plan, consuming it
    pub fn execute<T: SQLTransaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        let result = <dyn KVExecutor<T>>::build(self.plan).execute(txn)?;
        Ok(result)
    }
}
