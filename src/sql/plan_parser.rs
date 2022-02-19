use std::sync::Arc;

use super::plan::PlanNode;
use super::session::Catalog;
use super::sql_parser::KVParser;
use super::sql_statement::KVStatement;
use super::statements::AnalyzerResult;
use super::statements::AnalyzerStatement;
use crate::error::Error;
use crate::error::Result;

pub struct PlanParser;

impl PlanParser {
    pub fn parser(sql: &str, catalog: Arc<dyn Catalog>) -> Result<PlanNode> {
        let stmts = KVParser::parser_sql(sql)?;
        PlanParser::build_plan(stmts, catalog)
    }

    pub fn build_plan(stmts: Vec<KVStatement>, catalog: Arc<dyn Catalog>) -> Result<PlanNode> {
        if stmts.len() != 1 {
            return Err(Error::Internal("Only support single query".into()));
        }

        match stmts[0].analyze(catalog)? {
            AnalyzerResult::SimpleQuery(plan) => Ok(*plan),
        }
    }
}
