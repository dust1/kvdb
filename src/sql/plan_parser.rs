use crate::error::{Error, Result};

use super::{
    plan::PlanNode,
    sql_parser::KVParser,
    sql_statement::KVStatement,
    statements::{AnalyzerResult, AnalyzerStatement},
};

pub struct PlanParser;

impl PlanParser {
    pub fn parser(sql: &str) -> Result<PlanNode> {
        let stmts = KVParser::parser_sql(sql)?;
        PlanParser::build_plan(stmts)
    }

    pub fn build_plan(stmts: Vec<KVStatement>) -> Result<PlanNode> {
        if stmts.len() != 1 {
            return Err(Error::Internal("Only support single query".into()));
        }

        match stmts[0].analyze()? {
            AnalyzerResult::SimpleQuery(plan) => Ok(*plan),
        }
    }
}
