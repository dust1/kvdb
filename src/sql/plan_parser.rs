use sqlparser::ast::Statement;

use crate::{error::{Result, Error}, common::PlanNode};

use super::{sql_parser::KVParser, sql_statement::KVStatement};



pub struct PlanParser;

impl PlanParser {

    pub fn parser(sql: &str) -> Result<PlanNode> {
        let stmts = KVParser::parser_sql(sql)?;
        PlanParser::build_plan(stmts)
    }

    pub fn build_plan(mut stmts: Vec<KVStatement>) -> Result<PlanNode> {
        if stmts.len() != 1 {
            return Err(Error::Internal("Only support single query".into()));
        }

        todo!()
    }

    pub fn build_sql_plan(stmt: KVStatement) -> Result<PlanNode> {
        match stmt {
            _ => todo!()
        }
    }

    
}