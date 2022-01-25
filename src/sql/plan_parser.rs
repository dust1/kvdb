use sqlparser::ast::Statement;

use crate::{error::{Result, Error}, common::PlanNode};

use super::NBParser;


pub struct PlanParser;

impl PlanParser {

    pub fn parser(sql: &str) -> Result<PlanNode> {
        let stmts = NBParser::parser_sql(sql)?;
        PlanParser::build_plan(stmts)
    }

    pub fn build_plan(mut stmts: Vec<Statement>) -> Result<PlanNode> {
        if stmts.len() != 1 {
            return Err(Error::Internal("Only support single query".into()));
        }

        PlanParser::build_sql_plan(stmts.remove(0))
    }

    pub fn build_sql_plan(stmt: Statement) -> Result<PlanNode> {
        match stmt {
            _ => todo!()
        }
    }

    
}