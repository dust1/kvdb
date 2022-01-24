use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

use crate::error::Result;

/// Newbee DB's Parser
pub struct NBParser;

impl NBParser {
    /// parser sql
    pub fn parser_sql(sql: &str) -> Result<Vec<Statement>> {
        let dialect = GenericDialect {};
        Ok(Parser::parse_sql(&dialect, sql)?)
    }
}

