use std::time::Instant;

use sqlparser::{ast::Statement, dialect::{GenericDialect, keywords::Keyword, Dialect}, parser::Parser, tokenizer::{Tokenizer, Token}};

use crate::error::{Result, Error};

use super::sql_statement::KVStatement;

/// Newbee DB's Parser
pub struct KVParser<'a> {
    parser: Parser<'a>
}

impl<'a> KVParser<'a> {

    pub fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(KVParser {
            parser: Parser::new(tokens, dialect)
        })
    }

    /// parser sql
    pub fn parser_sql(sql: &str) -> Result<Vec<KVStatement>> {
        let dialect = &GenericDialect {};
        Parser::parse_sql(dialect, sql)?
        .iter()
        .map(KVParser::parse_statement)
        .collect::<Result<Vec<_>>>()
    }

    pub fn parse_statement(stmt: &Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Query(_) => KVParser::parse_query(stmt),
            _ => todo!()
        }
    }

    fn parse_query(stmt: &Statement) -> Result<KVStatement> {
        todo!()
    }

    fn expected(&self, expected: &str, found: Token) -> Result<KVStatement> {
        Err(Error::Internal(format!(
            "Expected {}, found: {}", 
            expected, found
        )))
    }

}