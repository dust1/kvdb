pub mod ast;
pub mod translate;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::*;
use crate::sql::parser::ast::KVStatement;

#[derive(Debug)]
pub struct KVParser {
    statements: Vec<Statement>,
}

impl KVParser {
    pub fn build(sql: &str) -> Result<Self> {
        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, sql) {
            Ok(statements) => Ok(Self { statements }),
            Err(sql_err) => Err(Error::Parse(sql_err.to_string())),
        }
    }

    pub fn parser(&self) -> Result<KVStatement> {
        Ok(KVStatement::build_statement(self.statements[0].clone()))
    }
}
