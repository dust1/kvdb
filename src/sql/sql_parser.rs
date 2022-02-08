


use sqlparser::{
    ast::{ObjectName, ObjectType, Query, Statement},
    dialect::{GenericDialect},
    parser::Parser,
};

use crate::{
    error::{Error, Result},
    sql::statements::KVInsertStatement,
};

use super::{sql_statement::KVStatement, statements::KVQueryStatement};

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(Error::Parse($MSG.to_string().into()))
    };
}

macro_rules! internal_err {
    ($EXPECTED:expr, $FOUND:expr) => {
        Err(Error::Internal(format!("Internal: {}, found: {}", $EXPECTED, $FOUND)))
    };
}

/// Newbee DB's Parser
pub struct KVParser;

impl KVParser {
    /// parser sql
    pub fn parser_sql(sql: &str) -> Result<Vec<KVStatement>> {
        let dialect = &GenericDialect {};
        Parser::parse_sql(dialect, sql)?
            .into_iter()
            .map(KVParser::parse_statement)
            .collect::<Result<Vec<_>>>()
    }

    pub fn parse_statement(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Query(query) => KVParser::parse_query(*query),
            Statement::Insert { .. } => KVParser::parse_insert(stmt),
            Statement::Update { .. } => KVParser::parse_update(stmt),
            Statement::Delete { .. } => KVParser::parse_delete(stmt),
            Statement::CreateTable { .. } => KVParser::parse_create_table(stmt),
            Statement::Drop { .. } => KVParser::parse_drop(stmt),
            s => internal_err!("an SQL statement", s),
        }
    }

    fn parse_drop(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Drop { object_type, if_exists, names, .. } => match object_type {
                ObjectType::Table => KVParser::parse_drop_table(if_exists, names),
                t => internal_err!("an SQL Drop Type", t),
            },
            _ => parser_err!("Expect set insert statement"),
        }
    }

    fn parse_drop_table(_if_exists: bool, _names: Vec<ObjectName>) -> Result<KVStatement> {
        todo!()
    }

    fn parse_create_table(_stmt: Statement) -> Result<KVStatement> {
        todo!()
    }

    fn parse_delete(_stmt: Statement) -> Result<KVStatement> {
        todo!()
    }

    fn parse_update(_stmt: Statement) -> Result<KVStatement> {
        todo!()
    }

    /// parse query
    fn parse_query(query: Query) -> Result<KVStatement> {
        Ok(KVStatement::Query(KVQueryStatement::try_from(query)?))
    }

    fn parse_insert(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Insert {
                or,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
            } => Ok(KVStatement::Insert(KVInsertStatement {
                or,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
            })),
            _ => parser_err!("Expect set insert statement"),
        }
    }

    fn expected(expected: &str, found: Statement) -> Result<KVStatement> {
        Err(Error::Internal(format!("Expected {}, found: {}", expected, found)))
    }
}
