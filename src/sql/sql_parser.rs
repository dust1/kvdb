use sqlparser::{
    ast::{ObjectName, ObjectType, Query, Statement},
    dialect::{GenericDialect},
    parser::Parser,
};

use crate::{
    error::{Error, Result},
    sql::statements::{KVInsertStatement, KVCreateTableStatement, KVUpdateStatement},
};

use super::{sql_statement::KVStatement, statements::{KVQueryStatement, KVDropTableStatement, KVDeleteStatement}};

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

    fn parse_drop_table(if_exists: bool, names: Vec<ObjectName>) -> Result<KVStatement> {
        Ok(KVStatement::DropTable(KVDropTableStatement {
            if_exists,
            names
        }))
    }

    fn parse_create_table(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                table_properties,
                query,
                like,
                ..
            } => {
                Ok(KVStatement::CreateTable(KVCreateTableStatement {
                    if_not_exists,
                    name,
                    columns,
                    config: table_properties,
                    query,
                    like
                }))
            },
            _ => parser_err!("Expect set create table statement"),
        }
    }

    fn parse_delete(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Delete {
                table_name,
                selection
            } => Ok(KVStatement::Delete(KVDeleteStatement {
                table_name,
                selection
            })),
            _ => parser_err!("Expect set create table statement"),
        }
    }

    fn parse_update(stmt: Statement) -> Result<KVStatement> {
        match stmt {
            Statement::Update {
                table_name,
                assignments,
                selection
            } => Ok(KVStatement::Update(KVUpdateStatement {
                table_name,
                assignments,
                selection
            })),
            _ => parser_err!("Expect set create table statement"),
        }
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

}
