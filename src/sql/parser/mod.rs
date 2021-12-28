pub mod ast;
mod translate;

use sqlparser::ast::{ColumnDef, ObjectType, Statement};
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;

use crate::error::*;
use crate::sql::parser::ast::KVStatement;
use translate::*;

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
        match &self.statements[0] {
            Statement::CreateTable {name, columns, query, .. } => {
                Ok(KVStatement::CreateTable {
                    name: translate_object_name(name),
                    columns: columns.iter()
                        .map(translate_column_def)
                        .collect::<Vec<_>>()
                })
            }
            Statement::Drop { object_type, names, .. } => {
                if !ObjectType::Table.eq(object_type) {
                    return Err(Error::Parse("can not drop this.".to_string()));
                }
                Ok(KVStatement::DropTable {
                    names: names.iter().map(translate_object_name).collect::<Vec<_>>()
                })
            }
            Statement::Query(query) => {
                Ok(KVStatement::Query(Box::new(translate_query(query))))
            }
            Statement::Insert { table_name, columns, source, .. } => {
                Ok(KVStatement::Insert {
                    table_name: translate_object_name(table_name),
                    columns: columns.iter().map(translate_ident).collect::<Vec<_>>(),
                    source: Box::new(translate_query(source))
                })
            }
            Statement::Update { table_name, assignments, selection, .. } => {
                let kv_selection = match selection {
                    Some(expr) => Some(translate_expr(expr)),
                    None => None
                };
                Ok(KVStatement::Update {
                    table_name: translate_object_name(table_name),
                    assignments: assignments.iter().map(translate_assignment).collect::<Vec<_>>(),
                    selection: kv_selection
                })
            }
            Statement::Delete { table_name, selection } => {
                let kv_selection = match selection {
                    Some(expr) => Some(translate_expr(expr)),
                    None => None
                };
                Ok(KVStatement::Delete {
                    table_name: translate_object_name(table_name),
                    selection: kv_selection
                })
            }
            _ => Err(Error::Parse("Not support this sql!!".to_string())),
        }
    }

}
