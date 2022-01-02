use sqlparser::ast::{
    Assignment, ColumnDef, Expr, Ident, ObjectName, ObjectType, Query, Statement,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KVStatement {
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: ObjectName,
        /// WHERE
        selection: Option<Expr>,
    },
    /// CREATE TABLE
    CreateTable {
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
    },
    /// DROP TABLE
    DropTable { names: Vec<ObjectName> },
}

impl KVStatement {
    pub fn build_statement(statement: Statement) -> Self {
        match statement {
            Statement::Query(query) => KVStatement::Query(query),
            Statement::Insert { table_name, columns, source, .. } => {
                KVStatement::Insert { table_name, columns, source }
            }
            Statement::Update { table_name, assignments, selection, .. } => {
                KVStatement::Update { table_name, assignments, selection }
            }
            Statement::Delete { table_name, selection, .. } => {
                KVStatement::Delete { table_name, selection }
            }
            Statement::CreateTable { name, columns, .. } => {
                KVStatement::CreateTable { name, columns }
            }
            Statement::Drop { names, object_type, .. } => match object_type {
                ObjectType::Table => KVStatement::DropTable { names },
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}
