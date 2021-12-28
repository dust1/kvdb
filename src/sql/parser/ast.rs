use sqlparser::ast::{AlterTableOperation, Assignment, ColumnDef, Expr, Ident, ObjectName, Query};

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
    DropTable {
        names: Vec<ObjectName>,
    },
}
