use crate::error::Error;
use crate::error::Result;

/// Return values for sqlite_exec()
#[derive(PartialEq, Clone, Copy, Debug)]
pub(super) enum SQLExecValue {
    OK,
    ERROR,
    INTERNAL,
    PERM,
    ABORT,
    BUSY,
    LOCKED,
    NOMEM,
    READONLY,
    INTERRUPT,
    IOERR,
    CORRUPT,
    NOTFOUND,
    FULL,
    CANTOPEN,
    PROTOCOL,
    EMPTY,
    SCHEMA,
    TOOBIG,
    CONSIRAINT,
    MISMATCH,
    MISUSE,
}

pub(super) fn error_values(value: SQLExecValue) -> Error {
    use SQLExecValue::*;
    match &value {
        OK => Error::Value("ok can not used it".into()),
        err => Error::Internal(match err {
            ERROR => "SQL Error or missing database".into(),
            INTERNAL => "An internal logic error in SQL".into(),
            PERM => "Access permission denied".into(),
            ABORT => "Callback routine requested an abort".into(),
            BUSY => "The database file is locked".into(),
            LOCKED => "A table in the database is locked".into(),
            NOMEM => "A malloc() fail".into(),
            READONLY => "Attempt to write a readonly database".into(),
            INTERRUPT => "Operation terminated by Pager::interrupt()".into(),
            IOERR => "Some kind of disk I/O error occurred".into(),
            CORRUPT => "The database disk image is malformed".into(),
            NOTFOUND => "(Internal Only) Table or record not found".into(),
            FULL => "Insertion failed because database is full".into(),
            CANTOPEN => "Unable to open the database file".into(),
            PROTOCOL => "Database lock protocol error".into(),
            EMPTY => "(Internal Only) Database table is empty".into(),
            SCHEMA => "The database schema changed".into(),
            TOOBIG => "Too much data for one row of a table".into(),
            CONSIRAINT => "Abort due to contraint violation".into(),
            MISMATCH => "Data type mismatch".into(),
            MISUSE => "Library used incorrectly".into(),
            _ => todo!(),
        }),
    }
}
