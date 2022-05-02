use crate::error::Error;

pub(super) const ERR_FULL: u8 = 0x01;
pub(super) const ERR_MEM: u8 = 0x02;
pub(super) const ERR_LOCK: u8 = 0x03;
pub(super) const ERR_CORRUPT: u8 = 0x04;
pub(super) const ERR_DISK: u8 = 0x05;

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

impl SQLExecValue {
    pub fn to_bit(&self) -> u8 {
        use SQLExecValue::*;
        match self {
            OK => 0,
            ERROR => 1,
            INTERNAL => 2,
            PERM => 3,
            ABORT => 4,
            BUSY => 5,
            LOCKED => 6,
            NOMEM => 7,
            READONLY => 8,
            INTERRUPT => 9,
            IOERR => 10,
            CORRUPT => 11,
            NOTFOUND => 12,
            FULL => 13,
            CANTOPEN => 14,
            PROTOCOL => 15,
            EMPTY => 16,
            SCHEMA => 17,
            TOOBIG => 18,
            CONSIRAINT => 19,
            MISMATCH => 20,
            MISUSE => 21,
        }
    }

    pub fn from_bit(bit: u8) -> SQLExecValue {
        use SQLExecValue::*;
        match bit {
            0 => OK,
            1 => ERROR,
            2 => INTERNAL,
            3 => PERM,
            4 => ABORT,
            5 => BUSY,
            6 => LOCKED,
            7 => NOMEM,
            8 => READONLY,
            9 => INTERRUPT,
            10 => IOERR,
            11 => CORRUPT,
            12 => NOTFOUND,
            13 => FULL,
            14 => CANTOPEN,
            15 => PROTOCOL,
            16 => EMPTY,
            17 => SCHEMA,
            18 => TOOBIG,
            19 => CONSIRAINT,
            20 => MISMATCH,
            21 => MISUSE,
            _ => OK,
        }
    }
}

pub(super) fn page_errorcode(err_mask: u8) -> Result<(), Error> {
    let mut rc = SQLExecValue::OK;
    if err_mask & ERR_LOCK != 0 {
        rc = SQLExecValue::PROTOCOL;
    }
    if err_mask & ERR_DISK != 0 {
        rc = SQLExecValue::IOERR;
    }
    if err_mask & ERR_FULL != 0 {
        rc = SQLExecValue::FULL;
    }
    if err_mask & ERR_MEM != 0 {
        rc = SQLExecValue::NOMEM;
    }
    if err_mask & ERR_CORRUPT != 0 {
        rc = SQLExecValue::CORRUPT;
    }
    if rc == SQLExecValue::OK {
        Ok(())
    } else {
        Err(error_values(rc))
    }
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
