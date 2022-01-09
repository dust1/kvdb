pub mod kv;

use crate::error::Result;
use crate::sql::types::Row;

/// a row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// the sql engine interface
pub trait Engine: Clone {
    // begin a session
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session { engine: self.clone() })
    }
}

/// An Sql Session
pub struct Session<E: Engine> {
    /// the sql engine
    engine: E,
}
