pub mod kv;

use crate::error::Result;





/// the sql engine interface
pub trait Engine: Clone {
    // begin a session
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session { engine: self.clone()})
    }
}

/// An Sql Session
pub struct Session<E: Engine> {
    /// the sql engine
    engine: E,
}
