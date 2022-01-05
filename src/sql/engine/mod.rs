pub mod kv;

use std::collections::HashSet;
use crate::error::Result;
use crate::sql::schema::Catalog;
use crate::sql::types::{Row, Value};
use crate::sql::types::expression::Expression;

/// the sql engine interface
pub trait Engine: Clone {
    // begin a session
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            txn: None
        })
    }
}
