use super::statements::{KVInsertStatement, KVQueryStatement};

pub enum KVStatement {
    Query(KVQueryStatement),
    Insert(KVInsertStatement),
}
