use super::statements::{KVInsertStatement, KVQueryStatement, KVDropTableStatement, KVCreateTableStatement, KVDeleteStatement, KVUpdateStatement};

pub enum KVStatement {
    Query(KVQueryStatement),
    Insert(KVInsertStatement),
    DropTable(KVDropTableStatement),
    CreateTable(KVCreateTableStatement),
    Delete(KVDeleteStatement),
    Update(KVUpdateStatement),
}
