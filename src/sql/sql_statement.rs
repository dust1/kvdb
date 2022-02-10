use super::statements::{
    KVCreateTableStatement, KVDeleteStatement, KVDropTableStatement, KVInsertStatement,
    KVQueryStatement, KVUpdateStatement,
};

pub enum KVStatement {
    Query(KVQueryStatement),
    Insert(KVInsertStatement),
    DropTable(KVDropTableStatement),
    CreateTable(KVCreateTableStatement),
    Delete(KVDeleteStatement),
    Update(KVUpdateStatement),
}
