use super::statements::KVCreateTableStatement;
use super::statements::KVDeleteStatement;
use super::statements::KVDropTableStatement;
use super::statements::KVInsertStatement;
use super::statements::KVQueryStatement;
use super::statements::KVUpdateStatement;

pub enum KVStatement {
    Query(KVQueryStatement),
    Insert(KVInsertStatement),
    DropTable(KVDropTableStatement),
    CreateTable(KVCreateTableStatement),
    Delete(KVDeleteStatement),
    Update(KVUpdateStatement),
}
