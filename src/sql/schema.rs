use sqlparser::ast::{ColumnDef, ObjectName};
use crate::error::Result;
use crate::sql::parser::translate::translate_object_name_to_string;

///TODO The catalog stores schema information
pub trait Catalog {
    //TODO
}

/// a table schema
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl Table {
    pub fn new(name: ObjectName, columns: Vec<ColumnDef>) -> Result<Table> {
        let table_name = translate_object_name_to_string(&name)?.clone();
        Ok(Table {
            name: table_name,
            columns
        })
    }

}
