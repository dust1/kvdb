use sqlparser::ast::ColumnDef;

///TODO The catalog stores schema information
pub trait Catalog {
    //TODO
}

/// a table schema
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>
}

impl Table {

}