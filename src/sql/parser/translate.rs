use sqlparser::ast::Assignment;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;

use crate::error::Error;
use crate::error::Result;

pub fn translate_object_name(sql_object_name: &ObjectName) -> ObjectName {
    sql_object_name.clone()
}

pub fn translate_object_name_to_string(object_name: &ObjectName) -> Result<String> {
    // todo can print object_name
    object_name
        .0
        .iter()
        .map(|ident| ident.value.clone())
        .last()
        .ok_or_else(|| Error::Parse("can not translate objectName to String".to_string()))
}

pub fn translate_ident(ident: &Ident) -> Ident {
    ident.clone()
}

pub fn translate_column_def(sql_column_def: &ColumnDef) -> ColumnDef {
    sql_column_def.clone()
}

pub fn translate_query(query: &Query) -> Query {
    query.clone()
}

pub fn translate_assignment(assignment: &Assignment) -> Assignment {
    assignment.clone()
}

pub fn translate_expr(expr: &Expr) -> Expr {
    expr.clone()
}
