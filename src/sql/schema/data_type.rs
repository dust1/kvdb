use std::fmt::Display;

use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::DataType as SQLDataType;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl DataType {
    pub fn try_form(t: &SQLDataType) -> Self {
        match t {
            SQLDataType::Boolean => DataType::Boolean,
            SQLDataType::Int(_)
            | SQLDataType::BigInt(_)
            | SQLDataType::TinyInt(_)
            | SQLDataType::SmallInt(_) => DataType::Integer,
            SQLDataType::Float(_) => DataType::Float,
            _ => DataType::String,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DataType::Boolean => "Boolean",
            DataType::Float => "Float",
            DataType::Integer => "Integer",
            DataType::String => "String",
        })
    }
}
