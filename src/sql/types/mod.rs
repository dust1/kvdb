pub mod expression;

use crate::error::Result;
use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::Expr;
use sqlparser::ast::Value as ExprValue;
use std::fmt::{Display, Formatter};

/// a row of values
pub type Row = Vec<Value>;

/// a row of iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

/// a column (in a result set, see schema::Column for table columns)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: Option<String>,
}

/// a set of columns
pub type Columns = Vec<Column>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl Value {
    pub fn from_expr(expr: &Expr) -> Self {
        if let Expr::Value(expr_value) = expr {
            return Self::from_expr_value(expr_value);
        }
        Value::Null
    }

    pub fn from_expr_value(expr_value: &ExprValue) -> Self {
        match expr_value {
            ExprValue::Number(n, _) => Value::parse_number(n),
            ExprValue::SingleQuotedString(ref s)
            | ExprValue::NationalStringLiteral(ref s)
            | ExprValue::HexStringLiteral(ref s)
            | ExprValue::DoubleQuotedString(ref s) => Value::parse_string(s),
            ExprValue::Boolean(b) => Value::Boolean(*b),
            ExprValue::Null => Value::Null,
            _ => Value::Null,
        }
    }

    pub fn parse_number(n: &str) -> Value {
        match n.parse::<i64>() {
            Ok(n) => Value::Integer(n),
            Err(_) => Value::Float(n.parse::<f64>().unwrap()),
        }
    }

    pub fn parse_string(n: &str) -> Value {
        Value::String(n.to_owned())
    }

    /// Returns the value's datatype, or None for null values
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::String(_) => Some(DataType::String),
            Self::Float(_) => Some(DataType::Float),
            Self::Integer(_) => Some(DataType::Integer),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "STRING",
            Self::Boolean => "BOOLEAN",
        })
    }
}

impl DataType {
    pub fn new(data_type: &sqlparser::ast::DataType) -> Self {
        use sqlparser::ast;
        match data_type {
            ast::DataType::Char(_)
            | ast::DataType::Varchar(_)
            | ast::DataType::String
            | ast::DataType::Text => DataType::String,
            ast::DataType::Int(_)
            | ast::DataType::BigInt(_)
            | ast::DataType::TinyInt(_)
            | ast::DataType::SmallInt(_) => DataType::Integer,
            ast::DataType::Float(_) => DataType::Float,
            ast::DataType::Boolean => DataType::Boolean,
            _ => DataType::String,
        }
    }
}
