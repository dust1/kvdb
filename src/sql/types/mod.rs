pub mod expression;

use serde_derive::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use sqlparser::ast::Expr;
use sqlparser::ast::Value as ExprValue;
use crate::error::Result;

/// a row of values
pub type Row = Vec<Value>;

/// a row of iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

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
            _ => Value::Null
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

impl DataType {
    pub fn new(data_type: &sqlparser::ast::DataType) -> Self {
        use sqlparser::ast;
        match data_type {
            ast::DataType::Char(_) | ast::DataType::Varchar(_) | ast::DataType::String | ast::DataType::Text => DataType::String,
            ast::DataType::Int(_) | ast::DataType::BigInt(_) | ast::DataType::TinyInt(_) | ast::DataType::SmallInt(_) => DataType::Integer,
            ast::DataType::Float(_) => DataType::Float,
            ast::DataType::Boolean => DataType::Boolean,
            _ => DataType::String
        }
    }
}
