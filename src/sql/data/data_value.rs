use std::fmt::Display;

use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl DataValue {
    pub fn from_expr(expr: &Expr) -> Self {
        match expr {
            Expr::Value(value) => Self::from_value(value),
            _ => Self::Null,
        }
    }

    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Number(n, _) => DataValue::parse_number(n),
            Value::SingleQuotedString(ref s)
            | Value::NationalStringLiteral(ref s)
            | Value::HexStringLiteral(ref s)
            | Value::DoubleQuotedString(ref s) => DataValue::parse_string(s),
            Value::Boolean(b) => DataValue::Boolean(*b),
            Value::Null => DataValue::Null,
            _ => DataValue::Null,
        }
    }

    pub fn parse_number(s: &str) -> Self {
        match s.parse::<i64>() {
            Ok(n) => DataValue::Integer(n),
            Err(_) => DataValue::Float(s.parse::<f64>().unwrap()),
        }
    }

    pub fn parse_string(s: &str) -> Self {
        Self::String(s.to_owned())
    }
}

impl Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
