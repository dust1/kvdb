use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;

use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::Expr;
use sqlparser::ast::Value;

use super::data_type::DataType;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Eq for DataValue {}

impl Hash for DataValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data_type().hash(state);
        match self {
            DataValue::Null => self.hash(state),
            DataValue::Boolean(v) => v.hash(state),
            DataValue::Integer(v) => v.hash(state),
            DataValue::Float(v) => v.to_be_bytes().hash(state),
            DataValue::String(v) => v.hash(state),
        }
    }
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

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            DataValue::Boolean(_) => Some(DataType::Boolean),
            DataValue::Integer(_) => Some(DataType::Integer),
            DataValue::Float(_) => Some(DataType::Float),
            DataValue::String(_) => Some(DataType::String),
            DataValue::Null => None,
        }
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
