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
            Expr::Value(value) => Self::from_expr_value(value),
            _ => Self::Null,
        }
    }

    pub fn from_expr_value(expr_value: &Value) -> Self {
        match expr_value {
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
