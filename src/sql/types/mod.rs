pub mod expression;

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
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
