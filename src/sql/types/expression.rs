use serde_derive::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use crate::sql::types::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // Values
    Constant(Value),
    Field(usize, Option<(Option<String>, String)>),
    Wildcard,

    // Logical operations
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparisons operations (GTE, LTE, and NEQ are composite operations)
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    // Mathematical operations
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operations
    Like(Box<Expression>, Box<Expression>),
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Constant(v) => v.to_string(),
            Self::Field(i, None) => format!("#{}", i),
            Self::Field(_, Some((None, name))) => name.to_string(),
            Self::Field(_, Some((Some(table), name))) => format!("{}.{}", table, name),

            Self::And(lhs, rhs) => format!("{} AND {}", lhs, rhs),
            Self::Or(lhs, rhs) => format!("{} OR {}", lhs, rhs),
            Self::Not(expr) => format!("NOT {}", expr),

            Self::Equal(lhs, rhs) => format!("{} = {}", lhs, rhs),
            Self::GreaterThan(lhs, rhs) => format!("{} > {}", lhs, rhs),
            Self::LessThan(lhs, rhs) => format!("{} < {}", lhs, rhs),
            Self::IsNull(expr) => format!("{} IS NULL", expr),

            Self::Add(lhs, rhs) => format!("{} + {}", lhs, rhs),
            Self::Assert(expr) => expr.to_string(),
            Self::Divide(lhs, rhs) => format!("{} / {}", lhs, rhs),
            Self::Exponentiate(lhs, rhs) => format!("{} ^ {}", lhs, rhs),
            Self::Factorial(expr) => format!("!{}", expr),
            Self::Modulo(lhs, rhs) => format!("{} % {}", lhs, rhs),
            Self::Multiply(lhs, rhs) => format!("{} * {}", lhs, rhs),
            Self::Negate(expr) => format!("-{}", expr),
            Self::Subtract(lhs, rhs) => format!("{} - {}", lhs, rhs),

            Self::Like(lhs, rhs) => format!("{} LIKE {}", lhs, rhs),
            Self::Wildcard => format!("*"),
        };
        write!(f, "{}", s)
    }
}
