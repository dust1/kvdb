use serde_derive::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};


use crate::error::{Error, Result};
use crate::sql::types::{Row, Value};

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
            Self::Wildcard => "*".to_string(),
        };
        write!(f, "{}", s)
    }
}

impl Expression {
    /// evaluate an expression to a value
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // constant value
            Self::Constant(c) => c.clone(),
            Self::Field(i, _) => row.and_then(|row| row.get(*i).cloned()).unwrap_or(Null),
            Self::Wildcard => Null,
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) < rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs < (rhs as f64)),
                (Float(lhs), Float(rhs)) => Boolean(lhs < rhs),
                (String(lhs), String(rhs)) => Boolean(lhs < rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!(
                        "Can't compare less than {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) > rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs > (rhs as f64)),
                (Float(lhs), Float(rhs)) => Boolean(lhs > rhs),
                (String(lhs), String(rhs)) => Boolean(lhs > rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!(
                        "Can't compare greater than {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::IsNull(expr) => match expr.evaluate(row)? {
                Null => Boolean(true),
                _ => Boolean(false),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(lhs), Null) if lhs => Boolean(true),
                (Null, Boolean(rhs)) if rhs => Boolean(true),
                (Null, Boolean(_)) | (Boolean(_), Null) | (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't or {} and {}", lhs, rhs))),
            },
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(lhs), Null) if !lhs => Boolean(false),
                (Null, Boolean(rhs)) if !rhs => Boolean(false),
                (Boolean(_), Null) | (Null, Boolean(_)) | (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!("Can't compare {} and {}", lhs, rhs)))
                }
            },
            Self::Not(expr) => match expr.evaluate(row)? {
                Boolean(b) => Boolean(!b),
                Null => Null,
                other => return Err(Error::Internal(format!("Can't not {} ", other))),
            },
            Self::Assert(expr) => match expr.evaluate(row)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => {
                    return Err(Error::Internal(format!("Can't take the positive of {}", expr)))
                }
            },
            // here determines our calculation rules
            // e.g. 1 + NULL = 1 or 1 + NULL = NULL
            Self::Add(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_add(rhs).ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Integer(_), Null) | (Null, Integer(_)) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(_), Null) | (Null, Float(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!("Can't add the {} and {}", lhs, rhs)))
                }
            },
            e => return Err(Error::Internal(format!("Unsupport expression evaluate: {}", e))),
        })
    }
}
