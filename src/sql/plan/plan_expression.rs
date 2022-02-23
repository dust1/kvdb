use std::fmt::Display;
use std::fmt::Formatter;

use regex::Regex;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::UnaryOperator;

use crate::common::result::DataRow;
use crate::common::scope::Scope;
use crate::error::Error;
use crate::error::Result;
use crate::sql::schema::data_value::DataValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // Values
    Constant(DataValue),
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
    pub fn from_query(query: &Query) -> Result<Vec<Vec<Expression>>> {
        let mut scope = Scope::new();
        if let SetExpr::Values(values) = &query.body {
            return values
                .0
                .iter()
                .map(|items| {
                    items
                        .iter()
                        .map(|expr| Expression::from_expr(expr, &mut scope))
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>();
        }

        Err(Error::Value(format!(
            "Un support insert by this query: {}",
            query
        )))
    }

    pub fn from_select_item(
        select: &SelectItem,
        scope: &mut Scope,
    ) -> Result<(Expression, Option<String>)> {
        match select {
            SelectItem::UnnamedExpr(expr) => Ok((Expression::from_expr(expr, scope)?, None)),
            SelectItem::ExprWithAlias { expr, alias } => {
                Ok((Expression::from_expr(expr, scope)?, Some(alias.to_string())))
            }
            SelectItem::Wildcard => Ok((Expression::Wildcard, None)),
            SelectItem::QualifiedWildcard(_) => Err(Error::Value(
                "can not support alias.* or even schema.table.*".to_string(),
            )),
        }
    }

    pub fn from_expr(expr: &Expr, scope: &mut Scope) -> Result<Expression> {
        use Expression::*;
        Ok(match expr {
            Expr::Value(literal) => Constant(DataValue::from_value(literal)),
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Or => Or(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::And => And(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::Eq => Equal(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::NotEq => Not(Equal(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                )
                .into()),
                BinaryOperator::Gt => GreaterThan(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::GtEq => Or(
                    GreaterThan(
                        Expression::from_expr(left, scope)?.into(),
                        Expression::from_expr(right, scope)?.into(),
                    )
                    .into(),
                    Equal(
                        Expression::from_expr(left, scope)?.into(),
                        Expression::from_expr(right, scope)?.into(),
                    )
                    .into(),
                ),
                BinaryOperator::Lt => LessThan(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::LtEq => Or(
                    LessThan(
                        Expression::from_expr(left, scope)?.into(),
                        Expression::from_expr(right, scope)?.into(),
                    )
                    .into(),
                    Equal(
                        Expression::from_expr(left, scope)?.into(),
                        Expression::from_expr(right, scope)?.into(),
                    )
                    .into(),
                ),
                BinaryOperator::Like => Like(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::Plus => Add(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::Minus => Subtract(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::Multiply => Multiply(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                BinaryOperator::Divide => Divide(
                    Expression::from_expr(left, scope)?.into(),
                    Expression::from_expr(right, scope)?.into(),
                ),
                _ => todo!(),
            },
            Expr::UnaryOp { expr, op } => match op {
                UnaryOperator::Not => Not(Expression::from_expr(expr, scope)?.into()),
                _ => todo!(),
            },
            Expr::IsNull(expr) => IsNull(Expression::from_expr(expr, scope)?.into()),
            Expr::Identifier(ident) => Field(
                scope.resolve(None, &ident.to_string())?,
                Some((None, ident.to_string())),
            ),
            Expr::CompoundIdentifier(idents) => {
                let idents: &Vec<Ident> = idents;
                if idents.len() == 2 {
                    let table = &idents[0];
                    let name = &idents[1];
                    Field(
                        scope.resolve(Some(&table.to_string()), &name.to_string())?,
                        Some((Some(table.to_string()), name.to_string())),
                    )
                } else if idents.len() == 1 {
                    let name = &idents[0];
                    Field(
                        scope.resolve(None, &name.to_string())?,
                        Some((None, name.to_string())),
                    )
                } else {
                    return Err(Error::Value(format!("Unsupported SQL statement. {}", expr)));
                }
            }
            Expr::Wildcard => Wildcard,
            _ => todo!(),
        })
    }

    /// evaluate an expression to a value
    pub fn evaluate(&self, row: Option<&DataRow>) -> Result<DataValue> {
        use DataValue::*;
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
                    return Err(Error::Internal(format!(
                        "Can't compare {} and {}",
                        lhs, rhs
                    )))
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
                    return Err(Error::Internal(format!(
                        "Can't compare {} and {}",
                        lhs, rhs
                    )))
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
                    return Err(Error::Internal(format!(
                        "Can't take the positive of {}",
                        expr
                    )))
                }
            },
            // here determines our calculation rules
            // e.g. 1 + NULL = 1 or 1 + NULL = NULL
            Self::Add(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_add(rhs)
                        .ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 + rhs),
                (Integer(_), Null) | (Null, Integer(_)) => Null,
                (Float(lhs), Integer(rhs)) => Float(lhs + rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(_), Null) | (Null, Float(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!(
                        "Can't add the {} and {}",
                        lhs, rhs
                    )))
                }
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Float(lhs as f64 / rhs as f64),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 / rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs / rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs / rhs),
                (Integer(_), Null) | (Null, Integer(_)) | (Float(_), Null) | (Null, Float(_)) => {
                    Null
                }
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't get {}/{}", lhs, rhs))),
            },
            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs ^ rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't get {} ^ {}", lhs, rhs))),
            },
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(expr) => Integer(!expr),
                Boolean(expr) => Boolean(!expr),
                Null => Null,
                expr => return Err(Error::Internal(format!("Can't get !{}", expr))),
            },
            Self::Modulo(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(lhs % rhs),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 % rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs % rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs % rhs),
                (_, Null) | (Null, _) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't get {} % {}", lhs, rhs))),
            },
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_mul(rhs)
                        .ok_or_else(|| Error::Internal("multiply overflow".into()))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 * rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs * rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't get {} * {}", lhs, rhs))),
            },
            Self::Negate(expr) => match expr.evaluate(row)? {
                Integer(expr) => Integer(
                    expr.checked_neg()
                        .ok_or_else(|| Error::Value(format!("{} overflow", expr)))?,
                ),
                Float(expr) => Float(-expr),
                Null => Null,
                expr => return Err(Error::Internal(format!("Cant't get -{}", expr))),
            },
            Self::Subtract(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => Integer(
                    lhs.checked_sub(rhs)
                        .ok_or_else(|| Error::Value(format!("overflow with {} - {}", lhs, rhs)))?,
                ),
                (Integer(lhs), Float(rhs)) => Float(lhs as f64 - rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs - rhs as f64),
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => return Err(Error::Internal(format!("Can't get {} - {}", lhs, rhs))),
            },
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => Boolean(
                    Regex::new(&format!(
                        "^{}$",
                        regex::escape(&rhs)
                            .replace("%", ".*")
                            .replace(".*.*", "%")
                            .replace("_", ".")
                            .replace("..", "_")
                    ))?
                    .is_match(&lhs),
                ),
                (String(_), Null) | (Null, String(_)) => Null,
                (lhs, rhs) => {
                    return Err(Error::Internal(format!("Can't get {} like {}", lhs, rhs)))
                }
            },
        })
    }
}
