use std::cmp::Ordering;

use crate::error::Error;
use crate::error::Result;
use crate::sql::execution::Executor;
use crate::sql::execution::ResultSet;
use crate::sql::plan::Direction;
use crate::sql::plan::planners::Expression;
use crate::sql::schema::Catalog;
use crate::sql::types::Column;
use crate::sql::types::Row;
use crate::sql::types::Value;

/// a filter executor
pub struct Filter<C: Catalog> {
    source: Box<dyn Executor<C>>,
    predicate: Expression,
}

/// a projection executor
pub struct Projection<C: Catalog> {
    source: Box<dyn Executor<C>>,
    expressions: Vec<(Expression, Option<String>)>,
}

pub struct Order<C: Catalog> {
    source: Box<dyn Executor<C>>,
    orders: Vec<(Expression, Direction)>,
}

pub struct GroupBy<C: Catalog> {
    source: Box<dyn Executor<C>>,
    groups: Vec<Expression>,
}

pub struct Limit<C: Catalog> {
    source: Box<dyn Executor<C>>,
    offset: usize,
    limit: Option<usize>,
}

impl<C: Catalog> Order<C> {
    pub fn new(source: Box<dyn Executor<C>>, orders: Vec<(Expression, Direction)>) -> Box<Self> {
        Box::new(Self { source, orders })
    }
}

impl<C: Catalog> Limit<C> {
    pub fn new(source: Box<dyn Executor<C>>, offset: usize, limit: Option<usize>) -> Box<Self> {
        Box::new(Self {
            source,
            offset,
            limit,
        })
    }

    fn check_and_sub(&mut self) -> bool {
        match self.limit {
            Some(l) => match l {
                0 => false,
                n => {
                    self.limit = Some(n - 1);
                    true
                }
            },
            None => true,
        }
    }
}

impl<C: Catalog> GroupBy<C> {
    pub fn new(source: Box<dyn Executor<C>>, groups: Vec<Expression>) -> Box<Self> {
        Box::new(Self { source, groups })
    }
}

impl<C: Catalog> Executor<C> for Limit<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        match self.source.execute(catalog)? {
            ResultSet::Query { columns, rows } => {
                let new_rows = rows.into_iter().skip(self.offset.clone());
                if let Some(limit) = self.limit {
                    return Ok(ResultSet::Query {
                        columns,
                        rows: Box::new(new_rows.take(limit)),
                    });
                }

                Ok(ResultSet::Query {
                    columns,
                    rows: Box::new(new_rows),
                })
            }
            r => Err(Error::Internal(format!("Can't LIMIT {}", r))),
        }
    }
}

impl<C: Catalog> Executor<C> for GroupBy<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        match self.source.execute(catalog)? {
            ResultSet::Query {
                columns: _,
                rows: _,
            } => {
                todo!()
            }
            r => Err(Error::Internal(format!("Can't GROUP BY {}", r))),
        }
    }
}

impl<C: Catalog> Executor<C> for Order<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        match self.source.execute(catalog)? {
            ResultSet::Query { columns, rows } => {
                let mut row_array = rows.collect::<Result<Vec<Row>>>()?;
                row_array.sort_by(|a, b| {
                    let mut sort = Ordering::Less;
                    for (expr, direction) in &self.orders {
                        sort = match (
                            expr.evaluate(Some(a)).unwrap_or(Value::Null),
                            expr.evaluate(Some(b)).unwrap_or(Value::Null),
                        ) {
                            (Value::Integer(a), Value::Integer(b)) => match a.cmp(&b) {
                                Ordering::Equal => {
                                    continue;
                                }
                                Ordering::Less => match direction {
                                    Direction::Ascending => Ordering::Less,
                                    Direction::Descending => Ordering::Greater,
                                },
                                Ordering::Greater => match direction {
                                    Direction::Ascending => Ordering::Greater,
                                    Direction::Descending => Ordering::Less,
                                },
                            },
                            (Value::Float(a), Value::Integer(b)) => {
                                let b = b as f64;
                                if a.eq(&b) {
                                    continue;
                                } else if a.ge(&b) {
                                    match direction {
                                        Direction::Ascending => Ordering::Greater,
                                        Direction::Descending => Ordering::Less,
                                    }
                                } else {
                                    match direction {
                                        Direction::Ascending => Ordering::Less,
                                        Direction::Descending => Ordering::Greater,
                                    }
                                }
                            }
                            (Value::Integer(a), Value::Float(b)) => {
                                let a = a as f64;
                                if a.eq(&b) {
                                    continue;
                                } else if a.ge(&b) {
                                    match direction {
                                        Direction::Ascending => Ordering::Greater,
                                        Direction::Descending => Ordering::Less,
                                    }
                                } else {
                                    match direction {
                                        Direction::Ascending => Ordering::Less,
                                        Direction::Descending => Ordering::Greater,
                                    }
                                }
                            }
                            (Value::Float(a), Value::Float(b)) => {
                                if a.eq(&b) {
                                    continue;
                                } else if a.ge(&b) {
                                    match direction {
                                        Direction::Ascending => Ordering::Greater,
                                        Direction::Descending => Ordering::Less,
                                    }
                                } else {
                                    match direction {
                                        Direction::Ascending => Ordering::Less,
                                        Direction::Descending => Ordering::Greater,
                                    }
                                }
                            }
                            (Value::Boolean(a), Value::Boolean(b)) => match a.cmp(&b) {
                                Ordering::Equal => {
                                    continue;
                                }
                                Ordering::Less => match direction {
                                    Direction::Ascending => Ordering::Less,
                                    Direction::Descending => Ordering::Greater,
                                },
                                Ordering::Greater => match direction {
                                    Direction::Ascending => Ordering::Greater,
                                    Direction::Descending => Ordering::Less,
                                },
                            },
                            (Value::String(a), Value::String(b)) => match a.cmp(&b) {
                                Ordering::Equal => {
                                    continue;
                                }
                                Ordering::Less => match direction {
                                    Direction::Ascending => Ordering::Less,
                                    Direction::Descending => Ordering::Greater,
                                },
                                Ordering::Greater => match direction {
                                    Direction::Ascending => Ordering::Greater,
                                    Direction::Descending => Ordering::Less,
                                },
                            },
                            (_, _) => Ordering::Greater,
                        };
                        break;
                    }
                    sort
                });
                Ok(ResultSet::Query {
                    columns,
                    rows: Box::new(row_array.into_iter().map(|i| Ok(i))),
                })
            }
            r => Err(Error::Internal(format!("Unexpected result {}", r))),
        }
    }
}

impl<C: Catalog> Filter<C> {
    pub fn new(source: Box<dyn Executor<C>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

impl<C: Catalog> Executor<C> for Filter<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> Result<ResultSet> {
        // find source data
        if let ResultSet::Query { columns, rows } = self.source.execute(catalog)? {
            let predicate = self.predicate;
            Ok(ResultSet::Query {
                columns,
                rows: Box::new(rows.filter_map(move |r| {
                    // filter row by predicate, so, it should be return false/true
                    r.and_then(|row| match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => Ok(Some(row)),
                        Value::Boolean(false) => Ok(None),
                        Value::Null => Ok(None),
                        value => Err(Error::Value(format!(
                            "Filter returned {}, expected boolean",
                            value
                        ))),
                    })
                    .transpose()
                })),
            })
        } else {
            Err(Error::Value("Unexpected result".into()))
        }
    }
}

impl<C: Catalog> Projection<C> {
    pub fn new(
        source: Box<dyn Executor<C>>,
        expressions: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self {
            source,
            expressions,
        })
    }
}

impl<C: Catalog> Executor<C> for Projection<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        if let ResultSet::Query { columns, rows } = self.source.execute(catalog)? {
            // e.g. SELECT id AS num FROM table;
            // expression = ["id"]
            // label = [Some("num")]
            let (expressions, labels): (Vec<Expression>, Vec<Option<String>>) =
                self.expressions.into_iter().unzip();
            let columns = expressions
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    if let Some(Some(label)) = labels.get(i) {
                        Column {
                            name: Some(label.clone()),
                        }
                    } else if let Expression::Field(i, _) = e {
                        columns.get(*i).cloned().unwrap_or(Column { name: None })
                    } else {
                        Column { name: None }
                    }
                })
                .collect();
            // execute expression recursively
            let rows = Box::new(rows.map(move |r| {
                r.and_then(|row| {
                    expressions
                        .iter()
                        .map(|e| e.evaluate(Some(&row)))
                        .collect::<Result<_>>()
                })
            }));
            Ok(ResultSet::Query { columns, rows })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
