use crate::error::{Error, Result};
use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::Catalog;
use crate::sql::types::expression::Expression;
use crate::sql::types::{Column, Value};

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
        Box::new(Self { source, expressions })
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
                        Column { name: Some(label.clone()) }
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
                    expressions.iter().map(|e| e.evaluate(Some(&row))).collect::<Result<_>>()
                })
            }));
            Ok(ResultSet::Query { columns, rows })
        } else {
            Err(Error::Internal("Unexpected result".into()))
        }
    }
}
