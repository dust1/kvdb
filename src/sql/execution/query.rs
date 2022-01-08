use crate::sql::execution::{Executor, ResultSet};
use crate::sql::schema::Catalog;
use crate::sql::types::expression::Expression;

/// a filter executor
pub struct Filter<C: Catalog> {
    source: Box<dyn Executor<C>>,
    predicate: Expression
}

/// a projection executor
pub struct Projection<C: Catalog> {
    source: Box<dyn Executor<C>>,
    expressions: Vec<(Expression, Option<String>)>
}

impl<C: Catalog> Filter<C> {
    pub fn new(source: Box<dyn Executor<C>>, predicate: Expression) -> Box<Self> {
        Box::new(Self {
            source,
            predicate
        })
    }
}

impl<C: Catalog> Executor<C> for Filter<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        todo!()
    }
}

impl<C: Catalog> Projection<C> {
    pub fn new(source: Box<dyn Executor<C>>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self> {
        Box::new(Self {
            source,
            expressions
        })
    }
}

impl<C: Catalog> Executor<C> for Projection<C> {
    fn execute(self: Box<Self>, catalog: &mut C) -> crate::error::Result<ResultSet> {
        todo!()
    }
}
