use crate::sql::types::Value;

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

impl Expression {}
