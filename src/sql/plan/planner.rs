use crate::error::{Error, Result};
use crate::sql::parser::ast::KVStatement;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::{Catalog, Table};
use crate::sql::types::expression::Expression;
use crate::sql::types::Value;
use sqlparser::ast::{
    Assignment, Expr, Ident, Join, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor,
    TableWithJoins,
};
use std::collections::{HashMap, HashSet};

use super::Direction;

/// query plan builder
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

/// Manages names available to expressions and executors, and maps them onto columns/fields.
#[derive(Clone, Debug)]
pub struct Scope {
    // If true, the scope is constant and cannot contain any variables.
    constant: bool,
    // Currently visible tables, by query name (i.e. alias or actual name).
    // the key is the table name
    // the value is the table object
    tables: HashMap<String, Table>,
    // Column labels, if any (qualified by table name when available)
    columns: Vec<(Option<String>, Option<String>)>,
    // Qualified names to column indexes.
    // index of columns constructed by table name and column name
    // the key is the union by table name and column name
    // the value is the index by columns
    qualified: HashMap<(String, String), usize>,
    // Unqualified names to column indexes, if unique.
    // if column name is unique, we can get column[i] just by column name
    // the key is column name
    // this value is the index by columns
    unqualified: HashMap<String, usize>,
    // Unqialified ambiguous names.
    // the value is the column with the same name
    ambiguous: HashSet<String>,
}

impl<'a, C: Catalog> Planner<'a, C> {
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    pub fn build(&mut self, statement: KVStatement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    fn build_statement(&mut self, statement: KVStatement) -> Result<Node> {
        match statement {
            KVStatement::CreateTable { name, columns } => {
                let table = Table::new(name, columns)?;
                Ok(Node::CreateTable { schema: table })
            }
            KVStatement::DropTable { names } => {
                let name = &names[0];
                Ok(Node::DropTable { table: name.to_string() })
            }
            KVStatement::Query(query) => self.query_to_plan(query.as_ref()),
            KVStatement::Insert { table_name, columns, source } => Ok(Node::Insert {
                table: table_name.to_string(),
                columns: columns.iter().map(|ident| ident.to_string()).collect::<Vec<String>>(),
                expressions: self.query_to_expressions(source.as_ref())?,
            }),
            KVStatement::Update { table_name, assignments, selection } => {
                let table = self.catalog.must_read_table(&table_name.to_string())?;
                let mut scope = Scope::from_table(table)?;
                let set = self.assignment_to_set(assignments, &mut scope)?;
                let filter =
                    selection.map(|expr| self.build_expression(&expr, &mut scope)).transpose()?;
                Ok(Node::Update {
                    table: table_name.to_string(),
                    source: Box::new(Node::Scan {
                        table: table_name.to_string(),
                        alias: None,
                        filter: filter,
                    }),
                    expressions: set,
                })
            }
            KVStatement::Delete { table_name, selection } => {
                let mut scope =
                    Scope::from_table(self.catalog.must_read_table(&table_name.to_string())?)?;
                let filter =
                    selection.map(|expr| self.build_expression(&expr, &mut scope)).transpose()?;
                Ok(Node::Delete {
                    table: table_name.to_string(),
                    source: Box::new(Node::Scan {
                        table: table_name.to_string(),
                        alias: None,
                        filter: filter,
                    }),
                })
            }
            _ => {
                todo!()
            }
        }
    }

    /// assignment to set
    fn assignment_to_set(
        &self,
        assignments: Vec<Assignment>,
        scope: &mut Scope,
    ) -> Result<Vec<(usize, Option<String>, Expression)>> {
        Ok(assignments
            .into_iter()
            .map(|issignment| {
                let field = issignment.id.to_string();
                Ok((
                    scope.resolve(None, &field)?,
                    Some(field),
                    self.build_expression(&issignment.value, scope)?,
                ))
            })
            .collect::<Result<_>>()?)
    }

    /// by select
    fn query_to_plan(&mut self, query: &Query) -> Result<Node> {
        println!("{:?}", query);
        let mut scope = Scope::new();
        let set_expr = &query.body;
        let mut node = self.set_expr_to_plan(set_expr, &mut scope)?;

        node = self.build_order_node(node, &query.order_by, &mut scope)?;

        if let Some(limit) = &query.limit {
            node = self.build_limit_node(node, limit)?;
        }

        Ok(node)
    }

    fn build_limit_node(&mut self, node: Node, limit: &Expr) -> Result<Node> {
        match limit {
            Expr::Value(sqlparser::ast::Value::Number(n, _)) => match n.parse::<usize>() {
                Ok(n) => Ok(Node::Limit { source: Box::new(node), offset: 0, limit: Some(n) }),
                Err(r) => return Err(Error::Internal(format!("Unknown limit value {}", r))),
            },
            e => return Err(Error::Internal(format!("Unknown limit {}", e))),
        }
    }

    fn build_order_node(
        &mut self,
        node: Node,
        order_by: &Vec<OrderByExpr>,
        scope: &mut Scope,
    ) -> Result<Node> {
        match order_by.is_empty() {
            true => Ok(node),
            false => Ok(Node::OrderBy {
                source: Box::new(node),
                orders: order_by
                    .iter()
                    .map(|o| {
                        let expr = self.build_expression(&o.expr, scope)?;
                        let direction = match &o.asc {
                            Some(true) => Direction::Ascending,
                            Some(false) => Direction::Descending,
                            None => Direction::Ascending,
                        };
                        Ok((expr, direction))
                    })
                    .collect::<Result<_>>()?,
            }),
        }
    }

    /// by Insert
    fn query_to_expressions(&mut self, query: &Query) -> Result<Vec<Vec<Expression>>> {
        let mut scope = Scope::new();
        if let SetExpr::Values(values) = &query.body {
            return values
                .0
                .iter()
                .map(|items| {
                    items
                        .iter()
                        .map(|expr| self.build_expression(expr, &mut scope))
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>();
        }

        Err(Error::Value(format!("Un support insert by this query: {}", query)))
    }

    fn set_expr_to_plan(&mut self, ext_expr: &SetExpr, scope: &mut Scope) -> Result<Node> {
        match ext_expr {
            SetExpr::Select(select) => self.select_to_plan(select.as_ref(), scope),
            _ => Err(Error::Parse("can not support this select.".to_string())),
        }
    }

    /// create plan from select, maybe should handled another JOIN in here.
    /// e.g. SELECT * FROM A, B WHERE A.id = B.id
    fn select_to_plan(&mut self, select: &Select, scope: &mut Scope) -> Result<Node> {
        // FROM
        let mut node = self.plan_from_table(&select.from, scope)?;

        // WHERE - Filter
        if let Some(expr) = &select.selection {
            node = Node::Filter {
                source: Box::new(node),
                predicate: self.build_expression(expr, scope)?,
            }
        }

        // projection expressions
        if let Some(expressions) = self.prepare_select_projection(&select.projection, scope)? {
            scope.project(&expressions)?;
            node = Node::Projection { source: Box::new(node), expressions }
        }

        // Group by
        node = self.build_group_by_plan(node, &select.group_by, scope)?;

        Ok(node)
    }

    /// group by
    fn build_group_by_plan(
        &self,
        node: Node,
        group_by: &Vec<Expr>,
        scope: &mut Scope,
    ) -> Result<Node> {
        if group_by.is_empty() {
            return Ok(node);
        }

        Ok(Node::GroupBy {
            source: Box::new(node),
            expression: group_by
                .iter()
                .map(|iter| self.build_expression(iter, scope))
                .collect::<Result<_>>()?,
        })
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    fn prepare_select_projection(
        &self,
        projection: &Vec<SelectItem>,
        scope: &mut Scope,
    ) -> Result<Option<Vec<(Expression, Option<String>)>>> {
        match projection.len() {
            0 => Ok(None),
            1 => {
                let select = &projection[0];
                match select {
                    SelectItem::Wildcard => Ok(None),
                    _ => {
                        let result = self.sql_select_to_expression(select, scope)?;
                        Ok(Some(vec![result]))
                    }
                }
            }
            _ => Ok(Some(
                projection
                    .iter()
                    .map(|expr| self.sql_select_to_expression(expr, scope))
                    .collect::<Result<Vec<_>>>()?,
            )),
        }
    }

    /// generate a relational expression from a select SQL expression
    fn sql_select_to_expression(
        &self,
        sql: &SelectItem,
        scope: &mut Scope,
    ) -> Result<(Expression, Option<String>)> {
        match sql {
            SelectItem::UnnamedExpr(expr) => Ok((self.build_expression(expr, scope)?, None)),
            SelectItem::ExprWithAlias { expr, alias } => {
                Ok((self.build_expression(expr, scope)?, Some(alias.to_string())))
            }
            SelectItem::Wildcard => Ok((Expression::Wildcard, None)),
            SelectItem::QualifiedWildcard(_) => {
                Err(Error::Value("can not support alias.* or even schema.table.*".to_string()))
            }
        }
    }

    /// build Expression by sqlparser::ast::Expr
    fn build_expression(&self, sql_expr: &Expr, scope: &mut Scope) -> Result<Expression> {
        use sqlparser::*;
        use Expression::*;
        Ok(match sql_expr {
            Expr::Value(literal) => Constant(Value::from_expr_value(literal)),
            Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::Or => Or(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::And => And(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::Eq => Equal(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::NotEq => Not(Equal(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                )
                .into()),
                ast::BinaryOperator::Gt => GreaterThan(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::GtEq => Or(
                    GreaterThan(
                        self.build_expression(left, scope)?.into(),
                        self.build_expression(right, scope)?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(left, scope)?.into(),
                        self.build_expression(right, scope)?.into(),
                    )
                    .into(),
                ),
                ast::BinaryOperator::Lt => LessThan(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::LtEq => Or(
                    LessThan(
                        self.build_expression(left, scope)?.into(),
                        self.build_expression(right, scope)?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(left, scope)?.into(),
                        self.build_expression(right, scope)?.into(),
                    )
                    .into(),
                ),
                ast::BinaryOperator::Like => Like(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::Plus => Add(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::Minus => Subtract(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::Multiply => Multiply(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                ast::BinaryOperator::Divide => Divide(
                    self.build_expression(left, scope)?.into(),
                    self.build_expression(right, scope)?.into(),
                ),
                _ => todo!(),
            },
            Expr::UnaryOp { expr, op } => match op {
                ast::UnaryOperator::Not => Not(self.build_expression(expr, scope)?.into()),
                _ => todo!(),
            },
            Expr::IsNull(expr) => IsNull(self.build_expression(expr, scope)?.into()),
            Expr::Identifier(ident) => {
                Field(scope.resolve(None, &ident.to_string())?, Some((None, ident.to_string())))
            }
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
                    Field(scope.resolve(None, &name.to_string())?, Some((None, name.to_string())))
                } else {
                    return Err(Error::Value(format!("Unsupported SQL statement. {}", sql_expr)));
                }
            }
            Expr::Wildcard => Wildcard,
            _ => todo!(),
        })
    }

    /// create logic plan node from TableWithJoins
    fn plan_from_table(&mut self, from: &[TableWithJoins], scope: &mut Scope) -> Result<Node> {
        if from.is_empty() {
            return Ok(Node::Nothing);
        }
        self.plan_table_with_joins(&from[0], scope)
    }

    /// create logic plan node from table with join
    /// e.g. SELECT * FROM A JOIN B ON A.id = B.id
    fn plan_table_with_joins(&mut self, t: &TableWithJoins, scope: &mut Scope) -> Result<Node> {
        let left = self.create_relation(&t.relation, scope)?;
        let joins: &Vec<Join> = &t.joins;
        if joins.is_empty() {
            return Ok(left);
        }

        todo!("TableWithJoins.joins")
    }

    /// create a relation plan node
    fn create_relation(&mut self, relation: &TableFactor, scope: &mut Scope) -> Result<Node> {
        match relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias.as_ref().map(|a| a.to_string());
                scope.add_table(
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone()),
                    self.catalog.must_read_table(&table_name)?,
                )?;
                Ok(Node::Scan { table: table_name, alias: alias_name, filter: None })
            }
            _ => Err(Error::Value("Can't support this select".to_string())),
        }
    }
}

impl Scope {
    pub fn new() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    pub fn from_table(table: Table) -> Result<Self> {
        let mut scope = Scope::new();
        scope.add_table(table.name.clone(), table)?;
        Ok(scope)
    }

    pub fn constant() -> Self {
        let mut scope = Self::new();
        scope.constant = true;
        scope
    }

    /// Add a table to the scope
    pub fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("can not modify constant scope".into()));
        }
        if self.tables.contains_key(&label) {
            return Err(Error::Value(format!("Duplicate table name {}", label)));
        }

        for column in &table.columns {
            self.add_column(Some(label.clone()), Some(column.name.clone()));
        }
        self.tables.insert(label, table);
        Ok(())
    }

    /// Add a column to the scope
    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                // save columns index by table name and column name
                self.qualified.insert((t, l.clone()), self.columns.len());
            }

            // if the column name only appears 0 or 1 times
            if !self.ambiguous.contains(&l) {
                if !self.unqualified.contains_key(&l) {
                    // we can find columns index just by column name
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    // the column name with the same name appears
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    /// resolves a name, optionally qualified by a tuple name
    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found field {}",
                if let Some(table) = table { format!("{}.{}", table, name) } else { name.into() }
            )));
        }

        if let Some(table) = table {
            if !self.tables.contains_key(table) {
                return Err(Error::Value(format!("Unknown table {}", table)));
            }
            self.qualified
                .get(&(table.into(), name.into()))
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}.{}", table, name)))
        } else if self.ambiguous.contains(name) {
            Err(Error::Value(format!("Ambiguous field {}, no table specified", name)))
        } else {
            self.unqualified
                .get(name)
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}", name)))
        }
    }

    /// Projects the scope. This takes a set of expressions and labels in the current scope,
    /// and returns a new scope for the projection.
    /// rebuild scope by projection
    fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        let mut new = Self::new();
        new.tables = self.tables.clone();
        for (expr, label) in projection {
            match (expr, label) {
                (_, Some(label)) => new.add_column(None, Some(label.clone())),
                (Expression::Field(_, Some((Some(table), name))), _) => {
                    new.add_column(Some(table.clone()), Some(name.clone()))
                }
                (Expression::Field(_, Some((None, name))), _) => {
                    if let Some(i) = self.unqualified.get(name) {
                        let (table, name) = self.columns[*i].clone();
                        new.add_column(table, name);
                    }
                }
                (Expression::Field(i, None), _) => {
                    let (table, label) = self.columns.get(*i).cloned().unwrap_or((None, None));
                    new.add_column(table, label);
                }
                _ => new.add_column(None, None),
            }
        }
        *self = new;
        Ok(())
    }
}
