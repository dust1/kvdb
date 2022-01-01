use crate::error::{Error, Result};
use crate::sql::parser::ast::KVStatement;
use crate::sql::parser::translate::translate_object_name_to_string;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::{Catalog, Table};
use crate::sql::types;
use crate::sql::types::expression::Expression;
use sqlparser::ast::{
    Expr, Ident, Join, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor,
    TableWithJoins,
};
use std::collections::{HashMap, HashSet};

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

    pub fn build_statement(&mut self, statement: KVStatement) -> Result<Node> {
        match statement {
            KVStatement::CreateTable { name, columns } => {
                let table = Table::new(name, columns)?;
                Ok(Node::CreateTable { schema: table })
            }
            KVStatement::DropTable { names } => {
                let name = &names[0];
                let table_name = translate_object_name_to_string(name)?;
                Ok(Node::DropTable { table: table_name })
            }
            KVStatement::Query(query) => self.query_to_plan(query.as_ref()),
            _ => {
                todo!()
            }
        }
    }

    fn query_to_plan(&mut self, query: &Query) -> Result<Node> {
        let mut scope = Scope::new();
        let set_expr = &query.body;

        let node = self.set_expr_to_plan(set_expr, &mut scope)?;
        // TODO ORDER BY
        // TODO LIMIT
        Ok(node)
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

        // WHERE
        if let Some(expr) = &select.selection {
            node = Node::Filter {
                source: Box::new(node),
                predicate: self.build_expression(expr, scope)?,
            }
        }

        // projection expressions
        if let Some(expressions) = self.prepare_select_projection(&select.projection, scope)? {
            scope.project(&expressions)?;
            node = Node::Projection { source: Box::new(node), expression: expressions }
        }

        // todo HAVING, ORDER, LIMIT, OFFSET
        Ok(node)
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
            Expr::Value(literal) => Constant(match literal {
                ast::Value::Number(n, _) => types::Value::parse_number(n),
                ast::Value::SingleQuotedString(ref s)
                | ast::Value::NationalStringLiteral(ref s)
                | ast::Value::HexStringLiteral(ref s)
                | ast::Value::DoubleQuotedString(ref s) => types::Value::parse_string(s),
                ast::Value::Boolean(b) => types::Value::Boolean(*b),
                ast::Value::Null => types::Value::Null,
                _ => todo!(),
            }),
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
                let alias_name = match alias {
                    Some(a) => Some(a.clone()),
                    None => None,
                };
                scope.add_table(
                    alias.as_ref().map(|a| a.name.value.clone()).unwrap_or(table_name.clone()),
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
            self.add_column(Some(label.clone()), Some(column.name.value.clone()));
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
    fn project(&mut self, _projection: &[(Expression, Option<String>)]) -> Result<()> {
        todo!()
    }
}
