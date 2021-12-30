use std::collections::{HashMap, HashSet};
use sqlparser::ast::{Expr, Ident, OrderByExpr, Query, Select, SetExpr, TableAlias, TableFactor, TableWithJoins};
use crate::error::{Error, Result};
use crate::sql::parser::ast::KVStatement;
use crate::sql::parser::translate::translate_object_name_to_string;
use crate::sql::plan::{Node, Plan};
use crate::sql::schema::{Catalog, Table};

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
    tables: HashMap<String, Table>,
    // Column labels, if any (qualified by table name when available)
    columns: Vec<(Option<String>, Option<String>)>,
    // Qualified names to column indexes.
    qualified: HashMap<(String, String), usize>,
    // Unqualified names to column indexes, if unique.
    unqualified: HashMap<String, usize>,
    // Unqialified ambiguous names.
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
            KVStatement::CreateTable {
                name,
                columns
            } => {
                let table = Table::new(name, columns)?;
                Ok(Node::CreateTable {
                    schema: table
                })
            },
            KVStatement::DropTable {
                names
            } => {
                let name = &names[0];
                let table_name = translate_object_name_to_string(name)?;
                Ok(Node::DropTable {
                    table: table_name
                })
            },
            KVStatement::Query(query) => self.query_to_plan(query.as_ref()),
            _ => {
                todo!()
            }
        }
    }

    fn query_to_plan(&mut self, query: &Query) -> Result<Node> {
        let mut scope = Scope::new();
        let set_expr = &query.body;

        let mut node = self.set_expr_to_plan(set_expr, &mut scope)?;
        node = order_by(&query.order_by, node)?;
        node = limit(&query.limit, node)?;
        Ok(node)
    }

    fn set_expr_to_plan(&mut self, ext_expr: &SetExpr, scope: &mut Scope) -> Result<Node> {
        match ext_expr {
            SetExpr::Select(select) => {
                self.select_to_plan(select.as_ref(), scope)
            },
            _ => Err(Error::Parse("can not support this select.".to_string()))
        }
    }

    /// create plan from select, maybe should handled another JOIN in here.
    /// e.g. SELECT * FROM A, B WHERE A.id = B.id
    fn select_to_plan(&mut self, select: &Select, scope: &mut Scope) -> Result<Node> {
        let plans = self.plan_from_table(&select.from)?;
        todo!()
    }

    /// create logic plan node from TableWithJoins
    fn plan_from_table(&mut self, from: &[TableWithJoins]) -> Result<Vec<Node>> {
        if from.len() != 1 {
            return Err(Error::Value("can not select * without a table".to_string()));
        }

        from.iter().map(|t| self.plan_table_with_joins(t))
            .collect::<Result<Vec<_>>>()
    }

    /// create logic plan node from table with join
    /// e.g. SELECT * FROM A JOIN B ON A.id = B.id
    fn plan_table_with_joins(&mut self, t: &TableWithJoins) -> Result<Node> {
        todo!()
    }

    /// create a relation plan node
    fn create_relation(&mut self, relation: &TableFactor, scope: &mut Scope) -> Result<Node> {
        match relation {
            TableFactor::Table {name, alias, ..} => {
                let table_name = name.to_string();
                let alias_name = match alias {
                    Some(a) => Some(a.clone()),
                    None => None
                };

                scope.add_table(
                    alias.as_ref().map(|a| a.name.value.clone())
                        .unwrap_or(table_name.clone()),
                    self.catalog.must_read_table(&table_name)?
                )?;
                Ok(Node::Scan {
                    table: table_name, alias: alias_name, filter: None
                })
            },
            _ => Err(Error::Value("Can't support this select".to_string()))
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
            ambiguous: HashSet::new()
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
                self.qualified.insert((t, l.clone()), self.columns.len());
            }
            // why ?
            if !self.ambiguous.contains(&l) {
                if !self.unqualified.contains_key(&l) {
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

}


fn limit(limit: &Option<Expr>, node: Node) -> Result<Node> {
    todo!()
}

fn order_by(order_by: &Vec<OrderByExpr>, node: Node) -> Result<Node> {
    todo!()
}




