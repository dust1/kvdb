use std::collections::HashMap;
use std::collections::HashSet;

use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::plan_expression::Expression;
use crate::sql::schema::table::Table;

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

impl Scope {
    /// Creates a new, empty scope.
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

    /// Creates a constant scope.
    pub fn constant() -> Self {
        let mut scope = Self::new();
        scope.constant = true;
        scope
    }

    /// Creates a scope from a table.
    pub fn from_table(table: Table) -> Result<Self> {
        let mut scope = Self::new();
        scope.add_table(table.name.clone(), table)?;
        Ok(scope)
    }

    /// Adds a column to the scope.
    #[allow(clippy::map_entry)]
    pub fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                // 通过表名和列名构建的元组作为key,value则是这次要添加的列的columns下标
                // the key is union by table name and column name
                // the value is index by this insert column
                self.qualified.insert((t, l.clone()), self.columns.len());
            }

            // if this column name was
            if !self.ambiguous.contains(&l) {
                // if this column name is unique, we can find it just by column name
                if !self.unqualified.contains_key(&l) {
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    // if a column with the same name appears
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    /// Adds a table to the scope.
    pub fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
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

    /// Fetches a column from the scope by index.
    pub fn get_column(&self, index: usize) -> Result<(Option<String>, Option<String>)> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found column {}",
                index
            )));
        }
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| Error::Value(format!("Column index {} not found", index)))
    }

    /// Fetches a column label by index, if any.
    pub fn get_label(&self, index: usize) -> Result<Option<(Option<String>, String)>> {
        Ok(match self.get_column(index)? {
            (table, Some(name)) => Some((table, name)),
            _ => None,
        })
    }

    /// Merges two scopes, by appending the given scope to self.
    pub fn merge(&mut self, scope: Scope) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant scope".into()));
        }
        for (label, table) in scope.tables {
            if self.tables.contains_key(&label) {
                return Err(Error::Value(format!("Duplicate table name {}", label)));
            }
            self.tables.insert(label, table);
        }
        for (table, label) in scope.columns {
            self.add_column(table, label);
        }
        Ok(())
    }

    /// Resolves a name, optionally qualified by a table name.
    pub fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found field {}",
                if let Some(table) = table {
                    format!("{}.{}", table, name)
                } else {
                    name.into()
                }
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
            Err(Error::Value(format!("Ambiguous field {}", name)))
        } else {
            self.unqualified
                .get(name)
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field {}", name)))
        }
    }

    /// Number of columns in the current scope.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Projects the scope. This takes a set of expressions and labels in the current scope,
    /// and returns a new scope for the projection.
    pub fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
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
                    new.add_column(table, label)
                }
                _ => new.add_column(None, None),
            }
        }
        *self = new;
        Ok(())
    }
}
