use std::sync::Arc;

use sqlparser::ast::Expr;
use sqlparser::ast::Offset;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;

use super::AnalyzerResult;
use super::AnalyzerStatement;
use crate::error::Error;
use crate::error::Result;
use crate::sql::plan::planner::Scope;
use crate::sql::plan::planners::Expression;
use crate::sql::plan::planners::FilterPlan;
use crate::sql::plan::planners::GroupByPlan;
use crate::sql::plan::planners::ProjectionPlan;
use crate::sql::plan::planners::ScanPlan;
use crate::sql::plan::PlanNode;
use crate::sql::session::Catalog;

pub struct KVQueryStatement {
    pub from: Vec<TableWithJoins>,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Offset>,
}

impl KVQueryStatement {
    pub fn try_from(stmt: Query) -> Result<Self> {
        if let SetExpr::Select(select) = stmt.body {
            let stmt = KVQueryStatement {
                from: select.from,
                projection: select.projection,
                selection: select.selection,
                group_by: select.group_by,
                having: select.having,
                order_by: stmt.order_by,
                limit: stmt.limit,
                offset: stmt.offset,
            };
            return Ok(stmt);
        }
        Err(Error::Internal(format!("unsupport Query type {}", stmt)))
    }
}

impl AnalyzerStatement for KVQueryStatement {
    fn analyze(&self, catalog: Arc<dyn Catalog>) -> Result<AnalyzerResult> {
        if self.from.is_empty() {
            return Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::Nothing)));
        }
        let mut scope = Scope::new();
        let ctx = catalog.clone();

        let mut node = self.plan_node_from(&mut scope, ctx)?;
        node = self.plan_node_selection(node, &mut scope)?;
        node = self.plan_node_projection(node, &mut scope)?;
        node = self.plan_node_group_by(node, &mut scope)?;

        Ok(AnalyzerResult::SimpleQuery(Box::new(node)))
    }
}

impl KVQueryStatement {
    // FROM
    fn plan_node_from(&self, scope: &mut Scope, ctx: Arc<dyn Catalog>) -> Result<PlanNode> {
        if self.from.len() != 1 {
            return Err(Error::Internal("unsupport two or than SELECT".into()));
        }
        let from = &self.from[0];
        // FIXME: should support JOIN
        match &from.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias.as_ref().map(|a| a.to_string());
                scope.add_table(
                    alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone()),
                    ctx.must_read_table(&table_name)?,
                )?;
                Ok(PlanNode::Scan(ScanPlan {
                    table_name,
                    alias: alias_name,
                    filter: None,
                }))
            }
            o => Err(Error::Internal(format!("unsupport this select {}", o))),
        }
    }

    // WHERE
    fn plan_node_selection(&self, node: PlanNode, scope: &mut Scope) -> Result<PlanNode> {
        match &self.selection {
            None => Ok(node),
            Some(selection) => Ok(PlanNode::Filter(FilterPlan {
                source: Box::new(node),
                predicate: Expression::from_expr(selection, scope)?,
            })),
        }
    }

    // Column
    fn plan_node_projection(&self, node: PlanNode, scope: &mut Scope) -> Result<PlanNode> {
        if self.projection.is_empty() {
            return Ok(node);
        }
        let projections = self
            .projection
            .iter()
            .map(|select| Expression::from_select_item(select, scope))
            .collect::<Result<Vec<_>>>()?;
        let p = &projections[..];
        scope.project(p)?;
        Ok(PlanNode::Projection(ProjectionPlan {
            source: Box::new(node),
            expressions: projections,
        }))
    }

    // GROPUP BY
    fn plan_node_group_by(&self, node: PlanNode, scope: &mut Scope) -> Result<PlanNode> {
        if self.group_by.is_empty() {
            return Ok(node);
        }

        Ok(PlanNode::GroupBy(GroupByPlan {
            source: Box::new(node),
            expressions: self
                .group_by
                .iter()
                .map(|group_by| Expression::from_expr(group_by, scope))
                .collect::<Result<_>>()?,
        }))
    }
}
