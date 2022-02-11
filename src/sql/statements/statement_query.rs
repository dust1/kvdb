use sqlparser::ast::Expr;
use sqlparser::ast::Offset;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::SelectItem;
use sqlparser::ast::TableWithJoins;

use super::AnalyzerStatement;
use crate::error::Result;

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
    pub fn try_from(_stmt: Query) -> Result<Self> {
        todo!()
    }
}

impl AnalyzerStatement for KVQueryStatement {
    fn analyze(&self) -> Result<super::analyzer_statement::AnalyzerResult> {
        todo!()
    }
}
