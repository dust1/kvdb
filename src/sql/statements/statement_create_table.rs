use sqlparser::ast::{ColumnDef, ObjectName, Query, SqlOption};

use crate::{
    error::Result,
    sql::{
        plan::PlanNode,
        schema::{Column, Table},
    },
};

use super::{AnalyzerResult, AnalyzerStatement};

pub struct KVCreateTableStatement {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub config: Vec<SqlOption>,
    pub query: Option<Box<Query>>,
    pub like: Option<ObjectName>,
}

impl AnalyzerStatement for KVCreateTableStatement {
    fn analyze(&self) -> Result<AnalyzerResult> {
        // let columns = self.columns.iter().map(Column).collect::<Vec<_>>();
        // let table = Table {
        //     name: self.name.to_string(),
        //     columns
        // };
        // Ok(AnalyzerResult::SimpleQuery(Box::new(PlanNode::CreateTable {
        //     schema: Table {
        //         name
        //     }
        // })))
        todo!()
    }
}
