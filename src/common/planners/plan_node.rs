use crate::sql::schema::Table;



pub enum PlanNode {
    Empty,
    CreateTable {
        schema: Table
    },
    // todo
}