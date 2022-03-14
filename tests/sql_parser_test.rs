use kvdb::error::Result;
use kvdb::sql::sql_parser::KVParser;
use kvdb::sql::sql_statement::KVStatement;
use kvdb::sql::statements::KVQueryStatement;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SelectItem;

use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;
use sqlparser::ast::Value;

struct StatementTest {
    sql: &'static str,
    stmt: KVStatement,
}

#[test]
fn query_test() -> Result<()> {
    let tests = [
        StatementTest {
            sql: "SELECT * from user",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                projection: vec![SelectItem::Wildcard],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        },
        StatementTest {
            sql: "SELECT id from user",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                }))],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        },
        StatementTest {
            sql: "SELECT id as num from user",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                projection: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                    }),
                    alias: Ident {
                        value: "num".to_string(),
                        quote_style: None,
                    },
                }],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        },
        StatementTest {
            sql: "SELECT id from user where id = 1",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                }))],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                }),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        },
    ];

    for test in tests {
        let stmts = KVParser::parser_sql(test.sql)?;
        assert_eq!(stmts[0], test.stmt)
    }

    Ok(())
}
