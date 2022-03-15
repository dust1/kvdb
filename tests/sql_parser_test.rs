use kvdb::error::Result;
use kvdb::sql::sql_parser::KVParser;
use kvdb::sql::sql_statement::KVStatement;
use kvdb::sql::statements::KVQueryStatement;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::Join;
use sqlparser::ast::JoinConstraint;
use sqlparser::ast::JoinOperator;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Offset;
use sqlparser::ast::OffsetRows;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::SelectItem;
use sqlparser::ast::TableAlias;
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
        StatementTest {
            sql: "SELECT id num from user where id > 1",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![]
                }],
                projection: vec![SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                    }),
                    alias: Ident {
                        value: "num".to_string(),
                        quote_style: None
                    }
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number("1".to_string(), false)))
                }),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        },
        StatementTest {
            sql: "SELECT * from user where id > 1 and name = '张三' order by id desc limit 1 offset 10",
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
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {value: "id".to_string(), quote_style: None})),
                        op: BinaryOperator::Gt,
                        right: Box::new(Expr::Value(Value::Number("1".to_string(), false)))
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {value: "name".to_string(), quote_style: None})),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("张三".to_string()))) 
                    })
                }),
                group_by: vec![],
                having: None,
                order_by: vec![
                    OrderByExpr {
                        expr: Expr::Identifier(Ident{value: "id".to_string(), quote_style: None}),
                        asc: Some(false),
                        nulls_first: None
                }],
                limit: Some(Expr::Value(Value::Number("1".to_string(), false))),
                offset: Some(Offset {
                    value: Expr::Value(Value::Number("10".to_string(), false)),
                    rows: OffsetRows::None
                })
            })
        },
        StatementTest {
            sql: "SELECT a.*, b.* from user a, clazz as b where a.id = b.user_id",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "user".to_string(),
                            quote_style: None
                        }]),
                        alias: Some(TableAlias {
                            name: Ident {
                                value: "a".to_string(),
                                quote_style: None
                            },
                            columns: vec![]
                        }),
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![]
                }, TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident{value: "clazz".to_string(), quote_style: None}]),
                        alias: Some(TableAlias {
                            name: Ident {
                                value: "b".to_string(),
                                quote_style: None
                            },
                            columns: vec![]
                        }),
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![]
                }],
                projection: vec![SelectItem::QualifiedWildcard(ObjectName(vec![
                    Ident {
                        value: "a".to_string(),
                        quote_style: None
                    }
                ])), SelectItem::QualifiedWildcard(ObjectName(vec![
                    Ident {
                        value: "b".to_string(),
                        quote_style: None
                    }
                ]))],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident {
                        value: "a".to_string(),
                        quote_style: None
                    }, Ident {
                        value: "id".to_string(),
                        quote_style: None
                    }])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::CompoundIdentifier(vec![Ident {
                        value: "b".to_string(),
                        quote_style: None
                    }, Ident {
                        value: "user_id".to_string(),
                        quote_style: None
                    }]))
                }),
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        },
        StatementTest {
            sql: "SELECT a.*, b.* from user a join clazz b on a.id = b.user_id",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![
                            Ident {
                                value: "user".to_string(),
                                quote_style: None
                            }
                        ]),
                        alias: Some(TableAlias {
                            name: Ident {
                                value: "a".to_string(),
                                quote_style: None
                            },
                            columns: vec![]
                        }),
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![Join {
                        relation: TableFactor::Table {
                            name: ObjectName(vec![
                                Ident {
                                    value: "clazz".to_string(),
                                    quote_style: None
                                }
                            ]),
                            alias: Some(TableAlias {
                                name: Ident {
                                    value: "b".to_string(),
                                    quote_style: None
                                },
                                columns: vec![]
                            }),
                            args: vec![],
                            with_hints: vec![]
                        },
                        join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident {
                                value: "a".to_string(),
                                quote_style: None
                            }, Ident {
                                value: "id".to_string(),
                                quote_style: None
                            }])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::CompoundIdentifier(vec![Ident {
                                value: "b".to_string(),
                                quote_style: None
                            }, Ident {
                                value: "user_id".to_string(),
                                quote_style: None
                            }]))
                        }))
                    }],
                }],
                projection: vec![SelectItem::QualifiedWildcard(ObjectName(vec![
                    Ident {
                        value: "a".to_string(),
                        quote_style: None
                    }
                ])), SelectItem::QualifiedWildcard(ObjectName(vec![
                    Ident {
                        value: "b".to_string(),
                        quote_style: None
                    }
                ]))],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        },
        StatementTest {
            sql: "SELECT count(1) from user",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![TableWithJoins {
                    relation: TableFactor::Table { name: ObjectName(vec![Ident {
                        value: "user".to_string(),
                        quote_style: None
                    }]), alias: None, args: vec![], with_hints: vec![] },
                    joins: vec![]
                }],
                projection: vec![SelectItem::UnnamedExpr(
                    Expr::Function(Function {
                        name: ObjectName(vec![Ident {
                            value: "count".to_string(),
                            quote_style: None
                        }]),
                        args: vec![
                            FunctionArg::Unnamed(Expr::Value(Value::Number("1".to_string(), false)))
                        ],
                        over: None,
                        distinct: false
                    })
                )],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        },
        StatementTest {
            sql: "SELECT 1 + 1",
            stmt: KVStatement::Query(KVQueryStatement {
                from: vec![],
                projection: vec![SelectItem::UnnamedExpr(
                    Expr::BinaryOp {
                        left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(Value::Number("1".to_string(), false)))
                    }
                )],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        }
    ];

    for test in tests {
        let stmts = KVParser::parser_sql(test.sql)?;
        assert_eq!(stmts[0], test.stmt)
    }

    Ok(())
}
