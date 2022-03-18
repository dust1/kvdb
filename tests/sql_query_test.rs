use kvdb::common::result::DataColumn;
use kvdb::common::result::DataRow;
use kvdb::common::result::ResultSet;
use kvdb::error::Result;
use kvdb::sql::engine::KVEngine;
use kvdb::sql::engine::SQLEngine;
use kvdb::sql::schema::data_value::DataValue;
use kvdb::storage::b_tree::Memory;
use kvdb::storage::mvcc::MVCC;
use kvdb::storage::Store;

struct QueryTest {
    sql: &'static str,
    columns: Vec<DataColumn>,
    rows: Vec<DataRow>,
}

#[test]
fn query_test() -> Result<()> {
    let mut engine = get_engine();
    init_db(&mut engine)?;

    let tests = [
        QueryTest {
            sql: "SELECT * from countries",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("fr".into()), DataValue::String("France".into())],
                vec![DataValue::String("ru".into()), DataValue::String("Russia".into())],
                vec![DataValue::String("us".into()), DataValue::String("United States of America".into())],
            ]
        },
        QueryTest {
            sql: "SELECT id from countries",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
            ],
            rows: vec![
                vec![DataValue::String("fr".into())],
                vec![DataValue::String("ru".into())],
                vec![DataValue::String("us".into())],
            ]
        },
        QueryTest {
            sql: "SELECT * from countries where id = 'fr'",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("fr".into()), DataValue::String("France".into())],
            ]
        },
        QueryTest {
            sql: "SELECT * from countries where name = 'Russia'",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("ru".into()), DataValue::String("Russia".into())],
            ]
        },
        QueryTest {
            sql: "SELECT id num, name title from countries",
            columns: vec![
                DataColumn {
                    name: Some("num".into()),
                },
                DataColumn {
                    name: Some("title".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("fr".into()), DataValue::String("France".into())],
                vec![DataValue::String("ru".into()), DataValue::String("Russia".into())],
                vec![DataValue::String("us".into()), DataValue::String("United States of America".into())],
            ]
        },
    ];

    for test in tests {
        let session = engine.session()?;
        let result = session.execute(&test.sql)?;
        match result {
            ResultSet::Query { columns, mut rows } => {
                assert_eq!(columns, test.columns);
                let mut index = 0;
                while let Some(row) = rows.next().transpose()? {
                    if let Some(test_row) = test.rows.get(index) {
                        assert_eq!(&row, test_row);
                    } else {
                        assert!(false);
                    }
                    index += 1;
                }
            }
            r => assert!(false),
        }
    }
    Ok(())
}

struct UpdateTest {
    update_sql: &'static str,
    update_count: u64,
    check_sql: &'static str,
    columns: Vec<DataColumn>,
    rows: Vec<DataRow>,
}


#[test]
fn update_test() -> Result<()> {
    let mut engine = get_engine();
    init_db(&mut engine)?;

    let tests = [
        UpdateTest {
            update_sql: "UPDATE countries set name = 'UpdateFrance' where id = 'fr'",
            update_count: 1,
            check_sql: "SELECT * from countries where id = 'fr'",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("fr".into()), DataValue::String("UpdateFrance".into())],
            ]
        },
        UpdateTest {
            update_sql: "UPDATE genres set id = 4 where id = 1",
            update_count: 1,
            check_sql: "SELECT * from genres where id = 4",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::Integer(4), DataValue::String("Science Fiction".into())],
            ]
        }
    ];
    
    for test in tests {
        let session = engine.session()?;
        let result = session.execute(&test.update_sql)?;
        match result {
            ResultSet::Update { count } => {
                assert_eq!(count, test.update_count);
                let result = session.execute(&test.check_sql)?;
                match result {
                    ResultSet::Query { columns, mut rows } => {
                        assert_eq!(columns, test.columns);
                        let mut index = 0;
                        while let Some(row) = rows.next().transpose()? {
                            if let Some(test_row) = test.rows.get(index) {
                                assert_eq!(&row, test_row);
                            } else {
                                assert!(false);
                            }
                            index += 1;
                        }
                    },
                    r => assert!(false, "check result error: {}", r)
                }
            },
            r => assert!(false, "update result error: {}", r),
        }
    }
    Ok(())
}

#[test]
fn delete_tests() -> Result<()> {
    let tests = [
        UpdateTest {
            update_sql: "DELETE from countries where id = 'fr",
            update_count: 1,
            check_sql: "SELECT * from countries",
            columns: vec![
                DataColumn {
                    name: Some("id".into()),
                },
                DataColumn {
                    name: Some("name".into())
                }
            ],
            rows: vec![
                vec![DataValue::String("fr".into()), DataValue::String("UpdateFrance".into())],
            ]       
        }
    ];
    Ok(())
}

fn get_engine() -> KVEngine {
    let store: Box<dyn Store> = Box::new(Memory::new());
    let mvcc = MVCC::new(store);
    KVEngine { mvcc }
}

fn init_db(engine: &mut KVEngine) -> Result<()> {
    let sqls = [
        "CREATE TABLE countries (
                    id STRING PRIMARY KEY,
                    name STRING NOT NULL
                )",
        "INSERT INTO countries VALUES
                    ('fr', 'France'),
                    ('ru', 'Russia'),
                    ('us', 'United States of America')",
        "CREATE TABLE genres (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL
                )",
        "INSERT INTO genres VALUES
                    (1, 'Science Fiction'),
                    (2, 'Action'),
                    (3, 'Comedy')",
    ];

    let session = engine.session()?;
    for sql in sqls {
        session.execute(sql)?;
    }
    Ok(())
}
