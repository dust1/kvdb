use kvdb::common::result::ResultSet;
use kvdb::error::Result;
use kvdb::sql::engine::KVEngine;
use kvdb::sql::engine::SQLEngine;
use kvdb::storage::b_tree::Memory;
use kvdb::storage::mvcc::MVCC;
use kvdb::storage::Store;

#[test]
fn test_query() -> Result<()> {
    let mut engine = get_engine();
    init_db(&mut engine)?;

    let sqls = ["SELECT * from countries"];

    for sql in sqls {
        let session = engine.session()?;
        let result = session.execute(sql)?;
        match result {
            ResultSet::Query { columns, mut rows } => {
                println!("{:?}", columns);
                while let Some(row) = rows.next().transpose()? {
                    println!("{:?}", row);
                }
            }
            r => println!("{}", r),
        }
    }
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
