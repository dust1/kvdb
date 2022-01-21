use crate::error::Result;
use crate::sql::engine::kv::KV;
use crate::sql::parser::KVParser;

use crate::sql::plan::Plan;

use crate::storage::kv::engine::KVStoreEngine;
use crate::storage::memory::Memory;

use super::execution::ResultSet;

#[test]
fn test() -> Result<()> {
    let sql = "SELECT SELECT WHERE ORDER BY LIMIT";
    parser_sql(sql);
    Ok(())
}

#[test]
fn test_plan() -> Result<()> {
    let sqls = [
        "CREATE TABLE movies (id INTEGER PRIMARY KEY, title VARCHAR NOT NULL);",
        "INSERT INTO movies VALUES (1, 'Sicario'), (2, 'Stalker'), (3, 'Her');",
        "SELECT * FROM movies;",
        "SELECT id as num FROM movies where id < 2;",
        "SELECT id as num FROM movies where id >= 3;",
        "UPDATE movies SET title = 'His' WHERE id = 3;",
        "SELECT * FROM movies;",
        "DELETE FROM movies WHERE id = 3;",
        "SELECT * FROM movies order by id DESC;",
    ];
    let store = Box::new(Memory::new());
    let store_engine = KVStoreEngine::new(store);
    let mut kv = KV::new(store_engine);

    for sql in sqls {
        let statement = KVParser::build(sql)?.parser()?;
        let plan = Plan::build(statement, &mut kv)?;
        let result = plan.optimize(&mut kv)?.execute(&mut kv)?;
        println!("sql => {:?}", sql);
        match result {
            ResultSet::Query { columns, rows } => {
                println!("columns => {:?}", columns);
                for row in rows {
                    match row {
                        Ok(res) => {
                            println!("rows => {:?}", res);
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
            res => {
                println!("{:?}", res);
            }
        }
        println!("---------------------");
    }
    Ok(())
}

#[test]
fn test_select_plan() -> Result<()> {
    let sqls = ["SELECT id as num from movies order by num;"];

    for sql in sqls {
        plan_sql(sql)?;
    }

    Ok(())
}

#[test]
fn test_select_parser() -> Result<()> {
    let sqls = [
        "SELECT 1;",
        "SELECT 1+1;",
        "SELECT LastName,FirstName FROM Persons",
        "SELECT * FROM Persons",
        "SELECT * FROM Persons WHERE City='Beijing'",
        "SELECT * FROM Persons JOIN Class ON Persons.classId = Class.Id WHERE Persons.City='Beijing'",
        "SELECT * FROM Persons, Class",
        "SELECT * FROM Persons, Class WHERE Persons.City='Beijing' AND Persons.classId = Class.Id",
        "SELECT * FROM Persons WHERE FirstName='Thomas' AND LastName='Carter'",
        "SELECT Company, OrderNumber FROM Orders ORDER BY Company",
        "SELECT Company C, OrderNumber AS OrderN FROM Orders ORDER BY Company",
        "SELECT Persons.Id FROM Persons",
        "SELECT Id as NUM, Name FROM Persons"
    ];

    for sql in sqls {
        parser_sql(sql)?;
    }

    Ok(())
}

#[test]
fn test_insert_parser() -> Result<()> {
    let sqls = [
        "INSERT INTO Persons VALUES ('Gates', 'Bill', 'Xuanwumen 10', 'Beijing')",
        "INSERT INTO Persons (LastName, Address) VALUES ('Wilson', 'Champs-Elysees')",
    ];

    for sql in sqls {
        parser_sql(sql)?;
    }

    Ok(())
}

#[test]
fn test_update_parser() -> Result<()> {
    let sqls = ["UPDATE Person SET FirstName = 'Fred' WHERE LastName = 'Wilson'"];

    for sql in sqls {
        parser_sql(sql)?;
    }

    Ok(())
}

#[test]
fn test_parser() -> Result<()> {
    let sqls = [
        "INSERT INTO Persons VALUES ('Gates', 'Bill', 'Xuanwumen 10', 'Beijing')",
        "INSERT INTO Persons (LastName, Address) VALUES ('Wilson', 'Champs-Elysees')",
        "UPDATE Person SET FirstName = 'Fred' WHERE LastName = 'Wilson'",
        "DELETE FROM Person WHERE LastName = 'Wilson'",
        "CREATE TABLE Persons
        (
        Id_P int,
        LastName varchar(255),
        FirstName varchar(255),
        Address varchar(255),
        City varchar(255)
        )",
        "DROP TABLE Company",
    ];

    for sql in sqls {
        parser_sql(sql)?;
    }

    Ok(())
}

fn parser_sql(sql: &str) -> Result<()> {
    let parser = KVParser::build(sql);
    match parser {
        Ok(p) => println!("{:?}", p),
        Err(e) => println!("Error: =>>>> {:?}", e),
    }
    Ok(())
}

fn plan_sql(sql: &str) -> Result<()> {
    let store = Box::new(Memory::new());
    let store_engine = KVStoreEngine::new(store);
    let mut kv = KV::new(store_engine);

    let statement = KVParser::build(sql)?.parser()?;
    let _plan = Plan::build(statement, &mut kv)?;
    Ok(())
}
