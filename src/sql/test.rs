use crate::error::Result;
use crate::sql::parser::KVParser;



use crate::sql::plan::Plan;

use crate::sql::schema::{Catalog, Table};

#[test]
fn test() -> Result<()> {
    let sql = "SELECT SELECT WHERE ORDER BY LIMIT";
    parser_sql(sql);
    Ok(())
}

#[test]
fn test_plan() -> Result<()> {
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
        Err(e) => println!("Error: =>>>> {:?}", e)
    }
    Ok(())
}

struct TestLog {}

impl TestLog {
    pub fn new() -> Self {
        TestLog {}
    }
}

impl Catalog for TestLog {
    fn read_table(&self, _table: &str) -> Result<Option<Table>> {
        todo!()
    }
}

fn plan_sql(sql: &str) -> Result<()> {
    let statement = KVParser::build(sql)?.parser()?;
    let mut test_log = TestLog::new();
    let _plan = Plan::build(statement, &mut test_log)?;
    Ok(())
}