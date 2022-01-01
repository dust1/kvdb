use crate::error::Result;
use crate::sql::parser::KVParser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
        exec_sql(sql)?;
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
        exec_sql(sql)?;
    }

    Ok(())
}

fn exec_sql(sql: &str) -> Result<()> {
    let parser = KVParser::build(sql)?;
    println!("{:?}", parser);
    Ok(())
}
