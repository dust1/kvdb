use crate::error::Result;
use crate::sql::parser::KVParser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn test_parser() -> Result<()> {
    let sqls = [
        "SELECT LastName,FirstName FROM Persons",
        "SELECT * FROM Persons WHERE City='Beijing'",
        "SELECT * FROM Persons WHERE FirstName='Thomas' AND LastName='Carter'",
        "SELECT Company, OrderNumber FROM Orders ORDER BY Company",
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
        "DROP TABLE Company"
    ];

    let mut result = vec![];
    for sql in sqls {
        let parser = KVParser::build(sql)?;
        result.push(format!("{:?}", parser));
    }

    for re in result {
        println!("{}", re);
    }
    Ok(())
}
