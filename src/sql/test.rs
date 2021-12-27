use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::error::Result;
use crate::sql::parser::KVParser;


#[test]
fn test_parser() -> Result<()> {
    let sql = "select 1 + 1;";
    let ast = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
    println!("{:?}", ast);
    Ok(())
}