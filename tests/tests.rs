#[cfg(test)]
mod test {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn parse_test() {
        let dialect = &GenericDialect {};
        let sql = "SHOW tables;";
        let stmts = Parser::parse_sql(dialect, sql).unwrap();
        for stmt in stmts {
            println!("{:?}", stmt);
        }
    }
}
