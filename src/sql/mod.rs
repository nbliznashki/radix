pub mod projection;

pub use projection::*;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn sql2ast(sqlstmt: &str) -> Vec<Statement> {
    let dialect = GenericDialect {}; // or AnsiDialect
    let ast = Parser::parse_sql(&dialect, sqlstmt).unwrap();
    ast
}
