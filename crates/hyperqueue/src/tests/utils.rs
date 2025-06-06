use crate::common::parser::{NomResult, consume_all};
use crate::common::parser2::CharParser;

pub fn check_parse_error<F: FnMut(&str) -> NomResult<O>, O>(
    parser: F,
    input: &str,
    expected_error: &str,
) {
    match consume_all(parser, input) {
        Err(e) => {
            let output = format!("{e:?}");
            assert_eq!(output, expected_error);
        }
        _ => panic!("The parser should have failed"),
    }
}

pub fn expect_parser_error<T: std::fmt::Debug>(parser: impl CharParser<T>, input: &str) -> String {
    let error = parser.parse_text(input).unwrap_err();
    format!("{error:?}")
}
