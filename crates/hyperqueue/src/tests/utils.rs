use crate::common::parser::{consume_all, NomResult};

pub fn check_parse_error<F: FnMut(&str) -> NomResult<O>, O>(
    parser: F,
    input: &str,
    expected_error: &str,
) {
    match consume_all(parser, input) {
        Err(e) => {
            let output = format!("{:?}", e);
            assert_eq!(output, expected_error);
        }
        _ => panic!("The parser should have failed"),
    }
}
