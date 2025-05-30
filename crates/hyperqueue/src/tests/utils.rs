use crate::HQ_VERSION;
use crate::common::parser::{NomResult, consume_all};
use crate::common::parser2::CharParser;
use crate::server::state::StateRef;
use crate::transfer::messages::ServerInfo;
use chrono::Utc;

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

pub fn create_hq_state() -> StateRef {
    StateRef::new(ServerInfo {
        server_uid: "testuid".to_string(),
        client_host: "test".to_string(),
        worker_host: "test".to_string(),
        client_port: 1200,
        worker_port: 1400,
        version: HQ_VERSION.to_string(),
        pid: std::process::id(),
        start_date: Utc::now(),
        journal_path: None,
    })
}
