use std::future::Future;

use tokio::task::{JoinHandle, LocalSet};

use crate::common::parser::{consume_all, NomResult};
use crate::server::state::StateRef;

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

pub async fn run_concurrent<
    R: 'static,
    Fut1: 'static + Future<Output = R>,
    Fut2: Future<Output = ()>,
>(
    background_fut: Fut1,
    fut: Fut2,
) -> (LocalSet, JoinHandle<R>) {
    let set = tokio::task::LocalSet::new();
    let handle = set.spawn_local(background_fut);
    set.run_until(fut).await;
    (set, handle)
}

pub fn create_hq_state() -> StateRef {
    StateRef::new(Default::default())
}
