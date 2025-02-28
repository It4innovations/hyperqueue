pub mod error;

use std::future::Future;
use tokio::task::LocalSet;

/// Borrow a Python object from a Py pointer.
#[macro_export]
macro_rules! borrow {
    ($py:expr, $instance:expr) => {
        $instance.as_ref($py).borrow()
    };
}

/// Borrow a Python object mutably from a Py pointer.
#[macro_export]
macro_rules! borrow_mut {
    ($py:expr, $instance:expr) => {
        $instance.bind($py).borrow_mut()
    };
}

/// Run the provided future to completion using a global Tokio runtime managed by `pyo3_async_runtimes`.
pub(crate) fn run_future<F: Future>(future: F) -> F::Output {
    let runtime = pyo3_async_runtimes::tokio::get_runtime();
    let set = LocalSet::new();
    set.block_on(runtime, future)
}
