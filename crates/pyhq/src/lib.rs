use pyo3::types::PyModule;
use pyo3::{pyclass, pyfunction, pymodule};
use pyo3::{wrap_pyfunction, Py, PyResult, Python};
use tokio::runtime::Builder;

use crate::utils::run_future;
use hyperqueue::client::default_server_directory_path;
use hyperqueue::server::bootstrap::get_client_connection;
use hyperqueue::transfer::connection::ClientConnection;
use hyperqueue::transfer::messages::FromClientMessage;

mod utils;

/// Opaque object that is returned by `connect_to_server` and then passed from the Python side
/// to various Rust functions.
#[pyclass]
struct HqContext {
    connection: ClientConnection,
}

#[pyfunction]
fn connect_to_server(py: Python) -> PyResult<Py<HqContext>> {
    run_future(async move {
        let connection = get_client_connection(&default_server_directory_path()).await?;
        let context = HqContext { connection };
        Py::new(py, context)
    })
}

#[pyfunction]
fn stop_server(py: Python, ctx: Py<HqContext>) -> PyResult<()> {
    run_future(async move {
        let mut ctx = borrow_mut!(py, ctx);
        ctx.connection.send(FromClientMessage::Stop).await.unwrap();
        Ok(())
    })
}

#[pymodule]
fn pyhq(_py: Python, m: &PyModule) -> PyResult<()> {
    // Use a single-threaded Tokio runtime for all Rust async operations
    let mut builder = Builder::new_current_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    m.add_class::<HqContext>()?;
    m.add_function(wrap_pyfunction!(connect_to_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop_server, m)?)?;
    Ok(())
}
