use pyo3::types::PyModule;
use pyo3::{pyclass, pyfunction, pymodule, FromPyObject};
use pyo3::{wrap_pyfunction, Py, PyResult, Python};
use tokio::runtime::Builder;

use hyperqueue::transfer::connection::ClientConnection;

use crate::job::{submit_job_impl, wait_for_jobs_impl, JobDescription};
use crate::server::{connect_to_server_impl, stop_server_impl};
use crate::utils::run_future;

mod job;
mod marshal;
mod server;
mod utils;

/// Opaque object that is returned by `connect_to_server` and then passed from the Python side
/// to various Rust functions.
#[pyclass]
struct HqContext {
    connection: ClientConnection,
}

type ContextPtr = Py<HqContext>;

type PyJobId = u32;

#[pyfunction]
fn connect_to_server(py: Python, directory: Option<String>) -> PyResult<ContextPtr> {
    connect_to_server_impl(py, directory)
}

#[pyfunction]
fn stop_server(py: Python, ctx: ContextPtr) -> PyResult<()> {
    stop_server_impl(py, ctx)
}

#[pyfunction]
fn submit_job(py: Python, ctx: ContextPtr, job: JobDescription) -> PyResult<PyJobId> {
    submit_job_impl(py, ctx, job)
}

#[pyfunction]
fn wait_for_jobs(py: Python, ctx: ContextPtr, job_ids: Vec<PyJobId>) -> PyResult<u32> {
    wait_for_jobs_impl(py, ctx, job_ids)
}

#[pymodule]
fn hyperqueue(_py: Python, m: &PyModule) -> PyResult<()> {
    // Use a single-threaded Tokio runtime for all Rust async operations
    let mut builder = Builder::new_current_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    m.add_class::<HqContext>()?;

    // Server
    m.add_function(wrap_pyfunction!(connect_to_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop_server, m)?)?;

    // Jobs
    m.add_function(wrap_pyfunction!(submit_job, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_jobs, m)?)?;

    Ok(())
}
