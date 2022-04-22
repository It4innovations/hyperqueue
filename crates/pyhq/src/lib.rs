use pyo3::types::PyModule;
use pyo3::{pyclass, pyfunction, pymethods, pymodule, FromPyObject};
use pyo3::{wrap_pyfunction, Py, PyResult, Python};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::runtime::Builder;

use hyperqueue::transfer::connection::ClientConnection;

use crate::cluster::Cluster;
use crate::utils::run_future;
use client::job::{get_error_messages_impl, submit_job_impl, wait_for_jobs_impl, JobDescription};
use client::server::{connect_to_server_impl, stop_server_impl};

mod client;
pub mod cluster;
mod marshal;
mod utils;

// Client code
/// Opaque object that is returned by `connect_to_server` and then passed from the Python side
/// to various Rust functions.
#[pyclass]
struct HqClientContext {
    connection: ClientConnection,
}

type ClientContextPtr = Py<HqClientContext>;

type PyJobId = u32;
type PyTaskId = u32;

#[pyfunction]
fn connect_to_server(py: Python, directory: Option<String>) -> PyResult<ClientContextPtr> {
    connect_to_server_impl(py, directory)
}

#[pyfunction]
fn stop_server(py: Python, ctx: ClientContextPtr) -> PyResult<()> {
    stop_server_impl(py, ctx)
}

#[pyfunction]
fn submit_job(py: Python, ctx: ClientContextPtr, job: JobDescription) -> PyResult<PyJobId> {
    submit_job_impl(py, ctx, job)
}

#[pyfunction]
fn wait_for_jobs(py: Python, ctx: ClientContextPtr, job_ids: Vec<PyJobId>) -> PyResult<u32> {
    wait_for_jobs_impl(py, ctx, job_ids)
}

#[pyfunction]
fn get_error_messages(
    py: Python,
    ctx: ClientContextPtr,
    job_ids: Vec<PyJobId>,
) -> PyResult<HashMap<PyJobId, HashMap<PyTaskId, String>>> {
    get_error_messages_impl(py, ctx, job_ids)
}

// Server code
#[pyclass]
struct HqClusterContext {
    cluster: Cluster,
}

#[pymethods]
impl HqClusterContext {
    #[getter]
    pub fn server_dir(&self) -> String {
        self.cluster.server_dir().to_string_lossy().into_owned()
    }

    pub fn stop(&mut self) {
        self.cluster.stop();
    }

    pub fn add_worker(&mut self, cores: usize) -> PyResult<()> {
        self.cluster.add_worker(cores)?;
        Ok(())
    }
}

#[pyfunction]
fn cluster_start(_py: Python, path: Option<String>) -> PyResult<HqClusterContext> {
    let path: Option<PathBuf> = path.map(|p| p.into());
    let cluster = Cluster::start(path)?;
    Ok(HqClusterContext { cluster })
}

#[pymodule]
fn hyperqueue(_py: Python, m: &PyModule) -> PyResult<()> {
    // Use a single-threaded Tokio runtime for all Rust async operations
    let mut builder = Builder::new_current_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    // Client
    m.add_class::<HqClientContext>()?;

    m.add_function(wrap_pyfunction!(connect_to_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop_server, m)?)?;

    m.add_function(wrap_pyfunction!(submit_job, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_jobs, m)?)?;
    m.add_function(wrap_pyfunction!(get_error_messages, m)?)?;

    // Cluster
    m.add_class::<HqClusterContext>()?;

    m.add_function(wrap_pyfunction!(cluster_start, m)?)?;

    Ok(())
}
