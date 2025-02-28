// This lint is currently buggy and doesn't work properly with procedural macros (#[pyfunction]).
// Re-check in version 1.65.0.
#![allow(clippy::borrow_deref_ref)]
// Temporarily allow this lint before we update pyo3.
#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::types::PyModule;
use pyo3::{FromPyObject, PyAny, pyclass, pyfunction, pymethods, pymodule};
use pyo3::{Py, PyResult, Python, wrap_pyfunction};
use std::path::PathBuf;
use tokio::runtime::Builder;

use ::hyperqueue::HQ_VERSION;
use ::hyperqueue::transfer::connection::ClientSession;

use crate::client::job::{FailedTaskMap, forget_job_impl};
use crate::cluster::Cluster;
use crate::utils::run_future;
use client::job::{PyJobDescription, get_failed_tasks_impl, submit_job_impl, wait_for_jobs_impl};
use client::server::{connect_to_server_impl, stop_server_impl};

mod client;
pub mod cluster;
mod marshal;
mod utils;

// Client code
/// Opaque object that is returned by `connect_to_server` and then passed from the Python side
/// to various Rust functions.
#[pyclass]
pub struct HqClientContext {
    session: ClientSession,
}

type ClientContextPtr = Py<HqClientContext>;

type PyJobId = u32;
type PyTaskId = u32;

#[pyfunction]
fn get_hq_version() -> String {
    HQ_VERSION.to_string()
}

#[pyfunction]
fn connect_to_server(py: Python, directory: Option<String>) -> PyResult<ClientContextPtr> {
    connect_to_server_impl(py, directory)
}

#[pyfunction]
fn stop_server(py: Python, ctx: ClientContextPtr) -> PyResult<()> {
    stop_server_impl(py, ctx)
}

#[pyfunction]
fn submit_job(py: Python, ctx: ClientContextPtr, job: PyJobDescription) -> PyResult<PyJobId> {
    submit_job_impl(py, ctx, job)
}

#[pyfunction]
fn forget_job(py: Python, ctx: ClientContextPtr, job_id: PyJobId) -> PyResult<()> {
    forget_job_impl(py, ctx, job_id)
}

/// Wait until the specified jobs finish.
/// `progress_callback` is a function that receives [client::job::WaitStatus].
/// It will be periodically called during waiting.
#[pyfunction]
fn wait_for_jobs(
    py: Python,
    ctx: ClientContextPtr,
    job_ids: Vec<PyJobId>,
    progress_callback: &PyAny,
) -> PyResult<Vec<PyJobId>> {
    wait_for_jobs_impl(py, ctx, job_ids, progress_callback)
}

#[pyfunction]
fn get_failed_tasks(
    py: Python,
    ctx: ClientContextPtr,
    job_ids: Vec<PyJobId>,
) -> PyResult<FailedTaskMap> {
    get_failed_tasks_impl(py, ctx, job_ids)
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

    pub fn stop(&mut self) -> PyResult<()> {
        self.cluster.stop()?;
        Ok(())
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

    // Common
    m.add_function(wrap_pyfunction!(get_hq_version, m)?)?;

    // Client
    m.add_class::<HqClientContext>()?;

    m.add_function(wrap_pyfunction!(connect_to_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop_server, m)?)?;

    m.add_function(wrap_pyfunction!(submit_job, m)?)?;
    m.add_function(wrap_pyfunction!(forget_job, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_jobs, m)?)?;
    m.add_function(wrap_pyfunction!(get_failed_tasks, m)?)?;

    // Cluster
    m.add_class::<HqClusterContext>()?;

    m.add_function(wrap_pyfunction!(cluster_start, m)?)?;

    Ok(())
}
