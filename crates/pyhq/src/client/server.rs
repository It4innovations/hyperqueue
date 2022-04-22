use pyo3::{Py, PyResult, Python};
use std::path::PathBuf;

use hyperqueue_core::client::default_server_directory_path;
use hyperqueue_core::server::bootstrap::get_client_connection;
use hyperqueue_core::transfer::messages::FromClientMessage;

use crate::utils::error::ToPyResult;
use crate::{borrow_mut, run_future, ClientContextPtr, HqClientContext};

pub(crate) fn connect_to_server_impl(
    py: Python,
    directory: Option<String>,
) -> PyResult<ClientContextPtr> {
    let directory = directory
        .map(|p| -> PathBuf { p.into() })
        .unwrap_or_else(default_server_directory_path);

    run_future(async move {
        let connection = get_client_connection(&directory).await?;
        let context = HqClientContext { connection };
        Py::new(py, context)
    })
}

pub(crate) fn stop_server_impl(py: Python, ctx: ClientContextPtr) -> PyResult<()> {
    run_future(async move {
        let mut ctx = borrow_mut!(py, ctx);
        ctx.connection
            .send(FromClientMessage::Stop)
            .await
            .map_py_err()?;
        Ok(())
    })
}
