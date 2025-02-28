use pyo3::{Py, PyResult, Python};
use std::path::PathBuf;

use hyperqueue::client::default_server_directory_path;
use hyperqueue::server::bootstrap::get_client_session;
use hyperqueue::transfer::messages::FromClientMessage;

use crate::utils::error::ToPyResult;
use crate::{ClientContextPtr, HqClientContext, borrow_mut, run_future};

pub(crate) fn connect_to_server_impl(
    py: Python,
    directory: Option<String>,
) -> PyResult<ClientContextPtr> {
    let directory = directory
        .map(|p| -> PathBuf { p.into() })
        .unwrap_or_else(default_server_directory_path);

    run_future(async move {
        let session = get_client_session(&directory).await?;
        let context = HqClientContext { session };
        Py::new(py, context)
    })
}

pub(crate) fn stop_server_impl(py: Python, ctx: ClientContextPtr) -> PyResult<()> {
    run_future(async move {
        let mut ctx = borrow_mut!(py, ctx);
        ctx.session
            .connection()
            .send(FromClientMessage::Stop)
            .await
            .map_py_err()?;
        Ok(())
    })
}
