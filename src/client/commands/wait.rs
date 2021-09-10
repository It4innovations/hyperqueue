use std::time::SystemTime;

use crate::client::globalsettings::GlobalSettings;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, Selector, ToClientMessage, WaitForJobsRequest};

pub async fn wait_for_jobs(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    selector: Selector,
) -> anyhow::Result<()> {
    let start = SystemTime::now();
    let response = rpc_call!(
        connection,
        FromClientMessage::WaitForJobs(WaitForJobsRequest {
            selector,
        }),
        ToClientMessage::WaitForJobsResponse(r) => r
    )
    .await?;

    let duration = start.elapsed()?;
    gsettings.printer().print_job_wait(duration, &response);

    if response.failed > 0 || response.canceled > 0 {
        return Err(anyhow::anyhow!(
            "Some jobs have failed or have been canceled"
        ));
    }

    Ok(())
}
