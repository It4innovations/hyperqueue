use std::time::SystemTime;

use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, Selector, ToClientMessage, WaitForJobsRequest};

pub async fn wait_for_jobs(
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

    let mut msgs = vec![];

    let mut format = |count: u32, action: &str| {
        if count > 0 {
            let job = if count == 1 { "job" } else { "jobs" };
            msgs.push(format!("{} {} {}", count, job, action));
        }
    };

    format(response.finished, "finished");
    format(response.failed, "failed");
    format(response.skipped, "skipped");
    format(response.invalid, "invalid");

    log::info!(
        "Wait finished in {}: {}",
        humantime::format_duration(duration),
        msgs.join(", ")
    );

    if response.failed > 0 {
        return Err(anyhow::anyhow!(
            "Some jobs have failed or have been canceled"
        ));
    }

    Ok(())
}
