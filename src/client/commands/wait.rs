use std::time::SystemTime;

use crate::client::utils::{TASK_COLOR_CANCELED, TASK_COLOR_FAILED, TASK_COLOR_FINISHED};
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, Selector, ToClientMessage, WaitForJobsRequest};
use colored::{Color, Colorize};

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

    let mut format = |count: u32, action: &str, color| {
        if count > 0 {
            let job = if count == 1 { "job" } else { "jobs" };
            msgs.push(
                format!("{} {} {}", count, job, action)
                    .color(color)
                    .to_string(),
            );
        }
    };

    format(response.finished, "finished", TASK_COLOR_FINISHED);
    format(response.failed, "failed", TASK_COLOR_FAILED);
    format(response.canceled, "canceled", TASK_COLOR_CANCELED);
    format(response.invalid, "invalid", Color::BrightRed);

    log::info!(
        "Wait finished in {}: {}",
        humantime::format_duration(duration),
        msgs.join(", ")
    );

    if response.failed > 0 || response.canceled > 0 {
        return Err(anyhow::anyhow!(
            "Some jobs have failed or have been canceled"
        ));
    }

    Ok(())
}
