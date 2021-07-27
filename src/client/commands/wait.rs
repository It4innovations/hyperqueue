use indicatif::ProgressBar;

use crate::client::status::is_terminated;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobInfoRequest, JobSelector, ToClientMessage};
use crate::{rpc_call, JobId, Set};

use std::thread::sleep;
use std::time::Duration;

pub async fn wait_on_job(
    connection: &mut ClientConnection,
    selector: JobSelector,
) -> anyhow::Result<()> {
    let conn_ref = &mut *connection;
    let response = rpc_call!(
        conn_ref,
        FromClientMessage::JobInfo(JobInfoRequest {
            selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;
    let job_ids: Vec<JobId> = response
        .jobs
        .iter()
        .filter(|info| !is_terminated(info))
        .map(|info| info.id)
        .collect();

    if job_ids.is_empty() {
        log::warn!("There are no jobs to wait for");
    } else {
        log::info!("Waiting for {} job(s)", job_ids.len());

        let bar = ProgressBar::new(job_ids.len() as u64);
        let mut non_terminated_ids: Set<JobId> = job_ids.into_iter().collect();

        while !non_terminated_ids.is_empty() {
            let conn_ref = &mut *connection;
            let ids_ref = &mut non_terminated_ids;
            let response = rpc_call!(
                conn_ref,
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: JobSelector::Specific(ids_ref.iter().copied().collect()),
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await?;

            for job in &response.jobs {
                if is_terminated(job) {
                    non_terminated_ids.remove(&job.id);
                    bar.inc(1);
                }
            }

            sleep(Duration::from_secs(1));
        }
        bar.finish();
    }

    Ok(())
}
