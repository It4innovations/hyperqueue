use tako::messages::common::MemoryStats;
use tako::messages::gateway::CollectedOverview;
use tako::messages::gateway::OverviewRequest;
use tako::messages::worker::WorkerHwStateMessage;

use crate::common::error::HqError;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, JobDetail, JobDetailRequest, Selector, ToClientMessage,
};
use crate::{rpc_call, JobId};

pub async fn get_hw_overview(
    connection: &mut ClientConnection,
) -> Result<CollectedOverview, HqError> {
    let response = rpc_call!(
        connection,
        FromClientMessage::Overview(OverviewRequest {
            enable_hw_overview: true
        }),
        ToClientMessage::OverviewResponse(response) => response
    )
    .await;
    response
}

pub async fn get_running_job_info(
    connection: &mut ClientConnection,
) -> Result<Vec<(u32, JobDetail)>, HqError> {
    let overview = get_hw_overview(connection).await?;
    let all_jobs = get_job_list(connection).await?;
    let mut worker_task_list: Vec<(u32, JobDetail)> = vec![];

    for worker_overview in overview.worker_overviews {
        for (_job_id, job_detail) in all_jobs.clone() {
            if let Some(detail) = job_detail.clone() {
                for task in &detail.clone().tasks {
                    let task_worker_id = task.state.get_worker().unwrap_or(999);
                    if task_worker_id == worker_overview.id {
                        &worker_task_list.push((task_worker_id, detail.clone()));
                    }
                }
            }
        }
    }
    Ok(worker_task_list)
}

pub async fn get_job_list(
    connection: &mut ClientConnection,
) -> Result<Vec<(JobId, Option<JobDetail>)>, HqError> {
    let response = rpc_call!(
        connection,
        FromClientMessage::JobDetail(JobDetailRequest {
        selector: Selector::All,
        include_tasks: true
    }),
        ToClientMessage::JobDetailResponse(response) => response
    )
    .await;

    response
}

pub fn calculate_memory_usage_percent(memory_stats: &MemoryStats) -> u64 {
    if memory_stats.total == 0 {
        return 0;
    }
    (((memory_stats.free as f64) / (memory_stats.total as f64)) * 100.00) as u64
}

pub fn get_average_cpu_usage_for_worker(hw_state: &WorkerHwStateMessage) -> f32 {
    let num_cpus = hw_state
        .state
        .worker_cpu_usage
        .cpu_per_core_percent_usage
        .len();
    let cpu_usage_sum_per_core = hw_state
        .state
        .worker_cpu_usage
        .cpu_per_core_percent_usage
        .iter()
        .copied()
        .reduce(|cpu_a, cpu_b| (cpu_a + cpu_b))
        .unwrap_or(0.0);
    (cpu_usage_sum_per_core / num_cpus as f32) as f32
}
