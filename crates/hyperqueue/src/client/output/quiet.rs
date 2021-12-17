use crate::client::job::WorkerMap;
use crate::client::output::outputs::Output;
use crate::client::status::{job_status, Status};
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{Allocation, AllocationEventHolder};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, LostWorkerReasonInfo, StatsResponse,
    WaitForJobsResponse, WorkerExitInfo, WorkerInfo,
};
use anyhow::Error;
use std::path::Path;
use std::time::Duration;
use tako::common::resources::ResourceDescriptor;

fn to_str(stat: &Status) -> anyhow::Result<&str> {
    Ok(match stat {
        Status::Waiting => "WAITING",
        Status::Running => "RUNNING",
        Status::Finished => "FINISHED",
        Status::Failed => "FAILED",
        Status::Canceled => "CANCELED",
    })
}

#[derive(Default)]
pub struct Quiet;

impl Output for Quiet {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        for worker in workers {
            let worker_status = match worker.ended.as_ref() {
                None => "RUNNING",
                Some(WorkerExitInfo {
                    reason: LostWorkerReasonInfo::ConnectionLost,
                    ..
                }) => "CONNECTION LOST",
                Some(WorkerExitInfo {
                    reason: LostWorkerReasonInfo::HeartbeatLost,
                    ..
                }) => "HEARTBEAT LOST",
                Some(WorkerExitInfo {
                    reason: LostWorkerReasonInfo::IdleTimeout,
                    ..
                }) => "IDLE TIMEOUT",
                Some(WorkerExitInfo {
                    reason: LostWorkerReasonInfo::Stopped,
                    ..
                }) => "STOPPED",
            };
            println!("{} {}", worker.id, worker_status)
        }
    }
    fn print_worker_info(&self, _worker_info: WorkerInfo) {}

    // Server
    fn print_server_record(&self, server_dir: &Path, _record: &AccessRecord) {
        println!("{}", server_dir.to_str().unwrap())
    }
    fn print_server_stats(&self, _stats: StatsResponse) {}

    // Jobs
    fn print_job_submitted(&self, job: JobDetail) {
        println!("{}", job.info.id)
    }
    fn print_job_list(&self, tasks: Vec<JobInfo>) {
        for task in tasks {
            let status = job_status(&task);

            println!(
                "{} {}",
                task.id,
                String::from(to_str(&status).unwrap_or("ERROR"))
            )
        }
    }
    fn print_job_detail(&self, _job: JobDetail, _show_tasks: bool, _worker_map: WorkerMap) {}
    fn print_job_tasks(
        &self,
        _completion_date_or_now: chrono::DateTime<chrono::Utc>,
        _tasks: Vec<JobTaskInfo>,
        _show_tasks: bool,
        _counters: &JobTaskCounters,
        _worker_map: &WorkerMap,
    ) {
    }
    fn print_job_wait(&self, _duration: Duration, _response: &WaitForJobsResponse) {
        println!("\n")
    }

    // Log
    fn print_summary(&self, _filename: &Path, _summary: Summary) {}

    // Autoalloc
    fn print_autoalloc_queues(&self, _info: AutoAllocListResponse) {}
    fn print_event_log(&self, _events: Vec<AllocationEventHolder>) {}
    fn print_allocations(&self, _allocations: Vec<Allocation>) {}

    // Hw
    fn print_hw(&self, _descriptor: &ResourceDescriptor) {}

    fn print_error(&self, error: Error) {
        eprintln!("{:?}", error);
    }
}
