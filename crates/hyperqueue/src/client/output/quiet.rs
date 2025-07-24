use std::path::Path;
use std::time::Duration;

use anyhow::Error;

use tako::gateway::LostWorkerReason;
use tako::resources::ResourceDescriptor;

use crate::client::job::WorkerMap;
use crate::client::output::cli::print_job_output;
use crate::client::output::common::{
    JOB_SUMMARY_STATUS_ORDER, TaskToPathsMap, Verbosity, group_jobs_by_status,
};
use crate::client::output::outputs::{Output, OutputStream};
use crate::client::status::{Status, job_status};
use crate::common::arraydef::IntArray;
use crate::server::autoalloc::Allocation;
use crate::server::job::JobTaskInfo;
use crate::stream::reader::outputlog::Summary;
use crate::transfer::messages::{
    AutoAllocListQueuesResponse, JobDetail, JobInfo, ServerInfo, WaitForJobsResponse,
    WorkerExitInfo, WorkerInfo,
};
use tako::server::TaskExplanation;
use tako::{JobId, JobTaskId, TaskId};

#[derive(Default)]
pub struct Quiet;

impl Output for Quiet {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        for worker in workers {
            let worker_status = match worker.ended.as_ref() {
                None => "RUNNING",
                Some(WorkerExitInfo {
                    reason: LostWorkerReason::ConnectionLost,
                    ..
                }) => "CONNECTION LOST",
                Some(WorkerExitInfo {
                    reason: LostWorkerReason::HeartbeatLost,
                    ..
                }) => "HEARTBEAT LOST",
                Some(WorkerExitInfo {
                    reason: LostWorkerReason::IdleTimeout,
                    ..
                }) => "IDLE TIMEOUT",
                Some(WorkerExitInfo {
                    reason: LostWorkerReason::TimeLimitReached,
                    ..
                }) => "TIME LIMIT REACHED",
                Some(WorkerExitInfo {
                    reason: LostWorkerReason::Stopped,
                    ..
                }) => "STOPPED",
            };
            println!("{} {}", worker.id, worker_status)
        }
    }
    fn print_worker_info(&self, _worker_info: WorkerInfo) {}

    // Server
    fn print_server_info(&self, server_dir: Option<&Path>, _record: &ServerInfo) {
        if let Some(dir) = server_dir {
            println!("{}", dir.display());
        }
    }

    // Jobs
    fn print_job_submitted(&self, job: JobDetail) {
        println!("{}", job.info.id)
    }

    fn print_job_open(&self, job_id: JobId) {
        println!("{job_id}");
    }

    fn print_job_list(&self, jobs: Vec<JobInfo>, _total_jobs: usize) {
        for task in jobs {
            let status = job_status(&task);
            println!("{} {}", task.id, format_status(&status))
        }
    }
    fn print_job_summary(&self, jobs: Vec<JobInfo>) {
        let statuses = group_jobs_by_status(&jobs);
        for status in &JOB_SUMMARY_STATUS_ORDER {
            let count = statuses.get(status).copied().unwrap_or_default();
            let status = match status {
                Status::Waiting => "WAITING",
                Status::Running => "RUNNING",
                Status::Finished => "FINISHED",
                Status::Failed => "FAILED",
                Status::Canceled => "CANCELED",
                Status::Opened => "OPENED",
            };

            println!("{status} {count}");
        }
    }
    fn print_job_detail(&self, _jobs: Vec<JobDetail>, _worker_map: WorkerMap, _server_uid: &str) {}

    fn print_job_wait(
        &self,
        _duration: Duration,
        _response: &WaitForJobsResponse,
        _details: &[(JobId, Option<JobDetail>)],
        _worker_map: WorkerMap,
    ) {
    }
    fn print_job_output(
        &self,
        job_detail: JobDetail,
        output_stream: OutputStream,
        task_header: bool,
        task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()> {
        print_job_output(job_detail, output_stream, task_header, task_paths)
    }

    // Tasks
    fn print_task_list(
        &self,
        _jobs: Vec<(JobId, JobDetail)>,
        _worker_map: WorkerMap,
        _server_uid: &str,
        _verbosity: Verbosity,
    ) {
    }

    fn print_task_info(
        &self,
        _job: (JobId, JobDetail),
        _tasks: &[(JobTaskId, JobTaskInfo)],
        _worker_map: WorkerMap,
        _server_uid: &str,
        _verbosity: Verbosity,
    ) {
    }

    fn print_task_ids(&self, _job_task_ids: Vec<(JobId, IntArray)>) {}

    // Stream
    fn print_summary(&self, _filename: &Path, _summary: Summary) {}

    // Autoalloc
    fn print_autoalloc_queues(&self, _info: AutoAllocListQueuesResponse) {}
    fn print_allocations(&self, _allocations: Vec<Allocation>) {}
    fn print_allocation_output(
        &self,
        _allocation: Allocation,
        _stream: OutputStream,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    // Hw
    fn print_hw(&self, _descriptor: &ResourceDescriptor) {}

    fn print_error(&self, error: Error) {
        eprintln!("{error:?}");
    }

    fn print_explanation(&self, _task_id: TaskId, _explanation: &TaskExplanation) {}
}

fn format_status(status: &Status) -> &str {
    match status {
        Status::Waiting => "WAITING",
        Status::Running => "RUNNING",
        Status::Finished => "FINISHED",
        Status::Failed => "FAILED",
        Status::Canceled => "CANCELED",
        Status::Opened => "OPENED",
    }
}
