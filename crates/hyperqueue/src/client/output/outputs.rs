use crate::transfer::messages::{
    AutoAllocListQueuesResponse, JobDetail, JobInfo, ServerInfo, WaitForJobsResponse, WorkerInfo,
};

use crate::client::job::WorkerMap;
use crate::server::autoalloc::Allocation;
use crate::stream::reader::outputlog::Summary;
use std::path::Path;

use crate::client::output::Verbosity;
use crate::client::output::common::TaskToPathsMap;
use crate::common::arraydef::IntArray;
use crate::server::job::JobTaskInfo;
use core::time::Duration;
use tako::resources::ResourceDescriptor;
use tako::server::TaskExplanation;
use tako::{JobId, JobTaskId, TaskId};

pub const MAX_DISPLAYED_WORKERS: usize = 2;

#[derive(clap::ValueEnum, Clone)]
pub enum Outputs {
    CLI,
    JSON,
    Quiet,
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum OutputStream {
    /// Displays stdout output stream
    Stdout,
    /// Displays stderr output stream
    Stderr,
}

pub trait Output {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>);
    fn print_worker_info(&self, worker_info: WorkerInfo);

    // Server
    fn print_server_info(&self, server_dir: Option<&Path>, record: &ServerInfo);

    // Jobs
    fn print_job_submitted(&self, job: JobDetail);

    fn print_job_open(&self, job_id: JobId);
    fn print_job_list(&self, jobs: Vec<JobInfo>, total_jobs: usize);
    fn print_job_summary(&self, jobs: Vec<JobInfo>);
    fn print_job_detail(&self, jobs: Vec<JobDetail>, worker_map: WorkerMap, server_uid: &str);
    fn print_job_workdir(&self, jobs: Vec<JobDetail>, server_uid: &str);
    fn print_job_wait(
        &self,
        duration: Duration,
        response: &WaitForJobsResponse,
        details: &[(JobId, Option<JobDetail>)],
        worker_map: WorkerMap,
    );
    fn print_job_output(
        &self,
        job: JobDetail,
        output_stream: OutputStream,
        task_header: bool,
        task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()>;

    // Tasks
    fn print_task_list(
        &self,
        jobs: Vec<(JobId, JobDetail)>,
        worker_map: WorkerMap,
        server_uid: &str,
        verbosity: Verbosity,
    );
    fn print_task_info(
        &self,
        job: (JobId, JobDetail),
        tasks: &[(JobTaskId, JobTaskInfo)],
        worker_map: WorkerMap,
        server_uid: &str,
        verbosity: Verbosity,
    );
    fn print_task_ids(&self, jobs_task_id: Vec<(JobId, IntArray)>);
    fn print_task_workdir(&self, jobs: Vec<(JobId, JobDetail)>, server_uid: &str);

    // Stream
    fn print_summary(&self, path: &Path, summary: Summary);

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListQueuesResponse);
    fn print_allocations(&self, allocations: Vec<Allocation>);
    fn print_allocation_output(
        &self,
        allocation: Allocation,
        stream: OutputStream,
    ) -> anyhow::Result<()>;

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor);

    fn print_error(&self, error: anyhow::Error);

    fn print_explanation(&self, task_id: TaskId, explanation: &TaskExplanation);
}
