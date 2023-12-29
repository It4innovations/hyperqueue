use crate::client::status::{job_status, Status};
use crate::common::placeholders::{
    fill_placeholders_in_paths, CompletePlaceholderCtx, ResolvablePaths,
};
use crate::server::job::JobTaskState;
use crate::transfer::messages::{JobDescription, JobDetail, JobInfo, TaskDescription, TaskKind};
use crate::JobTaskId;
use std::path::PathBuf;
use tako::program::StdioDef;
use tako::Map;

pub struct ResolvedTaskPaths {
    pub cwd: PathBuf,
    pub stdout: StdioDef,
    pub stderr: StdioDef,
}

pub type TaskToPathsMap = Map<JobTaskId, Option<ResolvedTaskPaths>>;

/// Resolves task paths of the given job, as they would look like from the perspective of the worker.
pub fn resolve_task_paths(job: &JobDetail, server_uid: &str) -> TaskToPathsMap {
    let task_to_desc_map: Map<JobTaskId, &TaskDescription> = match &job.job_desc {
        JobDescription::Array { .. } => Default::default(),
        JobDescription::Graph { tasks } => tasks.iter().map(|t| (t.id, &t.task_desc)).collect(),
    };
    let get_task_desc = |id: JobTaskId| -> &TaskDescription {
        match &job.job_desc {
            JobDescription::Array { task_desc, .. } => task_desc,
            JobDescription::Graph { .. } => task_to_desc_map[&id],
        }
    };

    job.tasks
        .iter()
        .map(|task| {
            let task_desc = get_task_desc(task.task_id);
            let paths = match &task_desc.kind {
                TaskKind::ExternalProgram { program, .. } => match &task.state {
                    JobTaskState::Canceled {
                        started_data: Some(started_data),
                        ..
                    }
                    | JobTaskState::Running { started_data, .. }
                    | JobTaskState::Finished { started_data, .. }
                    | JobTaskState::Failed {
                        started_data: Some(started_data),
                        ..
                    } => {
                        let ctx = CompletePlaceholderCtx {
                            job_id: job.info.id,
                            task_id: task.task_id,
                            instance_id: started_data.context.instance_id,
                            submit_dir: &job.submit_dir,
                            server_uid,
                        };

                        let mut resolved_paths = ResolvedTaskPaths {
                            cwd: program.cwd.clone(),
                            stdout: program.stdout.clone(),
                            stderr: program.stderr.clone(),
                        };
                        let paths = ResolvablePaths {
                            cwd: &mut resolved_paths.cwd,
                            stdout: &mut resolved_paths.stdout,
                            stderr: &mut resolved_paths.stderr,
                        };
                        fill_placeholders_in_paths(paths, ctx);
                        Some(resolved_paths)
                    }
                    _ => None,
                },
            };
            (task.task_id, paths)
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub enum Verbosity {
    Normal,
    Verbose,
}

#[derive(clap::Args)]
pub struct VerbosityFlag {
    /// Use this flag to enable verbose output.
    #[arg(short = 'v', action = clap::ArgAction::Count)]
    verbose: u8,
}

impl From<VerbosityFlag> for Verbosity {
    fn from(flag: VerbosityFlag) -> Self {
        if flag.verbose >= 1 {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        }
    }
}

pub const JOB_SUMMARY_STATUS_ORDER: [Status; 5] = [
    Status::Running,
    Status::Waiting,
    Status::Finished,
    Status::Failed,
    Status::Canceled,
];

pub fn group_jobs_by_status(jobs: &[JobInfo]) -> Map<Status, u64> {
    let mut map = Map::new();
    for status in &JOB_SUMMARY_STATUS_ORDER {
        map.insert(*status, 0);
    }
    for job in jobs {
        *map.entry(job_status(job)).or_default() += 1;
    }
    map
}
