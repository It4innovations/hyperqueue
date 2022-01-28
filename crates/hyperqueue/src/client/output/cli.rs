use cli_table::format::{Justify, Separator};
use cli_table::{print_stdout, Cell, CellStruct, Color, ColorChoice, Style, Table, TableStruct};

use std::fmt::{Display, Write};
use std::io::Write as write;

use crate::client::job::WorkerMap;
use crate::client::output::outputs::{Output, OutputStream, MAX_DISPLAYED_WORKERS};
use crate::client::status::{get_task_status, job_status, Status};
use crate::common::env::is_hq_env;
use crate::common::format::{human_duration, human_size};
use crate::common::manager::info::GetManagerInfo;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{
    Allocation, AllocationEvent, AllocationEventHolder, AllocationStatus,
};
use crate::server::job::{JobTaskCounters, JobTaskInfo, JobTaskState, StartedTaskData};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDescription, JobDetail, JobInfo, StatsResponse, TaskDescription,
    WaitForJobsResponse, WorkerExitInfo, WorkerInfo,
};
use crate::{JobTaskCount, WorkerId};

use chrono::{DateTime, Local, SubsecRound, Utc};
use core::time::Duration;
use humantime::format_duration;

use std::path::Path;
use std::time::SystemTime;

use tako::common::resources::{CpuRequest, ResourceDescriptor};
use tako::messages::common::StdioDef;

use crate::client::output::common::{resolve_task_paths, TaskToPathsMap};
use crate::common::strutils::{pluralize, select_plural};
use crate::worker::start::WORKER_EXTRA_PROCESS_PID;
use anyhow::Error;
use colored::Color as Colorization;
use colored::Colorize;
use std::collections::BTreeSet;
use std::fs::File;
use tako::messages::gateway::{LostWorkerReason, ResourceRequest};

pub const TASK_COLOR_CANCELED: Colorization = Colorization::Magenta;
pub const TASK_COLOR_FAILED: Colorization = Colorization::Red;
pub const TASK_COLOR_FINISHED: Colorization = Colorization::Green;
pub const TASK_COLOR_RUNNING: Colorization = Colorization::Yellow;
pub const TASK_COLOR_INVALID: Colorization = Colorization::BrightRed;

const TERMINAL_WIDTH: usize = 80;

pub struct CliOutput {
    color_policy: ColorChoice,
}

impl CliOutput {
    pub fn new(color_policy: ColorChoice) -> CliOutput {
        CliOutput { color_policy }
    }

    fn print_vertical_table(&self, rows: Vec<Vec<CellStruct>>) {
        let table = rows.table().separator(
            Separator::builder()
                .column(Some(Default::default()))
                .build(),
        );
        self.print_table(table);
    }

    fn print_horizontal_table(&self, rows: Vec<Vec<CellStruct>>, header: Vec<CellStruct>) {
        let table = rows
            .table()
            .separator(
                Separator::builder()
                    .title(Some(Default::default()))
                    .column(Some(Default::default()))
                    .build(),
            )
            .title(header);
        self.print_table(table);
    }

    fn print_table(&self, table: TableStruct) {
        let table = table.color_choice(self.color_policy);
        if let Err(e) = print_stdout(table) {
            log::error!("Cannot print table to stdout: {:?}", e);
        }
    }

    fn print_job_shared_task_description(
        &self,
        rows: &mut Vec<Vec<CellStruct>>,
        task_desc: &TaskDescription,
    ) {
        let TaskDescription {
            program,
            resources,
            pin,
            time_limit,
            priority,
        } = task_desc;

        let resources = format_resource_request(resources);
        rows.push(vec![
            "Resources".cell().bold(true),
            if *pin {
                format!("{} [pin]", resources)
            } else {
                resources
            }
            .cell(),
        ]);

        rows.push(vec!["Priority".cell().bold(true), priority.cell()]);

        rows.push(vec![
            "Command".cell().bold(true),
            program
                .args
                .iter()
                .map(|x| textwrap::fill(&x.to_string(), TERMINAL_WIDTH))
                .collect::<Vec<String>>()
                .join("\n")
                .cell(),
        ]);
        rows.push(vec![
            "Stdout".cell().bold(true),
            stdio_to_str(&program.stdout).cell(),
        ]);
        rows.push(vec![
            "Stderr".cell().bold(true),
            stdio_to_str(&program.stderr).cell(),
        ]);
        let mut env_vars: Vec<(_, _)> = program.env.iter().filter(|(k, _)| !is_hq_env(k)).collect();
        env_vars.sort_by_key(|item| item.0);
        rows.push(vec![
            "Environment".cell().bold(true),
            env_vars
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("\n")
                .cell(),
        ]);

        rows.push(vec![
            "Working directory".cell().bold(true),
            program.cwd.display().to_string().cell(),
        ]);

        rows.push(vec![
            "Task time limit".cell().bold(true),
            time_limit
                .map(|duration| humantime::format_duration(duration).to_string())
                .unwrap_or_else(|| "None".to_string())
                .cell(),
        ]);
    }

    fn print_task_summary(&self, tasks: Vec<JobTaskInfo>, info: JobInfo, worker_map: &WorkerMap) {
        const SHOWN_TASKS: usize = 5;

        let fail_rows: Vec<_> = tasks
            .iter()
            .filter_map(|t: &JobTaskInfo| match &t.state {
                JobTaskState::Failed {
                    started_data: StartedTaskData { worker_id, .. },
                    error,
                    ..
                } => Some(vec![
                    t.task_id.cell(),
                    format_worker(*worker_id, worker_map).cell(),
                    error.to_owned().cell().foreground_color(Some(Color::Red)),
                ]),
                _ => None,
            })
            .take(SHOWN_TASKS)
            .collect();

        if !fail_rows.is_empty() {
            let count = fail_rows.len() as JobTaskCount;
            let header = vec![
                "Task ID".cell().bold(true),
                "Worker".cell().bold(true),
                "Error".cell().bold(true),
            ];
            self.print_horizontal_table(fail_rows, header);

            let counters = info.counters;
            if count < counters.n_failed_tasks {
                log::warn!(
                    "{} tasks failed. ({} shown)",
                    counters.n_failed_tasks,
                    count
                );
            } else {
                log::warn!("{} tasks failed.", counters.n_failed_tasks);
            }
        }
    }
}

impl Output for CliOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        let rows: Vec<_> = workers
            .into_iter()
            .map(|worker| {
                let manager_info = worker.configuration.get_manager_info();
                vec![
                    worker.id.cell().justify(Justify::Right),
                    match worker.ended.as_ref() {
                        None => "RUNNING".cell().foreground_color(Some(Color::Green)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReason::ConnectionLost,
                            ..
                        }) => "CONNECTION LOST".cell().foreground_color(Some(Color::Red)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReason::HeartbeatLost,
                            ..
                        }) => "HEARTBEAT LOST".cell().foreground_color(Some(Color::Red)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReason::IdleTimeout,
                            ..
                        }) => "IDLE TIMEOUT".cell().foreground_color(Some(Color::Cyan)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReason::Stopped,
                            ..
                        }) => "STOPPED".cell().foreground_color(Some(Color::Magenta)),
                    },
                    worker.configuration.hostname.cell(),
                    worker.configuration.resources.summary(false).cell(),
                    manager_info
                        .as_ref()
                        .map(|info| info.manager.to_string())
                        .unwrap_or_else(|| "None".to_string())
                        .cell(),
                    manager_info
                        .as_ref()
                        .map(|info| info.job_id.as_str())
                        .unwrap_or("N/A")
                        .to_string()
                        .cell(),
                ]
            })
            .collect();

        let header = vec![
            "ID".cell(),
            "State".cell().bold(true),
            "Hostname".cell().bold(true),
            "Resources".cell().bold(true),
            "Manager".cell().bold(true),
            "Manager Job ID".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_worker_info(&self, worker_info: WorkerInfo) {
        let WorkerInfo {
            id,
            configuration,
            ended: _ended,
        } = worker_info;

        let manager_info = configuration.get_manager_info();
        let rows = vec![
            vec!["Worker ID".cell().bold(true), id.cell()],
            vec!["Hostname".cell().bold(true), configuration.hostname.cell()],
            vec![
                "Data provider".cell().bold(true),
                configuration.listen_address.cell(),
            ],
            vec![
                "Working directory".cell().bold(true),
                configuration.work_dir.display().cell(),
            ],
            vec![
                "Logging directory".cell().bold(true),
                configuration.log_dir.display().cell(),
            ],
            vec![
                "Heartbeat".cell().bold(true),
                format_duration(configuration.heartbeat_interval)
                    .to_string()
                    .cell(),
            ],
            vec![
                "Idle timeout".cell().bold(true),
                configuration
                    .idle_timeout
                    .map(|x| format_duration(x).to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .cell(),
            ],
            vec![
                "Resources".cell().bold(true),
                configuration.resources.summary(true).cell(),
            ],
            vec![
                "Time Limit".cell().bold(true),
                configuration
                    .time_limit
                    .map(|x| format_duration(x).to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .cell(),
            ],
            vec![
                "Process pid".cell().bold(true),
                configuration
                    .extra
                    .get(WORKER_EXTRA_PROCESS_PID)
                    .cloned()
                    .unwrap_or_else(|| "N/A".to_string())
                    .cell(),
            ],
            vec![
                "Manager".cell().bold(true),
                manager_info
                    .as_ref()
                    .map(|info| info.manager.to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .cell(),
            ],
            vec![
                "Manager Job ID".cell().bold(true),
                manager_info
                    .as_ref()
                    .map(|info| info.job_id.as_str())
                    .unwrap_or("N/A")
                    .cell(),
            ],
        ];
        self.print_vertical_table(rows);
    }

    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord) {
        let rows = vec![
            vec![
                "Server directory".cell().bold(true),
                server_dir.display().cell(),
            ],
            vec!["Host".cell().bold(true), record.host().cell()],
            vec!["Pid".cell().bold(true), record.pid().cell()],
            vec!["HQ port".cell().bold(true), record.server_port().cell()],
            vec![
                "Workers port".cell().bold(true),
                record.worker_port().cell(),
            ],
            vec![
                "Start date".cell().bold(true),
                record.start_date().format("%F %T %Z").cell(),
            ],
            vec!["Version".cell().bold(true), record.version().cell()],
        ];
        self.print_vertical_table(rows);
    }

    fn print_server_stats(&self, stats: StatsResponse) {
        let rows = vec![
            vec![
                "Stream connections".cell().bold(true),
                stats.stream_stats.connections.join("\n").cell(),
            ],
            vec![
                "Stream registrations".cell().bold(true),
                stats
                    .stream_stats
                    .registrations
                    .iter()
                    .map(|(job_id, path)| format!("{}: {}", job_id, path.display()))
                    .collect::<Vec<_>>()
                    .join("\n")
                    .cell(),
            ],
            vec![
                "Open files".cell().bold(true),
                stats.stream_stats.files.join("\n").cell(),
            ],
        ];
        self.print_vertical_table(rows);
    }

    fn print_job_submitted(&self, job: JobDetail) {
        println!(
            "Job submitted {}, job ID: {}",
            "successfully".color(colored::Color::Green),
            job.info.id
        );
    }

    fn print_job_list(&self, jobs: Vec<JobInfo>, total_jobs: usize) {
        let job_count = jobs.len();
        let rows: Vec<_> = jobs
            .into_iter()
            .map(|t| {
                let status = task_status_to_cell(job_status(&t));
                vec![
                    t.id.cell().justify(Justify::Right),
                    t.name.cell(),
                    status,
                    t.n_tasks.cell(),
                ]
            })
            .collect();

        let header = vec![
            "ID".cell().bold(true),
            "Name".cell().bold(true),
            "State".cell().bold(true),
            "Tasks".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);

        if job_count != total_jobs {
            println!(
                "There {} {total_jobs} {} in total. Use `--all` to display all jobs.",
                select_plural("is", "are", total_jobs),
                pluralize("job", total_jobs)
            );
        }
    }

    fn print_job_detail(&self, job: JobDetail, worker_map: WorkerMap) {
        let JobDetail {
            info,
            job_desc,
            mut tasks,
            max_fails: _,
            submission_date,
            completion_date_or_now,
            submit_dir,
        } = job;

        let mut rows = vec![
            vec!["ID".cell().bold(true), info.id.cell()],
            vec!["Name".cell().bold(true), info.name.as_str().cell()],
        ];

        let status = if info.n_tasks == 1 {
            task_status_to_cell(job_status(&info))
        } else {
            job_status_to_cell(&info).cell()
        };

        let state_label = "State".cell().bold(true);
        rows.push(vec![state_label, status]);

        let mut n_tasks = info.n_tasks.to_string();
        match &job_desc {
            JobDescription::Array { ids, .. } => {
                n_tasks.push_str(&format!("; Ids: {}", ids));
            }
            JobDescription::Graph { .. } => {
                // TODO
            }
        }

        rows.push(vec!["Tasks".cell().bold(true), n_tasks.cell()]);
        rows.push(vec![
            "Workers".cell().bold(true),
            format_job_workers(&tasks, &worker_map).cell(),
        ]);

        if let JobDescription::Array { task_desc, .. } = &job_desc {
            self.print_job_shared_task_description(&mut rows, task_desc);
        }

        rows.push(vec![
            "Submission date".cell().bold(true),
            submission_date.round_subsecs(0).cell(),
        ]);

        rows.push(vec![
            "Submission directory".cell().bold(true),
            submit_dir.to_str().unwrap().cell(),
        ]);

        rows.push(vec![
            "Makespan".cell().bold(true),
            human_duration(completion_date_or_now - submission_date).cell(),
        ]);
        self.print_vertical_table(rows);

        tasks.sort_unstable_by_key(|t| t.task_id);
        self.print_task_summary(tasks, info, &worker_map);
    }

    fn print_job_tasks(&self, job: JobDetail, worker_map: WorkerMap) {
        let task_to_paths = resolve_task_paths(&job);

        let mut tasks = job.tasks;
        tasks.sort_unstable_by_key(|t| t.task_id);

        let rows: Vec<_> = tasks
            .iter()
            .map(|task| {
                let (start, end) = get_task_time(&task.state);
                let (cwd, stdout, stderr) = format_task_paths(&task_to_paths, task);

                vec![
                    task.task_id.cell().justify(Justify::Right),
                    task_status_to_cell(get_task_status(&task.state)),
                    match task.state.get_worker() {
                        Some(worker) => format_worker(worker, &worker_map),
                        _ => "",
                    }
                    .cell(),
                    multiline_cell(vec![
                        (
                            "Start",
                            start
                                .map(|x| format_time(x).to_string())
                                .unwrap_or_else(|| "".to_string()),
                        ),
                        (
                            "End",
                            end.map(|x| format_time(x).to_string())
                                .unwrap_or_else(|| "".to_string()),
                        ),
                        ("Makespan", format_task_duration(start, end)),
                    ]),
                    multiline_cell(vec![
                        ("Workdir", cwd),
                        ("Stdout", stdout),
                        ("Stderr", stderr),
                    ]),
                    match &task.state {
                        JobTaskState::Failed { error, .. } => {
                            error.to_owned().cell().foreground_color(Some(Color::Red))
                        }
                        _ => "".cell(),
                    },
                ]
            })
            .collect();
        let header = vec![
            "ID".cell().bold(true),
            "State".cell().bold(true),
            "Worker".cell().bold(true),
            "Times".cell().bold(true),
            "Paths".cell().bold(true),
            "Error".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_job_wait(&self, duration: Duration, response: &WaitForJobsResponse) {
        let mut msgs = vec![];

        let mut format = |count: u32, action: &str, color| {
            if count > 0 {
                let job = pluralize("job", count as usize);
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
        format(response.invalid, "invalid", TASK_COLOR_INVALID);

        log::info!(
            "Wait finished in {}: {}",
            humantime::format_duration(duration),
            msgs.join(", ")
        );
    }

    fn print_job_output(
        &self,
        tasks: Vec<JobTaskInfo>,
        output_stream: OutputStream,
        task_header: bool,
        task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()> {
        print_job_output(tasks, output_stream, task_header, task_paths)
    }

    fn print_summary(&self, filename: &Path, summary: Summary) {
        let rows = vec![
            vec!["Filename".cell().bold(true), filename.display().cell()],
            vec![
                "Tasks".cell().bold(true),
                summary.n_tasks.to_string().cell(),
            ],
            vec![
                "Opened streams".cell().bold(true),
                summary.n_opened.to_string().cell(),
            ],
            vec![
                "Stdout/stderr size".cell().bold(true),
                format!(
                    "{} / {}",
                    human_size(summary.stdout_size),
                    human_size(summary.stderr_size)
                )
                .cell(),
            ],
            vec![
                "Superseded streams".cell().bold(true),
                summary.n_superseded.to_string().cell(),
            ],
            vec![
                "Superseded stdout/stderr size".cell().bold(true),
                format!(
                    "{} / {}",
                    human_size(summary.superseded_stdout_size),
                    human_size(summary.superseded_stderr_size)
                )
                .cell(),
            ],
        ];
        self.print_vertical_table(rows);
    }

    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        let mut descriptors: Vec<_> = info.descriptors.into_iter().collect();
        descriptors.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let rows: Vec<_> = descriptors
            .into_iter()
            .map(|(id, data)| {
                vec![
                    id.cell(),
                    data.info.backlog().cell(),
                    data.info.workers_per_alloc().cell(),
                    humantime::format_duration(data.info.timelimit())
                        .to_string()
                        .cell(),
                    data.manager_type.cell(),
                    data.name.unwrap_or_else(|| "".to_string()).cell(),
                    data.info.additional_args().join(",").cell(),
                ]
            })
            .collect();

        let header = vec![
            "ID".cell().bold(true),
            "Backlog size".cell().bold(true),
            "Workers per alloc".cell().bold(true),
            "Timelimit".cell().bold(true),
            "Manager".cell().bold(true),
            "Name".cell().bold(true),
            "Args".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_event_log(&self, events: Vec<AllocationEventHolder>) {
        let event_name = |event: &AllocationEventHolder| -> CellStruct {
            match event.event {
                AllocationEvent::AllocationQueued(..) => "Allocation queued"
                    .cell()
                    .foreground_color(Some(Color::Yellow)),
                AllocationEvent::AllocationStarted(..) => "Allocation started"
                    .cell()
                    .foreground_color(Some(Color::Green)),
                AllocationEvent::AllocationFinished(..) => "Allocation finished"
                    .cell()
                    .foreground_color(Some(Color::Blue)),
                AllocationEvent::AllocationFailed(..) => "Allocation failed"
                    .cell()
                    .foreground_color(Some(Color::Red)),
                AllocationEvent::AllocationDisappeared(..) => "Allocation disappeared"
                    .cell()
                    .foreground_color(Some(Color::Red)),
                AllocationEvent::QueueFail { .. } => "Allocation submission failed"
                    .cell()
                    .foreground_color(Some(Color::Red)),
                AllocationEvent::StatusFail { .. } => "Allocation status check failed"
                    .cell()
                    .foreground_color(Some(Color::Red)),
            }
        };
        let event_message = |event: &AllocationEventHolder| -> CellStruct {
            match &event.event {
                AllocationEvent::AllocationQueued(id)
                | AllocationEvent::AllocationStarted(id)
                | AllocationEvent::AllocationFailed(id)
                | AllocationEvent::AllocationFinished(id)
                | AllocationEvent::AllocationDisappeared(id) => id.cell(),
                AllocationEvent::QueueFail { error } | AllocationEvent::StatusFail { error } => {
                    error.cell()
                }
            }
        };

        let rows: Vec<_> = events
            .into_iter()
            .map(|event| {
                vec![
                    event_name(&event),
                    format_systemtime(event.date).cell(),
                    event_message(&event),
                ]
            })
            .collect();

        let header = vec![
            "Event".cell().bold(true),
            "Time".cell().bold(true),
            "Message".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_allocations(&self, mut allocations: Vec<Allocation>) {
        let format_time = |time: Option<SystemTime>| match time {
            Some(time) => format_systemtime(time).cell(),
            None => "".cell(),
        };

        allocations.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        let rows: Vec<_> = allocations
            .into_iter()
            .map(|allocation| {
                let times = allocation_times_from_alloc(&allocation);
                vec![
                    allocation.id.cell(),
                    allocation_status_to_cell(&allocation.status),
                    allocation.working_dir.display().cell(),
                    allocation.worker_count.cell(),
                    format_time(Some(times.get_queued_at())),
                    format_time(times.get_started_at()),
                    format_time(times.get_finished_at()),
                ]
            })
            .collect();

        let header = vec![
            "ID".cell().bold(true),
            "State".cell().bold(true),
            "Working directory".cell().bold(true),
            "Worker count".cell().bold(true),
            "Queue time".cell().bold(true),
            "Start time".cell().bold(true),
            "Finish time".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_hw(&self, descriptor: &ResourceDescriptor) {
        println!("Summary: {}", descriptor.summary(true));
        println!("Cpu Ids: {}", descriptor.full_describe());
    }

    fn print_error(&self, error: Error) {
        eprintln!("{:?}", error);
    }
}

struct AllocationTimes {
    queued_at: SystemTime,
    started_at: Option<SystemTime>,
    finished_at: Option<SystemTime>,
}

impl AllocationTimes {
    fn get_queued_at(&self) -> SystemTime {
        self.queued_at
    }
    fn get_started_at(&self) -> Option<SystemTime> {
        self.started_at
    }
    fn get_finished_at(&self) -> Option<SystemTime> {
        self.finished_at
    }
}

fn multiline_cell<T: AsRef<str>>(rows: Vec<(&'static str, T)>) -> CellStruct {
    if rows.iter().all(|(_, value)| value.as_ref().is_empty()) {
        return "".cell();
    }

    rows.into_iter()
        .map(|(label, value)| format!("{label}: {}", value.as_ref()))
        .collect::<Vec<_>>()
        .join("\n")
        .cell()
}

fn stdio_to_str(stdio: &StdioDef) -> &str {
    match stdio {
        StdioDef::Null => "<None>",
        StdioDef::File(filename) => filename.to_str().unwrap(),
        StdioDef::Pipe => "<Stream>",
    }
}

/// Allocation
fn allocation_status_to_cell(status: &AllocationStatus) -> CellStruct {
    match status {
        AllocationStatus::Queued => "Queued"
            .cell()
            .foreground_color(Some(cli_table::Color::Yellow)),
        AllocationStatus::Running { .. } => "Running"
            .cell()
            .foreground_color(Some(cli_table::Color::Blue)),
        AllocationStatus::Finished { .. } => "Finished"
            .cell()
            .foreground_color(Some(cli_table::Color::Green)),
        AllocationStatus::Failed { .. } => "Failed"
            .cell()
            .foreground_color(Some(cli_table::Color::Red)),
    }
}

fn allocation_times_from_alloc(allocation: &Allocation) -> AllocationTimes {
    let mut started = None;
    let mut finished = None;

    match &allocation.status {
        AllocationStatus::Queued => {}
        AllocationStatus::Running { started_at } => {
            started = Some(started_at);
        }
        AllocationStatus::Finished {
            started_at,
            finished_at,
        }
        | AllocationStatus::Failed {
            started_at,
            finished_at,
        } => {
            started = Some(started_at);
            finished = Some(finished_at);
        }
    }
    AllocationTimes {
        queued_at: allocation.queued_at,
        started_at: started.cloned(),
        finished_at: finished.cloned(),
    }
}

/// Jobs & Tasks
fn job_status_to_cell(info: &JobInfo) -> String {
    let row = |result: &mut String, string, value, color| {
        if value > 0 {
            let text = format!("{} ({})", string, value).color(color);
            writeln!(result, "{}", text).unwrap();
        }
    };
    let mut result = format!("{}\n", job_progress_bar(info.counters, info.n_tasks, 40));
    row(
        &mut result,
        "RUNNING",
        info.counters.n_running_tasks,
        colored::Color::Yellow,
    );
    row(
        &mut result,
        "FAILED",
        info.counters.n_failed_tasks,
        colored::Color::Red,
    );
    row(
        &mut result,
        "FINISHED",
        info.counters.n_finished_tasks,
        colored::Color::Green,
    );
    row(
        &mut result,
        "CANCELED",
        info.counters.n_canceled_tasks,
        colored::Color::Magenta,
    );
    row(
        &mut result,
        "WAITING",
        info.counters.n_waiting_tasks(info.n_tasks),
        colored::Color::Cyan,
    );
    result
}

pub(crate) fn job_progress_bar(
    counters: JobTaskCounters,
    n_tasks: JobTaskCount,
    width: usize,
) -> String {
    let mut buffer = String::from("[");

    let parts = vec![
        (counters.n_canceled_tasks, TASK_COLOR_CANCELED),
        (counters.n_failed_tasks, TASK_COLOR_FAILED),
        (counters.n_finished_tasks, TASK_COLOR_FINISHED),
        (counters.n_running_tasks, TASK_COLOR_RUNNING),
    ];

    let chars = |count: JobTaskCount| {
        let ratio = count as f64 / n_tasks as f64;
        (ratio * width as f64).ceil() as usize
    };

    let mut total_char_count: usize = 0;
    for (count, color) in parts {
        let char_count = std::cmp::min(width - total_char_count, chars(count));
        write!(buffer, "{}", "#".repeat(char_count).color(color)).unwrap();
        total_char_count += char_count;
    }
    write!(
        buffer,
        "{}",
        ".".repeat(width.saturating_sub(total_char_count))
    )
    .unwrap();

    buffer.push(']');
    buffer
}

pub fn print_job_output(
    tasks: Vec<JobTaskInfo>,
    output_stream: OutputStream,
    task_header: bool,
    task_paths: TaskToPathsMap,
) -> anyhow::Result<()> {
    let read_stream = |task_info: &JobTaskInfo, output_stream: &OutputStream| {
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();

        let (_, stdout_path, stderr_path) = get_task_paths(&task_paths, task_info);
        let (opt_path, stream_name) = match output_stream {
            OutputStream::Stdout => (stdout_path, "stdout"),
            OutputStream::Stderr => (stderr_path, "stderr"),
        };

        if task_header {
            writeln!(stdout, "# Task {}", task_info.task_id).expect("Could not write output");
        }

        if let Some(path) = opt_path {
            match File::open(path) {
                Ok(mut file) => {
                    let copy = std::io::copy(&mut file, &mut stdout);
                    if let Err(error) = copy {
                        log::warn!("Could not output contents of `{path}`: {error:?}");
                    }
                }
                Err(error) => log::warn!("File `{path}` cannot be opened: {error:?}"),
            };
        } else {
            log::warn!(
                "Task {} has no `{stream_name}` stream associated with it",
                task_info.task_id
            );
        }
    };

    for task in tasks {
        read_stream(&task, &output_stream);
    }

    Ok(())
}

fn task_status_to_cell(status: Status) -> CellStruct {
    match status {
        Status::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        Status::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        Status::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        Status::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        Status::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
    }
}

fn format_resource_request(rq: &ResourceRequest) -> String {
    let mut result = format_cpu_request(&rq.cpus);
    for grq in &rq.generic {
        result.push_str(&format!("\n{}: {}", grq.resource, grq.amount))
    }
    result
}

fn format_cpu_request(cr: &CpuRequest) -> String {
    match cr {
        CpuRequest::Compact(n_cpus) => {
            format!("cpus: {} compact", *n_cpus)
        }
        CpuRequest::ForceCompact(n_cpus) => {
            format!("cpus: {} compact!", *n_cpus)
        }
        CpuRequest::Scatter(n_cpus) => {
            format!("cpus: {} scatter", *n_cpus)
        }
        CpuRequest::All => "cpus: all".to_string(),
    }
}

/// Formatting
pub fn format_job_workers(tasks: &[JobTaskInfo], worker_map: &WorkerMap) -> String {
    // BTreeSet is used to both filter duplicates and keep a stable order
    let worker_set: BTreeSet<_> = tasks
        .iter()
        .filter_map(|task| task.state.get_worker())
        .collect();
    let worker_count = worker_set.len();

    let mut result = worker_set
        .into_iter()
        .take(MAX_DISPLAYED_WORKERS)
        .map(|id| format_worker(id, worker_map))
        .collect::<Vec<_>>()
        .join(", ");

    if worker_count > MAX_DISPLAYED_WORKERS {
        write!(result, ", … ({} total)", worker_count).unwrap();
    }

    result
}

fn format_worker(id: WorkerId, worker_map: &WorkerMap) -> &str {
    worker_map
        .get(&id)
        .map(|s| s.as_str())
        .unwrap_or_else(|| "N/A")
}

fn get_task_time(state: &JobTaskState) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    match state {
        JobTaskState::Canceled {
            started_data: Some(started_data),
        } => (Some(started_data.start_date), None),
        JobTaskState::Running { started_data, .. } => (Some(started_data.start_date), None),
        JobTaskState::Finished {
            started_data,
            end_date,
            ..
        }
        | JobTaskState::Failed {
            started_data,
            end_date,
            ..
        } => (Some(started_data.start_date), Some(*end_date)),
        JobTaskState::Canceled { started_data: None } | JobTaskState::Waiting => (None, None),
    }
}

/// Returns Option(working directory, stdout, stderr)
fn get_task_paths<'a>(
    task_map: &'a TaskToPathsMap,
    state: &JobTaskInfo,
) -> (Option<&'a str>, Option<&'a str>, Option<&'a str>) {
    match task_map[&state.task_id] {
        Some(ref paths) => (
            Some(paths.cwd.to_str().unwrap()),
            match &paths.stdout {
                StdioDef::File(filename) => Some(filename.to_str().unwrap()),
                _ => None,
            },
            match &paths.stderr {
                StdioDef::File(filename) => Some(filename.to_str().unwrap()),
                _ => None,
            },
        ),
        None => (None, None, None),
    }
}

/// Returns (working directory, stdout, stderr)
fn format_task_paths<'a>(
    task_map: &'a TaskToPathsMap,
    state: &JobTaskInfo,
) -> (&'a str, &'a str, &'a str) {
    match task_map[&state.task_id] {
        Some(ref paths) => (
            paths.cwd.to_str().unwrap(),
            stdio_to_str(&paths.stdout),
            stdio_to_str(&paths.stderr),
        ),
        None => ("", "", ""),
    }
}

fn format_task_duration(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> String {
    match (start, end) {
        (Some(start), None) => human_duration(Utc::now() - start),
        (Some(start), Some(end)) => human_duration(end - start),
        _ => "".to_string(),
    }
}

fn format_systemtime(time: SystemTime) -> impl Display {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%d.%m.%Y %H:%M:%S")
}

fn format_time(time: DateTime<Utc>) -> impl Display {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%d.%m.%Y %H:%M:%S")
}
