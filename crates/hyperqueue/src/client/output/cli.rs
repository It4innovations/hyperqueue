use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, CellStruct, Color, ColorChoice, Style, Table, TableStruct};

use std::fmt::{Display, Write};

use crate::client::job::WorkerMap;
use crate::client::output::outputs::{Output, MAX_DISPLAYED_WORKERS};
use crate::client::resources::resource_request_to_string;
use crate::client::status::{job_status, task_status, Status};
use crate::common::env::is_hq_env;
use crate::common::format::{human_duration, human_size};
use crate::common::manager::info::GetManagerInfo;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{
    Allocation, AllocationEvent, AllocationEventHolder, AllocationStatus,
};
use crate::server::job::{JobTaskCounters, JobTaskInfo, JobTaskState};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, JobType, LostWorkerReasonInfo, StatsResponse,
    WaitForJobsResponse, WorkerExitInfo, WorkerInfo,
};
use crate::{JobTaskCount, WorkerId};

use chrono::{DateTime, Local, SubsecRound, Utc};
use core::time::Duration;
use humantime::format_duration;

use std::path::Path;
use std::time::SystemTime;

use tako::common::resources::ResourceDescriptor;
use tako::messages::common::{StdioDef, WorkerConfiguration};

use crate::common::strutils::pluralize;
use crate::worker::start::WORKER_EXTRA_PROCESS_PID;
use colored::Color as Colorization;
use colored::Colorize;
use std::collections::BTreeSet;

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

    fn print_rows(&self, rows: Vec<Vec<CellStruct>>) {
        self.print_table(rows.table())
    }

    fn print_table(&self, table: TableStruct) {
        let table = table.color_choice(self.color_policy);
        if let Err(e) = print_stdout(table) {
            log::error!("Cannot print table to stdout: {:?}", e);
        }
    }
}

impl Output for CliOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        let rows: Vec<_> = workers
            .into_iter()
            .map(|worker| {
                let manager_info = worker.configuration.get_manager_info();
                [
                    worker.id.cell().justify(Justify::Right),
                    match worker.ended.as_ref() {
                        None => "RUNNING".cell().foreground_color(Some(Color::Green)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReasonInfo::ConnectionLost,
                            ..
                        }) => "CONNECTION LOST".cell().foreground_color(Some(Color::Red)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReasonInfo::HeartbeatLost,
                            ..
                        }) => "HEARTBEAT LOST".cell().foreground_color(Some(Color::Red)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReasonInfo::IdleTimeout,
                            ..
                        }) => "IDLE TIMEOUT".cell().foreground_color(Some(Color::Cyan)),
                        Some(WorkerExitInfo {
                            reason: LostWorkerReasonInfo::Stopped,
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

        let table = rows.table().title(vec![
            "Id".cell(),
            "State".cell().bold(true),
            "Hostname".cell().bold(true),
            "Resources".cell().bold(true),
            "Manager".cell().bold(true),
            "Manager Job Id".cell().bold(true),
        ]);
        self.print_table(table);
    }

    fn print_worker_info(&self, worker_id: WorkerId, configuration: WorkerConfiguration) {
        let manager_info = configuration.get_manager_info();
        let rows = vec![
            vec!["Worker ID".cell().bold(true), worker_id.cell()],
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
                "Manager Job Id".cell().bold(true),
                manager_info
                    .as_ref()
                    .map(|info| info.job_id.as_str())
                    .unwrap_or("N/A")
                    .cell(),
            ],
        ];
        self.print_rows(rows);
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
        self.print_rows(rows);
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
        self.print_rows(rows);
    }

    fn print_job_list(&self, tasks: Vec<JobInfo>) {
        let rows: Vec<_> = tasks
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

        let table = rows.table().title(vec![
            "Id".cell().bold(true),
            "Name".cell().bold(true),
            "State".cell().bold(true),
            "Tasks".cell().bold(true),
        ]);
        self.print_table(table);
    }

    fn print_job_detail(
        &self,
        job: JobDetail,
        just_submitted: bool,
        show_tasks: bool,
        worker_map: WorkerMap,
        resource_names: &[String],
    ) {
        let mut rows = vec![
            vec!["Id".cell().bold(true), job.info.id.cell()],
            vec!["Name".cell().bold(true), job.info.name.as_str().cell()],
        ];

        let status = if just_submitted {
            "SUBMITTED".cell().foreground_color(Some(Color::Cyan))
        } else if job.info.n_tasks == 1 {
            task_status_to_cell(job_status(&job.info))
        } else {
            job_status_to_cell(&job.info).cell()
        };

        let state_label = "State".cell().bold(true);
        rows.push(vec![state_label, status]);

        let mut n_tasks = job.info.n_tasks.to_string();
        if let JobType::Array(array_def) = &job.job_type {
            n_tasks.push_str(&format!("; Ids: {}", array_def));
        }

        rows.push(vec!["Tasks".cell().bold(true), n_tasks.cell()]);
        rows.push(vec![
            "Workers".cell().bold(true),
            format_job_workers(&job, &worker_map).cell(),
        ]);

        let resources = resource_request_to_string(&job.resources, resource_names);

        rows.push(vec![
            "Resources".cell().bold(true),
            if job.pin {
                format!("{} [pin]", resources)
            } else {
                resources
            }
            .cell(),
        ]);

        rows.push(vec!["Priority".cell().bold(true), job.priority.cell()]);

        let program_def = job.program_def;
        rows.push(vec![
            "Command".cell().bold(true),
            program_def
                .args
                .iter()
                .map(|x| textwrap::fill(&x.to_string(), TERMINAL_WIDTH))
                .collect::<Vec<String>>()
                .join("\n")
                .cell(),
        ]);
        rows.push(vec![
            "Stdout".cell().bold(true),
            stdio_to_cell(&program_def.stdout),
        ]);
        rows.push(vec![
            "Stderr".cell().bold(true),
            stdio_to_cell(&program_def.stderr),
        ]);
        let mut env_vars: Vec<(_, _)> = program_def
            .env
            .iter()
            .filter(|(k, _)| !is_hq_env(k))
            .collect();
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
            "Working Dir".cell().bold(true),
            program_def
                .cwd
                .map(|cwd| cwd.display().to_string())
                .unwrap()
                .cell(),
        ]);

        rows.push(vec![
            "Submission date".cell().bold(true),
            job.submission_date.round_subsecs(0).cell(),
        ]);

        rows.push(vec![
            "Task time limit".cell().bold(true),
            job.time_limit
                .map(|duration| humantime::format_duration(duration).to_string())
                .unwrap_or_else(|| "None".to_string())
                .cell(),
        ]);

        rows.push(vec![
            "Makespan".cell().bold(true),
            human_duration(job.completion_date_or_now - job.submission_date).cell(),
        ]);
        self.print_rows(rows);

        if !job.tasks.is_empty() {
            self.print_job_tasks(
                job.completion_date_or_now,
                job.tasks,
                show_tasks,
                &job.info.counters,
                &worker_map,
            );
        }
    }

    fn print_job_tasks(
        &self,
        completion_date_or_now: DateTime<Utc>,
        mut tasks: Vec<JobTaskInfo>,
        show_tasks: bool,
        counters: &JobTaskCounters,
        worker_map: &WorkerMap,
    ) {
        tasks.sort_unstable_by_key(|t| t.task_id);
        let make_error_row = |t: &JobTaskInfo| match &t.state {
            JobTaskState::Failed { worker, error, .. } => Some(vec![
                t.task_id.cell(),
                format_worker(*worker, worker_map).cell(),
                error.to_owned().cell().foreground_color(Some(Color::Red)),
            ]),
            _ => None,
        };

        if show_tasks {
            let rows: Vec<_> = tasks
                .iter()
                .map(|t| {
                    vec![
                        t.task_id.cell(),
                        task_status_to_cell(task_status(&t.state)),
                        match t.state.get_worker() {
                            Some(worker) => format_worker(worker, worker_map),
                            _ => "",
                        }
                        .cell(),
                        format_task_duration(&completion_date_or_now, &t.state).cell(),
                        match &t.state {
                            JobTaskState::Failed { error, .. } => {
                                error.to_owned().cell().foreground_color(Some(Color::Red))
                            }
                            _ => "".cell(),
                        },
                    ]
                })
                .collect();
            let table = rows.table().title(vec![
                "Task Id".cell().bold(true),
                "State".cell().bold(true),
                "Worker".cell().bold(true),
                "Time".cell().bold(true),
                "Message".cell().bold(true),
            ]);
            self.print_table(table);
        } else {
            const SHOWN_TASKS: usize = 5;
            let fail_rows: Vec<_> = tasks
                .iter()
                .filter_map(make_error_row)
                .take(SHOWN_TASKS)
                .collect();

            if !fail_rows.is_empty() {
                let count = fail_rows.len() as JobTaskCount;
                let table = fail_rows.table().title(vec![
                    "Task Id".cell().bold(true),
                    "Worker".cell().bold(true),
                    "Error".cell().bold(true),
                ]);
                self.print_table(table);

                if count < counters.n_failed_tasks {
                    println!(
                        "{} tasks failed. ({} shown)",
                        counters.n_failed_tasks, count
                    );
                } else {
                    println!("{} tasks failed.", counters.n_failed_tasks);
                }
            }
        }
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
        self.print_rows(rows);
    }

    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        let mut rows = vec![vec![
            "ID".cell().bold(true),
            "Backlog size".cell().bold(true),
            "Workers per alloc".cell().bold(true),
            "Timelimit".cell().bold(true),
            "Manager".cell().bold(true),
            "Name".cell().bold(true),
            "Args".cell().bold(true),
        ]];

        let mut descriptors: Vec<_> = info.descriptors.into_iter().collect();
        descriptors.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        rows.extend(descriptors.into_iter().map(|(id, data)| {
            vec![
                id.cell(),
                data.info.backlog().cell(),
                data.info.workers_per_alloc().cell(),
                data.info
                    .timelimit()
                    .map(|d| humantime::format_duration(d).to_string())
                    .unwrap_or_else(|| "N/A".to_string())
                    .cell(),
                data.manager_type.cell(),
                data.name.unwrap_or_else(|| "".to_string()).cell(),
                data.info.additional_args().join(",").cell(),
            ]
        }));
        self.print_rows(rows);
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

        let mut rows = vec![vec![
            "Event".cell().bold(true),
            "Time".cell().bold(true),
            "Message".cell().bold(true),
        ]];
        rows.extend(events.into_iter().map(|event| {
            vec![
                event_name(&event),
                format_time(event.date).cell(),
                event_message(&event),
            ]
        }));
        self.print_rows(rows);
    }

    fn print_allocations(&self, mut allocations: Vec<Allocation>) {
        let mut rows = vec![vec![
            "Id".cell().bold(true),
            "State".cell().bold(true),
            "Working directory".cell().bold(true),
            "Worker count".cell().bold(true),
            "Queue time".cell().bold(true),
            "Start time".cell().bold(true),
            "Finish time".cell().bold(true),
        ]];

        let format_time = |time: Option<SystemTime>| match time {
            Some(time) => format_time(time).cell(),
            None => "".cell(),
        };

        allocations.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        rows.extend(allocations.into_iter().map(|allocation| {
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
        }));
        self.print_rows(rows);
    }

    fn print_hw(&self, descriptor: &ResourceDescriptor) {
        println!("Summary: {}", descriptor.summary(true));
        println!("Cpu Ids: {}", descriptor.full_describe());
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

fn stdio_to_cell(stdio: &StdioDef) -> CellStruct {
    match stdio {
        StdioDef::Null => "<None>".cell(),
        StdioDef::File(filename) => filename.display().cell(),
        StdioDef::Pipe => "<Stream>".cell(),
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

fn task_status_to_cell(status: Status) -> CellStruct {
    match status {
        Status::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        Status::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        Status::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        Status::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        Status::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
    }
}

/// Formatting
fn format_job_workers(job: &JobDetail, worker_map: &WorkerMap) -> String {
    // BTreeSet is used to both filter duplicates and keep a stable order
    let worker_set: BTreeSet<_> = job
        .tasks
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

fn format_task_duration(
    completion_date_or_now: &chrono::DateTime<chrono::Utc>,
    state: &JobTaskState,
) -> String {
    let duration = match state {
        JobTaskState::Canceled | JobTaskState::Waiting => return "".to_string(),
        JobTaskState::Running { start_date, .. } => *completion_date_or_now - *start_date,
        JobTaskState::Finished {
            start_date,
            end_date,
            ..
        }
        | JobTaskState::Failed {
            start_date,
            end_date,
            ..
        } => *end_date - *start_date,
    };
    human_duration(duration)
}

fn format_time(time: SystemTime) -> impl Display {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%d.%m.%Y %H:%M:%S")
}
