use cli_table::format::{Justify, Separator};
use cli_table::{Cell, CellStruct, Color, ColorChoice, Style, Table, TableStruct, print_stdout};
use itertools::Itertools;

use std::fmt::{Display, Write};

use crate::client::job::WorkerMap;
use crate::client::output::outputs::{MAX_DISPLAYED_WORKERS, Output, OutputStream};
use crate::client::status::{Status, get_task_status, job_status};
use crate::common::env::is_hq_env;
use crate::common::format::{human_duration, human_mem_amount, human_size};
use crate::common::manager::info::GetManagerInfo;
use crate::server::autoalloc::{Allocation, AllocationState};
use crate::server::job::{JobTaskCounters, JobTaskInfo, JobTaskState};
use crate::stream::reader::outputlog::Summary;
use crate::transfer::messages::{
    AutoAllocListQueuesResponse, JobDetail, JobInfo, JobTaskDescription, PinMode, QueueData,
    QueueState, ServerInfo, TaskDescription, TaskKind, TaskKindProgram, WaitForJobsResponse,
    WorkerExitInfo, WorkerInfo,
};
use tako::{JobId, JobTaskCount, JobTaskId, TaskId, WorkerId};

use chrono::{DateTime, Local, SubsecRound, Utc};
use core::time::Duration;
use humantime::format_duration;
use std::borrow::Cow;

use std::path::Path;

use tako::program::StdioDef;
use tako::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind};

use crate::client::output::Verbosity;
use crate::client::output::common::{
    JOB_SUMMARY_STATUS_ORDER, TaskToPathsMap, group_jobs_by_status, resolve_task_paths,
};
use crate::client::output::json::format_datetime;
use crate::common::arraydef::IntArray;
use crate::common::utils::str::{pluralize, select_plural, truncate_middle};
use crate::common::utils::time::AbsoluteTime;
use crate::worker::start::WORKER_EXTRA_PROCESS_PID;
use anyhow::Error;
use colored::Colorize;
use colored::{Color as Colorization, ColoredString};
use std::collections::BTreeSet;
use std::fs::File;
use tako::gateway::{
    LostWorkerReason, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
    WorkerRuntimeInfo,
};
use tako::server::{TaskExplainItem, TaskExplanation};
use tako::{Map, format_comma_delimited};

pub const TASK_COLOR_CANCELED: Colorization = Colorization::Magenta;
pub const TASK_COLOR_FAILED: Colorization = Colorization::Red;
pub const TASK_COLOR_FINISHED: Colorization = Colorization::Green;
pub const TASK_COLOR_RUNNING: Colorization = Colorization::Yellow;
pub const TASK_COLOR_INVALID: Colorization = Colorization::BrightRed;

const TERMINAL_WIDTH: usize = 80;
const ERROR_TRUNCATE_LENGTH_LIST: usize = 16;
const ERROR_TRUNCATE_LENGTH_INFO: usize = 237;

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
            log::error!("Cannot print table to stdout: {e:?}");
        }
    }

    fn print_job_shared_task_description(
        &self,
        rows: &mut Vec<Vec<CellStruct>>,
        task_desc: &TaskDescription,
    ) {
        let TaskDescription {
            kind,
            resources,
            time_limit,
            priority,
            crash_limit,
        } = task_desc;

        match kind {
            TaskKind::ExternalProgram(TaskKindProgram {
                program,
                pin_mode,
                task_dir: _task_dir,
            }) => {
                let resources = format_resource_variants(resources);
                rows.push(vec![
                    "Resources".cell().bold(true),
                    if !matches!(pin_mode, PinMode::None) {
                        format!("{resources} [pin]")
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
                let mut env_vars: Vec<(_, _)> =
                    program.env.iter().filter(|(k, _)| !is_hq_env(k)).collect();
                env_vars.sort_by_key(|item| item.0);
                rows.push(vec![
                    "Environment".cell().bold(true),
                    env_vars
                        .into_iter()
                        .map(|(k, v)| format!("{k}={v}"))
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
                        .map(|duration| format_duration(duration).to_string())
                        .unwrap_or_else(|| "None".to_string())
                        .cell(),
                ]);

                rows.push(vec!["Crash limit".cell().bold(true), crash_limit.cell()]);
            }
        }
    }

    fn print_task_summary(
        &self,
        tasks: &[(JobTaskId, JobTaskInfo)],
        info: &JobInfo,
        worker_map: &WorkerMap,
    ) {
        const SHOWN_TASKS: usize = 5;

        let fail_rows: Vec<_> = tasks
            .iter()
            .filter_map(|(task_id, t): &(JobTaskId, JobTaskInfo)| match &t.state {
                JobTaskState::Failed {
                    started_data,
                    error,
                    ..
                } => {
                    let worker_ids = started_data.as_ref().map(|data| data.worker_ids.as_slice());

                    Some(vec![
                        task_id.cell(),
                        format_workers(worker_ids, worker_map).cell(),
                        error.to_owned().cell().foreground_color(Some(Color::Red)),
                    ])
                }
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

impl Output for CliOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        let rows: Vec<_> = workers
            .into_iter()
            .map(|worker| {
                let manager_info = worker.configuration.get_manager_info();
                vec![
                    worker.id.cell().justify(Justify::Right),
                    worker_status(&worker),
                    worker.configuration.hostname.cell(),
                    resources_summary(&worker.configuration.resources, false).cell(),
                    manager_info
                        .as_ref()
                        .map(|info| info.manager.to_string())
                        .unwrap_or_else(|| "None".to_string())
                        .cell(),
                    manager_info
                        .as_ref()
                        .map(|info| info.allocation_id.as_str())
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
        let state = worker_status(&worker_info);
        let WorkerInfo {
            id,
            configuration,
            started,
            ended: _ended,
            runtime_info,
            last_task_started,
        } = worker_info;

        let manager_info = configuration.get_manager_info();
        let mut rows = vec![
            vec!["Worker".cell().bold(true), id.cell()],
            vec!["State".cell().bold(true), state],
            vec!["Hostname".cell().bold(true), configuration.hostname.cell()],
            vec!["Started".cell().bold(true), format_datetime(started).cell()],
            vec![
                "Data provider".cell().bold(true),
                configuration.listen_address.cell(),
            ],
            vec![
                "Working directory".cell().bold(true),
                configuration.work_dir.display().cell(),
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
                "Overview interval".cell().bold(true),
                configuration
                    .overview_configuration
                    .send_interval
                    .map(|x| format_duration(x).to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .cell(),
            ],
            vec![
                "Resources".cell().bold(true),
                resources_summary(&configuration.resources, true).cell(),
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
            vec!["Group".cell().bold(true), configuration.group.cell()],
            vec![
                "Downloads".cell().bold(true),
                format!(
                    "{} parallel; max {} fails + {} delay",
                    configuration.max_parallel_downloads,
                    configuration.max_download_tries,
                    format_duration(configuration.wait_between_download_tries)
                )
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
                    .map(|info| info.allocation_id.as_str())
                    .unwrap_or("N/A")
                    .cell(),
            ],
            vec![
                "Last task started".cell().bold(true),
                last_task_started
                    .map(|t| format!("{}; Time: {}", t.task_id, format_datetime(t.time)).cell())
                    .unwrap_or_else(|| "".cell()),
            ],
        ];
        if let Some(runtime_info) = runtime_info {
            let mut s = String::with_capacity(60);
            match runtime_info {
                WorkerRuntimeInfo::SingleNodeTasks {
                    running_tasks,
                    assigned_tasks,
                    is_reserved,
                } => {
                    write!(s, "assigned tasks: {assigned_tasks}").unwrap();
                    if running_tasks > 0 {
                        write!(s, "; running tasks: {running_tasks}").unwrap();
                    }
                    if is_reserved {
                        write!(s, "; reserved for a multi-node task").unwrap();
                    }
                }
                WorkerRuntimeInfo::MultiNodeTask { main_node } => {
                    write!(s, "running multinode task; ").unwrap();
                    if main_node {
                        write!(s, "main node").unwrap();
                    } else {
                        write!(s, "secondary node").unwrap();
                    }
                }
            };
            rows.push(vec!["Runtime Info".cell().bold(true), s.cell()]);
        }
        self.print_vertical_table(rows);
    }

    fn print_server_info(&self, server_dir: Option<&Path>, info: &ServerInfo) {
        let ServerInfo {
            server_uid,
            client_host,
            worker_host,
            client_port,
            worker_port,
            version,
            pid,
            start_date,
            journal_path,
        } = info;

        let mut rows = vec![
            vec![
                "Server UID".cell().bold(true),
                server_uid.to_string().cell(),
            ],
            vec![
                "Client host".cell().bold(true),
                client_host.to_string().cell(),
            ],
            vec!["Client port".cell().bold(true), client_port.cell()],
            vec![
                "Worker host".cell().bold(true),
                worker_host.to_string().cell(),
            ],
            vec!["Worker port".cell().bold(true), worker_port.cell()],
            vec!["Version".cell().bold(true), version.to_string().cell()],
            vec!["Pid".cell().bold(true), pid.cell()],
            vec![
                "Start date".cell().bold(true),
                start_date.format("%F %T %Z").cell(),
            ],
            vec![
                "Journal path".cell().bold(true),
                journal_path
                    .as_ref()
                    .map(|p| format!("{}", p.display()))
                    .unwrap_or_else(|| "".to_string())
                    .cell(),
            ],
        ];
        if let Some(dir) = server_dir {
            rows.insert(
                0,
                vec!["Server directory".cell().bold(true), dir.display().cell()],
            )
        }
        self.print_vertical_table(rows);
    }

    fn print_job_submitted(&self, job: JobDetail) {
        println!(
            "Job submitted {}, job ID: {}",
            "successfully".color(colored::Color::Green),
            job.info.id
        );
    }

    fn print_task_ids(&self, job_task_ids: Vec<(JobId, IntArray)>) {
        if job_task_ids.len() == 1 {
            println!("{}", job_task_ids[0].1);
        } else {
            for (job_id, array) in &job_task_ids {
                println!("{job_id}: {array}");
            }
        }
    }

    fn print_job_list(&self, jobs: Vec<JobInfo>, total_jobs: usize) {
        let job_count = jobs.len();
        let mut has_opened = false;
        let rows: Vec<_> = jobs
            .into_iter()
            .map(|t| {
                let status = status_to_cell(&job_status(&t));
                vec![
                    if t.is_open {
                        has_opened = true;
                        format!("*{}", t.id).cell()
                    } else {
                        t.id.cell()
                    }
                    .justify(Justify::Right),
                    truncate_middle(&t.name, 50).cell(),
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

        if has_opened {
            println!("* = Open jobs")
        }

        if job_count != total_jobs {
            println!(
                "There {} {total_jobs} {} in total. Use `--all` to display all jobs.",
                select_plural("is", "are", total_jobs),
                pluralize("job", total_jobs)
            );
        }
    }

    fn print_job_summary(&self, jobs: Vec<JobInfo>) {
        let statuses = group_jobs_by_status(&jobs);

        let rows: Vec<_> = JOB_SUMMARY_STATUS_ORDER
            .iter()
            .map(|status| {
                let count = statuses.get(status).copied().unwrap_or_default();
                vec![status_to_cell(status), count.cell().justify(Justify::Right)]
            })
            .collect();

        let header = vec!["Status".cell().bold(true), "Count".cell().bold(true)];

        self.print_horizontal_table(rows, header);
    }

    fn print_job_detail(&self, jobs: Vec<JobDetail>, worker_map: WorkerMap, _server_uid: &str) {
        for job in jobs {
            let JobDetail {
                info,
                job_desc: _,
                submit_descs,
                mut tasks,
                tasks_not_found: _,
                submission_date,
                completion_date_or_now,
            } = job;

            let mut rows = vec![
                vec!["ID".cell().bold(true), info.id.cell()],
                vec!["Name".cell().bold(true), info.name.as_str().cell()],
            ];

            let status = if info.n_tasks == 1 {
                status_to_cell(&job_status(&info))
            } else {
                job_status_to_cell(&info).cell()
            };

            let state_label = "State".cell().bold(true);
            rows.push(vec![state_label, status]);

            let state_label = "Session".cell().bold(true);
            rows.push(vec![state_label, session_to_cell(info.is_open)]);

            let mut n_tasks = info.n_tasks.to_string();

            let ids = if submit_descs.len() == 1
                && matches!(
                    &submit_descs[0].description().task_desc,
                    JobTaskDescription::Array { .. }
                ) {
                match &submit_descs[0].description().task_desc {
                    JobTaskDescription::Array { ids, .. } => ids.clone(),
                    _ => unreachable!(),
                }
            } else {
                let mut ids: Vec<u32> = submit_descs
                    .iter()
                    .flat_map(|submit_desc| match &submit_desc.description().task_desc {
                        JobTaskDescription::Array { ids, .. } => {
                            itertools::Either::Left(ids.iter())
                        }
                        JobTaskDescription::Graph { tasks } => {
                            itertools::Either::Right(tasks.iter().map(|t| t.id.as_num()))
                        }
                    })
                    .collect();
                ids.sort();
                IntArray::from_sorted_ids(ids.into_iter())
            };
            if !ids.is_empty() {
                write!(n_tasks, "; Ids: {ids}").unwrap();
            }

            rows.push(vec!["Tasks".cell().bold(true), n_tasks.cell()]);
            rows.push(vec![
                "Workers".cell().bold(true),
                format_job_workers(&tasks, &worker_map).cell(),
            ]);

            if submit_descs.len() == 1 {
                if let JobTaskDescription::Array { task_desc, .. } =
                    &submit_descs[0].description().task_desc
                {
                    self.print_job_shared_task_description(&mut rows, task_desc);
                }
            }

            rows.push(vec![
                "Submission date".cell().bold(true),
                submission_date.round_subsecs(0).cell(),
            ]);

            if submit_descs.len() == 1 {
                rows.push(vec![
                    "Submission directory".cell().bold(true),
                    submit_descs[0]
                        .description()
                        .submit_dir
                        .to_str()
                        .unwrap()
                        .cell(),
                ]);
            }

            rows.push(vec![
                "Makespan".cell().bold(true),
                human_duration(completion_date_or_now - submission_date).cell(),
            ]);
            self.print_vertical_table(rows);

            tasks.sort_unstable_by_key(|t| t.0);
            self.print_task_summary(&tasks, &info, &worker_map);
        }
    }

    fn print_job_wait(
        &self,
        duration: Duration,
        response: &WaitForJobsResponse,
        details: &[(JobId, Option<JobDetail>)],
        worker_map: WorkerMap,
    ) {
        let mut msgs = vec![];

        let mut format = |count: u32, action: &str, color| {
            if count > 0 {
                let job = pluralize("job", count as usize);
                msgs.push(format!("{count} {job} {action}").color(color).to_string());
            }
        };

        format(response.finished, "finished", TASK_COLOR_FINISHED);
        format(response.failed, "failed", TASK_COLOR_FAILED);
        format(response.canceled, "canceled", TASK_COLOR_CANCELED);
        format(response.invalid, "invalid", TASK_COLOR_INVALID);

        for detail in details {
            match detail {
                (id, None) => log::warn!("Job {id} not found"),
                (_id, Some(detail)) => {
                    self.print_task_summary(&detail.tasks, &detail.info, &worker_map)
                }
            }
        }

        println!(
            "Wait finished in {}: {}",
            format_duration(duration),
            msgs.join(", ")
        );
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

    fn print_task_list(
        &self,
        mut jobs: Vec<(JobId, JobDetail)>,
        worker_map: WorkerMap,
        _server_uid: &str,
        verbosity: Verbosity,
    ) {
        let mut is_truncated = false;
        jobs.sort_unstable_by_key(|x| x.0);
        let mut rows: Vec<Vec<CellStruct>> = vec![];
        let jobs_len = jobs.len();

        for (id, job) in jobs {
            let mut tasks = job.tasks;
            tasks.sort_unstable_by_key(|t| t.0);

            rows.append(
                &mut tasks
                    .iter()
                    .map(|(task_id, task)| {
                        let (start, end) = get_task_time(&task.state);
                        let mut job_rows = match jobs_len {
                            1 => vec![],
                            _ => vec![id.cell().justify(Justify::Right)],
                        };

                        job_rows.append(&mut vec![
                            task_id.cell().justify(Justify::Right),
                            status_to_cell(&get_task_status(&task.state)),
                            format_workers(task.state.get_workers(), &worker_map).cell(),
                            format_task_duration(start, end).cell(),
                            match (verbosity, &task.state) {
                                (Verbosity::Normal, JobTaskState::Failed { error, .. }) => {
                                    let mut error_mut = error.clone();
                                    if error_mut.len() >= ERROR_TRUNCATE_LENGTH_LIST {
                                        error_mut.truncate(ERROR_TRUNCATE_LENGTH_LIST);
                                        error_mut.push_str("...");
                                        is_truncated = true;
                                    }
                                    error_mut.cell().foreground_color(Some(Color::Red))
                                }
                                (Verbosity::Verbose, JobTaskState::Failed { error, .. }) => {
                                    error.to_owned().cell().foreground_color(Some(Color::Red))
                                }
                                _ => "".cell(),
                            },
                        ]);

                        job_rows
                    })
                    .collect(),
            );
        }

        let mut header = match jobs_len {
            1 => vec![],
            _ => vec!["Job ID".cell().bold(true)],
        };

        header.append(&mut vec![
            "Task ID".cell().bold(true),
            "State".cell().bold(true),
            "Worker".cell().bold(true),
            "Makespan".cell().bold(true),
            "Error".cell().bold(true),
        ]);

        self.print_horizontal_table(rows, header);
        if is_truncated {
            log::info!("An error message was truncated. Use -v to display the full error.");
        }
    }

    fn print_task_info(
        &self,
        job: (JobId, JobDetail),
        tasks: &[(JobTaskId, JobTaskInfo)],
        worker_map: WorkerMap,
        server_uid: &str,
        verbosity: Verbosity,
    ) {
        let (job_id, job) = job;
        let task_to_paths = resolve_task_paths(&job, server_uid);
        let mut is_truncated = false;

        for (task_id, task) in tasks {
            let (start, end) = get_task_time(&task.state);
            let (cwd, stdout, stderr) = format_task_paths(&task_to_paths, *task_id);

            let (task_desc, task_deps) = if let Some(x) =
                job.submit_descs.iter().find_map(|submit_desc| {
                    match &submit_desc.description().task_desc {
                        JobTaskDescription::Array {
                            ids,
                            entries: _,
                            task_desc,
                        } if ids.contains(task_id.as_num()) => Some((task_desc, [].as_slice())),
                        JobTaskDescription::Array { .. } => None,
                        JobTaskDescription::Graph { tasks } => tasks
                            .iter()
                            .find(|t| t.id == *task_id)
                            .map(|task_dep| (&task_dep.task_desc, task_dep.task_deps.as_slice())),
                    }
                }) {
                x
            } else {
                log::error!("Task {task_id} not found in (graph) job {job_id}");
                return;
            };

            match &task_desc.kind {
                TaskKind::ExternalProgram(TaskKindProgram {
                    program,
                    pin_mode,
                    task_dir,
                }) => {
                    let mut env_vars: Vec<(_, _)> =
                        program.env.iter().filter(|(k, _)| !is_hq_env(k)).collect();
                    env_vars.sort_by_key(|item| item.0);

                    let rows: Vec<Vec<CellStruct>> = vec![
                        vec!["Task ID".cell().bold(true), task_id.cell()],
                        vec![
                            "State".cell().bold(true),
                            status_to_cell(&get_task_status(&task.state)),
                        ],
                        vec![
                            "Command".cell().bold(true),
                            program
                                .args
                                .iter()
                                .map(|x| textwrap::fill(&x.to_string(), TERMINAL_WIDTH))
                                .collect::<Vec<String>>()
                                .join("\n")
                                .cell(),
                        ],
                        vec![
                            "Environment".cell().bold(true),
                            env_vars
                                .into_iter()
                                .map(|(k, v)| format!("{k}={v}"))
                                .collect::<Vec<_>>()
                                .join("\n")
                                .cell(),
                        ],
                        vec![
                            "Worker".cell().bold(true),
                            format_workers(task.state.get_workers(), &worker_map).cell(),
                        ],
                        vec![
                            "Start".cell().bold(true),
                            start
                                .map(|x| format_time(x).to_string())
                                .unwrap_or_else(|| "".to_string())
                                .cell(),
                        ],
                        vec![
                            "End".cell().bold(true),
                            end.map(|x| format_time(x).to_string())
                                .unwrap_or_else(|| "".to_string())
                                .cell(),
                        ],
                        vec![
                            "Makespan".cell().bold(true),
                            format_task_duration(start, end).cell(),
                        ],
                        vec!["Workdir".cell().bold(true), cwd.cell()],
                        vec!["Stdout".cell().bold(true), stdout.cell()],
                        vec!["Stderr".cell().bold(true), stderr.cell()],
                        vec![
                            "Time limit".cell().bold(true),
                            task_desc
                                .time_limit
                                .map(|duration| format_duration(duration).to_string())
                                .unwrap_or_else(|| "None".to_string())
                                .cell(),
                        ],
                        vec![
                            "Resources".cell().bold(true),
                            format_resource_variants(&task_desc.resources).cell(),
                        ],
                        vec!["Priority".cell().bold(true), task_desc.priority.cell()],
                        vec!["Pin".cell().bold(true), pin_mode.to_str().cell()],
                        vec![
                            "Task dir".cell().bold(true),
                            if *task_dir { "yes" } else { "no" }.cell(),
                        ],
                        vec![
                            "Crash limit".cell().bold(true),
                            task_desc.crash_limit.cell(),
                        ],
                        vec![
                            "Dependencies".cell().bold(true),
                            task_deps
                                .iter()
                                .map(|task_id| task_id.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                                .cell(),
                        ],
                        vec![
                            "Error".cell().bold(true),
                            match (verbosity, &task.state) {
                                (Verbosity::Normal, JobTaskState::Failed { error, .. }) => {
                                    let mut error_mut = error.clone();
                                    if error_mut.len() >= ERROR_TRUNCATE_LENGTH_INFO {
                                        error_mut.truncate(ERROR_TRUNCATE_LENGTH_INFO);
                                        error_mut.push_str("...");
                                        is_truncated = true;
                                    }
                                    error_mut.cell().foreground_color(Some(Color::Red))
                                }
                                (Verbosity::Verbose, JobTaskState::Failed { error, .. }) => {
                                    error.to_owned().cell().foreground_color(Some(Color::Red))
                                }
                                _ => "".cell(),
                            },
                        ],
                    ];
                    self.print_vertical_table(rows);
                }
            }
        }

        if is_truncated {
            log::info!("An error message(s) was truncated. Use -v to display the full error(s).");
        }
    }

    fn print_summary(&self, filename: &Path, summary: Summary) {
        let rows = vec![
            vec!["Path".cell().bold(true), filename.display().cell()],
            vec![
                "Files".cell().bold(true),
                summary.n_files.to_string().cell(),
            ],
            vec!["Jobs".cell().bold(true), summary.n_jobs.to_string().cell()],
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

    fn print_autoalloc_queues(&self, info: AutoAllocListQueuesResponse) {
        let mut queues: Vec<_> = info.queues.into_iter().collect();
        queues.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let rows: Vec<_> = queues
            .into_iter()
            .map(|(id, data)| {
                let QueueData {
                    params,
                    name,
                    manager_type,
                    state,
                    known_worker_resources,
                } = data;

                vec![
                    id.cell(),
                    match state {
                        QueueState::Active => "ACTIVE",
                        QueueState::Paused => "PAUSED",
                    }
                    .cell(),
                    params.backlog.cell(),
                    params.max_workers_per_alloc.cell(),
                    params.max_worker_count.unwrap_or_default().cell(),
                    format_duration(params.timelimit).to_string().cell(),
                    manager_type.cell(),
                    known_worker_resources
                        .map(|res| resources_summary(&res, false))
                        .unwrap_or_else(|| "<unknown>".to_string())
                        .cell(),
                    name.unwrap_or_default().cell(),
                    params.additional_args.join(",").cell(),
                ]
            })
            .collect();

        let header = vec![
            "ID".cell().bold(true),
            "State".cell().bold(true),
            "Backlog size".cell().bold(true),
            "Max workers per alloc".cell().bold(true),
            "Max worker count".cell().bold(true),
            "Timelimit".cell().bold(true),
            "Manager".cell().bold(true),
            "Worker resources".cell().bold(true),
            "Name".cell().bold(true),
            "Args".cell().bold(true),
        ];
        self.print_horizontal_table(rows, header);
    }

    fn print_allocations(&self, mut allocations: Vec<Allocation>) {
        let format_time = |time: Option<AbsoluteTime>| match time {
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
                    allocation.working_dir.as_ref().display().cell(),
                    allocation.target_worker_count.cell(),
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
        println!("Summary:\n{}\n", resources_summary(descriptor, true));
        println!("Full Description:\n{}", resources_full_describe(descriptor));
    }

    fn print_error(&self, error: Error) {
        eprintln!("{error:?}");
    }

    fn print_job_open(&self, job_id: JobId) {
        println!(
            "Job {} is open.",
            job_id.to_string().color(colored::Color::Green),
        );
    }

    fn print_explanation(&self, task_id: TaskId, explanation: &TaskExplanation) {
        if explanation.n_task_deps == 0 {
            println!(
                "Task {} is {} to run because it has no dependencies.",
                task_id.to_string().color(colored::Color::Cyan),
                "ready".color(colored::Color::Yellow),
            )
        } else if explanation.n_waiting_deps == explanation.n_task_deps {
            println!(
                "Task {} is {} to run because its all {} dependencies are finished.",
                task_id.to_string().color(colored::Color::Cyan),
                "ready".color(colored::Color::Yellow),
                explanation
                    .n_task_deps
                    .to_string()
                    .color(colored::Color::Cyan),
            )
        } else {
            println!(
                "Task {} is {} to run, because {} dependencies are not finished.",
                task_id.to_string().color(colored::Color::Cyan),
                "not ready".color(colored::Color::Green),
                format!("{}/{}", explanation.n_waiting_deps, explanation.n_task_deps)
                    .to_string()
                    .color(colored::Color::Red)
            )
        }
        let runnable_worker_ids = explanation
            .workers
            .iter()
            .filter(|&w| w.is_enabled())
            .map(|w| w.worker_id.as_num())
            .collect_vec();
        if runnable_worker_ids.is_empty() {
            println!(
                "There is {} where the task can run.",
                "no worker".color(colored::Color::Red)
            );
        } else {
            let count = runnable_worker_ids.len();
            let ids = IntArray::from_sorted_ids(runnable_worker_ids.into_iter());
            println!(
                "The task can run on {} {}: {}",
                count.to_string().color(colored::Color::Green),
                pluralize("worker", count),
                ids.to_string().color(colored::Color::Green)
            );
        }
        if !explanation.workers.is_empty() {
            let mut rows = Vec::new();
            for w in &explanation.workers {
                let all_varints = w.n_variants();
                let enabled_variants = w.n_enabled_variants();
                let can_run = if enabled_variants == 0 {
                    "No".cell().foreground_color(Some(Color::Red))
                } else if all_varints == 1 {
                    "Yes".cell().foreground_color(Some(Color::Green))
                } else {
                    format!(
                        "{} ({}/{})",
                        "Yes".color(colored::Color::Green),
                        enabled_variants.to_string().color(colored::Color::Green),
                        all_varints
                    )
                    .cell()
                };
                let mut header = vec![w.worker_id.cell(), can_run];
                for (i, variant) in w.variants.iter().enumerate() {
                    if !variant.is_empty() {
                        let (mut rtype, mut provs, mut rqs) = (Vec::new(), Vec::new(), Vec::new());
                        variant.iter().for_each(|v| {
                            let (rt, rq, pr) = explanation_item_to_strings(v);
                            rtype.push(rt.to_string());
                            provs.push(rq.to_string());
                            rqs.push(pr.to_string());
                        });
                        if header.is_empty() {
                            header.push("".cell());
                            header.push("".cell());
                        }
                        header.push(i.cell().foreground_color(
                            if variant.iter().any(|v| v.is_blocking()) {
                                Some(Color::Red)
                            } else {
                                None
                            },
                        ));
                        header.push(rtype.join("\n").cell());
                        header.push(provs.join("\n").cell());
                        header.push(rqs.join("\n").cell());
                        rows.push(std::mem::take(&mut header));
                    }
                }
                if !header.is_empty() {
                    for _ in 0..5 {
                        header.push("".cell());
                    }
                    rows.push(header);
                }
            }
            let header = vec![
                "Worker Id".cell(),
                "Runnable".cell(),
                "Variant".cell(),
                "Type".cell(),
                "Provides".cell(),
                "Request".cell(),
            ];
            self.print_horizontal_table(rows, header);
        }
    }

    fn print_allocation_output(
        &self,
        allocation: Allocation,
        stream: OutputStream,
    ) -> anyhow::Result<()> {
        let path = match stream {
            OutputStream::Stdout => allocation.working_dir.stdout(),
            OutputStream::Stderr => allocation.working_dir.stderr(),
        };
        log::info!("Outputting allocation path at {}", path.display());
        output_file(&path);
        Ok(())
    }
}

struct AllocationTimes {
    queued_at: AbsoluteTime,
    started_at: Option<AbsoluteTime>,
    finished_at: Option<AbsoluteTime>,
}

impl AllocationTimes {
    fn get_queued_at(&self) -> AbsoluteTime {
        self.queued_at
    }
    fn get_started_at(&self) -> Option<AbsoluteTime> {
        self.started_at
    }
    fn get_finished_at(&self) -> Option<AbsoluteTime> {
        self.finished_at
    }
}

fn stdio_to_str(stdio: &StdioDef) -> &str {
    match stdio {
        StdioDef::Null => "<None>",
        StdioDef::File { path, .. } => path.to_str().unwrap(),
        StdioDef::Pipe => "<Stream>",
    }
}

// Allocation
fn allocation_status_to_cell(status: &AllocationState) -> CellStruct {
    match status {
        AllocationState::Queued { .. } => "QUEUED".cell().foreground_color(Some(Color::Yellow)),
        AllocationState::Running { .. } => "RUNNING".cell().foreground_color(Some(Color::Blue)),
        AllocationState::Finished { .. } | AllocationState::FinishedUnexpectedly { .. } => {
            match status.is_failed() {
                true => "FAILED".cell().foreground_color(Some(Color::Red)),
                false => "FINISHED".cell().foreground_color(Some(Color::Green)),
            }
        }
    }
}

fn allocation_times_from_alloc(allocation: &Allocation) -> AllocationTimes {
    let mut started = None;
    let mut finished = None;

    match &allocation.status {
        AllocationState::Queued { .. } => {}
        AllocationState::Running { started_at, .. } => {
            started = Some(started_at);
        }
        AllocationState::Finished {
            started_at,
            finished_at,
            ..
        } => {
            started = Some(started_at);
            finished = Some(finished_at);
        }
        AllocationState::FinishedUnexpectedly {
            started_at,
            finished_at,
            ..
        } => {
            started = started_at.as_ref();
            finished = Some(finished_at);
        }
    }
    AllocationTimes {
        queued_at: allocation.queued_at,
        started_at: started.copied(),
        finished_at: finished.copied(),
    }
}

/// Jobs & Tasks
fn job_status_to_cell(info: &JobInfo) -> String {
    let row = |result: &mut String, string, value, color| {
        if value > 0 {
            let text = format!("{string} ({value})").color(color);
            writeln!(result, "{text}").unwrap();
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

pub fn worker_status(worker_info: &WorkerInfo) -> CellStruct {
    match worker_info.ended.as_ref() {
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
        Some(WorkerExitInfo {
            reason: LostWorkerReason::TimeLimitReached,
            ..
        }) => "TIME LIMIT REACHED"
            .cell()
            .foreground_color(Some(Color::Cyan)),
    }
}

pub fn job_progress_bar(counters: JobTaskCounters, n_tasks: JobTaskCount, width: usize) -> String {
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
    job_detail: JobDetail,
    output_stream: OutputStream,
    task_header: bool,
    task_paths: TaskToPathsMap,
) -> anyhow::Result<()> {
    let read_stream = |task_id: JobTaskId, output_stream: &OutputStream| {
        let (_, stdout_path, stderr_path) = get_task_paths(&task_paths, task_id);
        let (opt_path, stream_name) = match output_stream {
            OutputStream::Stdout => (stdout_path, "stdout"),
            OutputStream::Stderr => (stderr_path, "stderr"),
        };

        if task_header {
            println!("# Job {}, task {}", job_detail.info.id, task_id);
        }

        if let Some(path) = opt_path {
            output_file(Path::new(path));
        } else {
            log::warn!("Task {task_id} has no `{stream_name}` stream associated with it");
        }
    };

    for (task_id, _task) in &job_detail.tasks {
        read_stream(*task_id, &output_stream);
    }

    Ok(())
}

fn output_file(path: &Path) {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    match File::open(path) {
        Ok(mut file) => {
            let copy = std::io::copy(&mut file, &mut stdout);
            if let Err(error) = copy {
                log::warn!(
                    "Could not output contents of `{}`: {error:?}",
                    path.display()
                );
            }
        }
        Err(error) => log::warn!("File at `{}` cannot be opened: {error:?}", path.display()),
    };
}

fn session_to_cell(is_open: bool) -> CellStruct {
    if is_open {
        "open".cell().foreground_color(Some(Color::Green))
    } else {
        "closed".cell()
    }
}

fn status_to_cell(status: &Status) -> CellStruct {
    match status {
        Status::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        Status::Opened => "OPENED".cell().foreground_color(Some(Color::Cyan)),
        Status::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        Status::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        Status::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        Status::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
    }
}

fn format_resource_request(rq: &ResourceRequest) -> String {
    if rq.n_nodes > 0 {
        return format!("nodes: {}", rq.n_nodes);
    }
    let mut result = String::new();
    let mut first = true;

    let mut entries: Vec<&ResourceRequestEntry> = rq.resources.iter().collect();
    entries.sort_unstable_by_key(|x| &x.resource);

    for grq in entries {
        write!(
            result,
            "{}{}: {}",
            if first { "" } else { "\n" },
            grq.resource,
            grq.policy
        )
        .unwrap();
        first = false;
    }
    result
}

fn format_resource_variants(rqv: &ResourceRequestVariants) -> String {
    if rqv.variants.len() == 1 {
        return format_resource_request(&rqv.variants[0]);
    }

    let mut result = String::new();
    for (i, v) in rqv.variants.iter().enumerate() {
        let is_last = rqv.variants.len() == i + 1;
        write!(
            result,
            "# Variant {}\n{}{}",
            (i + 1),
            format_resource_request(v),
            if is_last { "" } else { "\n\n" }
        )
        .unwrap();
    }
    result
}

/// Formatting
pub fn format_job_workers(tasks: &[(JobTaskId, JobTaskInfo)], worker_map: &WorkerMap) -> String {
    // BTreeSet is used to both filter duplicates and keep a stable order
    let worker_set: BTreeSet<_> = tasks
        .iter()
        .filter_map(|(_, task)| task.state.get_workers())
        .flatten()
        .collect();
    let worker_count = worker_set.len();

    let mut result = format_comma_delimited(
        worker_set
            .into_iter()
            .take(MAX_DISPLAYED_WORKERS)
            .map(|id| format_worker(*id, worker_map)),
    );

    if worker_count > MAX_DISPLAYED_WORKERS {
        write!(result, ", … ({worker_count} total)").unwrap();
    }

    result
}

fn format_worker(id: WorkerId, worker_map: &WorkerMap) -> &str {
    worker_map
        .get(&id)
        .map(|s| s.as_str())
        .unwrap_or_else(|| "N/A")
}

fn format_workers<'a>(ids: Option<&[WorkerId]>, worker_map: &'a WorkerMap) -> Cow<'a, str> {
    match ids {
        Some(ids) => {
            if ids.len() == 1 {
                format_worker(ids[0], worker_map).into()
            } else {
                assert!(!ids.is_empty());
                let mut result = String::new();
                for id in ids {
                    result.push_str(format_worker(*id, worker_map));
                    result.push('\n');
                }
                result.into()
            }
        }
        None => "".into(),
    }
}

fn get_task_time(state: &JobTaskState) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    match state {
        JobTaskState::Canceled {
            started_data: Some(started_data),
            cancelled_date,
        } => (Some(started_data.start_date), Some(*cancelled_date)),
        JobTaskState::Running { started_data, .. } => (Some(started_data.start_date), None),
        JobTaskState::Finished {
            started_data,
            end_date,
            ..
        }
        | JobTaskState::Failed {
            started_data: Some(started_data),
            end_date,
            ..
        } => (Some(started_data.start_date), Some(*end_date)),
        JobTaskState::Canceled {
            started_data: None,
            cancelled_date: _,
        }
        | JobTaskState::Failed {
            started_data: None, ..
        }
        | JobTaskState::Waiting => (None, None),
    }
}

/// Returns Option(working directory, stdout, stderr)
fn get_task_paths(
    task_map: &TaskToPathsMap,
    task_id: JobTaskId,
) -> (Option<&str>, Option<&str>, Option<&str>) {
    match task_map[&task_id] {
        Some(ref paths) => (
            Some(paths.cwd.to_str().unwrap()),
            match &paths.stdout {
                StdioDef::File { path, .. } => Some(path.to_str().unwrap()),
                _ => None,
            },
            match &paths.stderr {
                StdioDef::File { path, .. } => Some(path.to_str().unwrap()),
                _ => None,
            },
        ),
        None => (None, None, None),
    }
}

/// Returns (working directory, stdout, stderr)
fn format_task_paths(task_map: &TaskToPathsMap, task_id: JobTaskId) -> (&str, &str, &str) {
    match task_map[&task_id] {
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

fn format_systemtime(time: AbsoluteTime) -> impl Display {
    let datetime: DateTime<Local> = time.inner().into();
    datetime.format("%d.%m.%Y %H:%M:%S")
}

fn format_time(time: DateTime<Utc>) -> impl Display {
    let datetime: DateTime<Local> = time.into();
    datetime.format("%d.%m.%Y %H:%M:%S")
}

fn resources_full_describe(resources: &ResourceDescriptor) -> String {
    let mut result = String::new();
    let mut first = true;
    for descriptor in &resources.resources {
        write!(
            result,
            "{}{}: {}",
            if first { "" } else { "\n" },
            &descriptor.name,
            format_descriptor_kind(&descriptor.kind),
        )
        .unwrap();
        first = false;
    }
    result
}

fn format_descriptor_kind(kind: &ResourceDescriptorKind) -> String {
    match kind {
        ResourceDescriptorKind::List { values } => {
            format!("[{}]", format_comma_delimited(values))
        }
        ResourceDescriptorKind::Groups { groups } => {
            format!(
                "[{}]",
                format_comma_delimited(
                    groups
                        .iter()
                        .map(|g| format!("[{}]", format_comma_delimited(g)))
                )
            )
        }
        ResourceDescriptorKind::Range { start, end } if start == end => format!("[{start}]"),
        ResourceDescriptorKind::Range { start, end } => format!("range({start}-{end})"),
        ResourceDescriptorKind::Sum { size } => format!("sum({size})"),
    }
}

fn resource_summary_kind(kind: &ResourceDescriptorKind) -> String {
    match kind {
        ResourceDescriptorKind::List { values } => values.len().to_string(),
        ResourceDescriptorKind::Groups { groups } => {
            if groups.len() == 1 {
                format!("{}", groups[0].len())
            } else {
                let mut counts = Map::<usize, usize>::default();
                for group in groups {
                    *counts.entry(group.len()).or_default() += 1;
                }
                let mut counts: Vec<_> = counts.into_iter().collect();
                counts.sort_unstable();
                counts
                    .iter()
                    .map(|(cores, count)| format!("{count}x{cores}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            }
        }
        ResourceDescriptorKind::Range { start, end } => {
            (end.as_num() - start.as_num() + 1).to_string()
        }
        ResourceDescriptorKind::Sum { size } => size.to_string(),
    }
}

fn resources_summary(resources: &ResourceDescriptor, multiline: bool) -> String {
    let special_format = |descriptor: &ResourceDescriptorItem| -> Option<String> {
        if descriptor.name == tako::resources::MEM_RESOURCE_NAME {
            if let ResourceDescriptorKind::Sum { size } = descriptor.kind {
                return Some(human_mem_amount(size));
            }
        }
        None
    };

    let mut result = String::new();
    let mut first = true;
    for descriptor in &resources.resources {
        write!(
            result,
            "{}{}{} {}",
            if first {
                ""
            } else if multiline {
                "\n"
            } else {
                "; "
            },
            &descriptor.name,
            if multiline { ":" } else { "" },
            special_format(descriptor).unwrap_or_else(|| resource_summary_kind(&descriptor.kind))
        )
        .unwrap();
        if multiline
            && resources
                .coupling
                .as_ref()
                .is_some_and(|c| c.names.contains(&descriptor.name))
        {
            result.push_str(" [coupled]");
        }
        first = false;
    }
    result
}

fn explanation_item_to_strings(
    item: &TaskExplainItem,
) -> (ColoredString, ColoredString, ColoredString) {
    let request_color = if item.is_blocking() {
        colored::Color::Red
    } else {
        colored::Color::Green
    };
    match item {
        TaskExplainItem::Time {
            min_time,
            remaining_time,
        } => (
            "time".color(colored::Color::Magenta),
            if let Some(time) = remaining_time {
                human_duration(chrono::Duration::from_std(*time).unwrap())
                    .to_string()
                    .color(colored::Color::Yellow)
            } else {
                "unknown".color(colored::Color::Yellow)
            },
            human_duration(chrono::Duration::from_std(*min_time).unwrap())
                .to_string()
                .color(request_color),
        ),
        TaskExplainItem::Resources {
            resource,
            request_amount,
            worker_amount,
        } => (
            resource.color(colored::Color::Cyan),
            format!("{worker_amount}").color(colored::Color::Yellow),
            format!("{request_amount}").color(request_color),
        ),
        TaskExplainItem::WorkerGroup {
            n_nodes,
            group_size,
        } => (
            "nodes".color(colored::Color::Magenta),
            format!("group size {group_size}").color(colored::Color::Yellow),
            format!("{n_nodes}").color(request_color),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::client::output::cli::{resources_full_describe, resources_summary};
    use tako::internal::tests::utils::shared::{res_kind_groups, res_kind_list, res_kind_sum};
    use tako::resources::{
        MEM_RESOURCE_NAME, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
    };

    #[test]
    fn test_resources_summary() {
        let d = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(1),
            }],
            None,
        );
        assert_eq!(resources_summary(&d, false), "cpus 1");

        let d = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(5),
            }],
            None,
        );
        assert_eq!(resources_summary(&d, true), "cpus: 5");

        let d = ResourceDescriptor::new(
            vec![
                ResourceDescriptorItem {
                    name: "cpus".into(),
                    kind: res_kind_groups(&[
                        vec!["0", "1", "2", "4"],
                        vec!["10", "11", "12", "14"],
                    ]),
                },
                ResourceDescriptorItem {
                    name: "gpus".into(),
                    kind: res_kind_list(&["4", "7"]),
                },
                ResourceDescriptorItem {
                    name: "mmmem".into(),
                    kind: res_kind_sum(1234),
                },
                ResourceDescriptorItem {
                    name: "zzz".into(),
                    kind: res_kind_groups(&[
                        vec!["0", "1"],
                        vec!["10", "11"],
                        vec!["20", "21"],
                        vec!["30", "31"],
                        vec!["40", "41"],
                        vec!["50", "51", "52", "53", "54", "55"],
                    ]),
                },
            ],
            None,
        );
        assert_eq!(
            resources_summary(&d, true),
            "cpus: 2x4\ngpus: 2\nmmmem: 1234\nzzz: 5x2 1x6"
        );
        assert_eq!(
            resources_summary(&d, false),
            "cpus 2x4; gpus 2; mmmem 1234; zzz 5x2 1x6"
        );
    }

    #[test]
    fn test_resource_full_describe() {
        let d = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(1),
            }],
            None,
        );
        assert_eq!(resources_full_describe(&d), "cpus: [0]");

        let d = ResourceDescriptor::new(
            vec![
                ResourceDescriptorItem {
                    name: "cpus".into(),
                    kind: res_kind_groups(&[
                        vec!["0", "1", "2", "4"],
                        vec!["10", "11", "12", "14"],
                    ]),
                },
                ResourceDescriptorItem {
                    name: "gpus".into(),
                    kind: res_kind_list(&["4", "7"]),
                },
                ResourceDescriptorItem {
                    name: "mem".into(),
                    kind: res_kind_sum(1234),
                },
            ],
            None,
        );
        assert_eq!(
            resources_full_describe(&d),
            "cpus: [[0,1,2,4],[10,11,12,14]]\ngpus: [4,7]\nmem: sum(1234)"
        );
    }

    #[test]
    fn test_resources_summary_mem() {
        let d = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: MEM_RESOURCE_NAME.into(),
                kind: res_kind_sum(4 * 1024 + 123),
            }],
            None,
        );
        assert_eq!(resources_summary(&d, false), "mem 4.12 GiB");
    }
}
