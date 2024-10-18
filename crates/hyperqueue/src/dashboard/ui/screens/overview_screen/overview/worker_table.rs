use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Cell, Row};

use tako::gateway::LostWorkerReason;
use tako::WorkerId;

use crate::dashboard::data::timelines::worker_timeline::{WorkerDisconnectInfo, WorkerStatus};
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::styles::table_style_selected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    get_progress_bar_color, render_progress_bar_at, ProgressPrintStyle,
};
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::dashboard::utils::{get_average_cpu_usage_for_worker, get_memory_usage_pct};

#[derive(Default)]
pub struct WorkerTable {
    table: StatefulTable<WorkerRow>,
}

enum WorkerState {
    Connected {
        num_tasks: u32,
        average_cpu_usage: Option<f64>,
        memory_usage: Option<u64>,
    },
    Disconnected {
        info: WorkerDisconnectInfo,
    },
}

struct WorkerRow {
    id: WorkerId,
    hostname: Option<String>,
    state: WorkerState,
}

impl WorkerTable {
    pub fn update(&mut self, data: &DashboardData) {
        let current_time = data.current_time();

        let mut rows: Vec<WorkerRow> = data
            .workers()
            .query_known_worker_ids_at(current_time)
            .map(|(worker_id, status)| {
                let config = data.workers().query_worker_config_for(worker_id);
                let hostname = config.map(|config| config.hostname.clone());

                let state = match status {
                    WorkerStatus::Disconnected(info) => WorkerState::Disconnected { info },
                    WorkerStatus::Connected => {
                        let overview = data
                            .workers()
                            .query_worker_overview_at(worker_id, current_time)
                            .map(|item| &item.item);
                        let hw_state = overview.and_then(|o| o.hw_state.as_ref());
                        let average_cpu_usage = hw_state.map(get_average_cpu_usage_for_worker);
                        let memory_usage =
                            hw_state.map(|s| get_memory_usage_pct(&s.state.memory_usage));
                        let running_tasks =
                            overview.map(|o| o.running_tasks.len() as u32).unwrap_or(0);
                        WorkerState::Connected {
                            num_tasks: running_tasks,
                            average_cpu_usage,
                            memory_usage,
                        }
                    }
                };

                WorkerRow {
                    id: worker_id,
                    hostname,
                    state,
                }
            })
            .collect();

        rows.sort_by_key(|w| {
            (
                if matches!(w.state, WorkerState::Connected { .. }) {
                    0
                } else {
                    1
                },
                w.id,
            )
        });

        self.table.set_items(rows);
    }

    pub fn select_next_worker(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_worker(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<WorkerId> {
        let selection = self.table.current_selection();
        selection.map(|row| row.id)
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Worker list",
                inline_help: "",
                table_headers: Some(vec![
                    "ID",
                    "Status",
                    "Hostname",
                    "Running tasks",
                    "CPU util",
                    "Mem util",
                ]),
                column_widths: vec![
                    Constraint::Percentage(10),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(10),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                ],
            },
            |data| {
                let missing_data = "";

                let (status, tasks, cpu, mem) = match &data.state {
                    WorkerState::Connected {
                        num_tasks,
                        average_cpu_usage,
                        memory_usage,
                    } => {
                        let cpu_progress = (average_cpu_usage.unwrap_or(0.0)) / 100.0;
                        let mem_progress = (memory_usage.unwrap_or(0) as f64) / 100.0;
                        let cpu_prog_bar = render_progress_bar_at(
                            None,
                            cpu_progress,
                            18,
                            ProgressPrintStyle::default(),
                        );

                        let mem_prog_bar = render_progress_bar_at(
                            None,
                            mem_progress,
                            18,
                            ProgressPrintStyle::default(),
                        );

                        (
                            Cell::from("online").style(Style::default().fg(Color::Green)),
                            Cell::from(num_tasks.to_string()),
                            Cell::from(cpu_prog_bar).style(get_progress_bar_color(cpu_progress)),
                            Cell::from(mem_prog_bar).style(get_progress_bar_color(mem_progress)),
                        )
                    }
                    WorkerState::Disconnected { info } => {
                        let status = match &info.reason {
                            LostWorkerReason::Stopped => "stopped",
                            LostWorkerReason::ConnectionLost | LostWorkerReason::HeartbeatLost => {
                                "lost"
                            }
                            LostWorkerReason::IdleTimeout => "timeout",
                            LostWorkerReason::TimeLimitReached => "time-limit",
                        };

                        (
                            Cell::from(format!("offline: {status}"))
                                .style(Style::default().fg(Color::Red)),
                            Cell::from(missing_data.to_string()),
                            Cell::from(missing_data.to_string()),
                            Cell::from(missing_data.to_string()),
                        )
                    }
                };

                Row::new(vec![
                    Cell::from(data.id.to_string()),
                    Cell::from(
                        data.hostname
                            .clone()
                            .unwrap_or_else(|| missing_data.to_string()),
                    ),
                    status,
                    tasks,
                    cpu,
                    mem,
                ])
            },
            table_style_selected(),
        );
    }
}
