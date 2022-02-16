use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::MonitoringEvent;
use crate::WorkerId;
use std::time::SystemTime;
use tako::TaskId;

pub struct TaskInfo {
    pub worker_id: WorkerId,
    pub task_id: TaskId,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub current_task_state: DashboardTaskState,
}

#[derive(Copy, Clone)]
pub enum DashboardTaskState {
    Running,
    Finished,
    Failed,
}

impl TaskInfo {
    pub fn set_end_time_and_status(&mut self, end_time: &SystemTime, status: DashboardTaskState) {
        self.end_time = Some(*end_time);
        self.current_task_state = status;
    }
}

/// Stores information about the workers at different times
#[derive(Default)]
pub struct TaskTimeline {
    task_timeline: Vec<TaskInfo>,
}

//TODO: Optimize
impl TaskTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[MonitoringEvent]) {
        for event in events {
            match &event.payload {
                MonitoringEventPayload::TaskStarted { task_id, worker_id } => {
                    self.task_timeline.push(TaskInfo {
                        worker_id: *worker_id,
                        task_id: *task_id,
                        start_time: event.time,
                        end_time: None,
                        current_task_state: DashboardTaskState::Running,
                    });
                }
                MonitoringEventPayload::TaskFinished(finished_id) => {
                    for info in &mut self.task_timeline {
                        if info.task_id == *finished_id {
                            info.set_end_time_and_status(&event.time, DashboardTaskState::Finished);
                        }
                    }
                }
                MonitoringEventPayload::TaskFailed(failed_id) => {
                    for info in &mut self.task_timeline {
                        if info.task_id == *failed_id {
                            info.set_end_time_and_status(&event.time, DashboardTaskState::Failed);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn get_worker_task_history(
        &self,
        worker_id: WorkerId,
        at_time: SystemTime,
    ) -> impl Iterator<Item = &TaskInfo> + '_ {
        self.task_timeline
            .iter()
            .filter(move |info| info.worker_id == worker_id && info.start_time <= at_time)
    }
}
