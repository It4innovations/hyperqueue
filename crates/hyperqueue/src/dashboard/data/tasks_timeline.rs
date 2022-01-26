use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::MonitoringEvent;
use crate::WorkerId;
use std::time::SystemTime;
use tako::TaskId;

#[derive(Clone)]
pub struct TaskInfo {
    worker_id: WorkerId,
    task_id: TaskId,
    start_time: SystemTime,
    end_time: Option<SystemTime>,
}

impl TaskInfo {
    pub fn set_end_time(&mut self, end_time: &SystemTime) {
        self.end_time = Some(*end_time);
    }
}

/// Stores information about the workers at different times
#[derive(Default)]
pub struct TasksTimeline {
    task_timeline: Vec<TaskInfo>,
}

impl TasksTimeline {
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
                    });
                }
                MonitoringEventPayload::TaskFinished(finished_id) => {
                    for info in &mut self.task_timeline {
                        if info.task_id == *finished_id {
                            info.set_end_time(&event.time);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn get_tasks_running_on_worker(
        &self,
        worker_id: WorkerId,
        time: SystemTime,
    ) -> impl Iterator<Item = TaskInfo> + '_ {
        //todo: verify
        self.get_running_tasks_info(time)
            .filter(move |info| info.worker_id == worker_id)
    }

    pub fn get_running_tasks_info(&self, time: SystemTime) -> impl Iterator<Item = TaskInfo> + '_ {
        self.task_timeline
            .iter()
            .filter(move |info| {
                let has_started = info.start_time <= time;
                let has_finished = match info.end_time {
                    Some(end_time) => end_time <= time,
                    None => false,
                };
                has_started && !has_finished
            })
            .cloned()
    }
}
