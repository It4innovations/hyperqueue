use crate::client::output::json::format_datetime;
use crate::server::event::Event;
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::{
    JobSubmitDescription, JobTaskDescription, SubmitRequest, TaskDescription,
};
use serde_json::json;
use tako::worker::WorkerOverview;

pub fn format_event(event: Event) -> serde_json::Value {
    json!({
        "time": format_datetime(event.time),
        "event": format_payload(event.payload)
    })
}

fn format_payload(event: EventPayload) -> serde_json::Value {
    match event {
        EventPayload::WorkerConnected(id, configuration) => json!({
            "type": "worker-connected",
            "id": id,
            "configuration": configuration
        }),
        EventPayload::WorkerLost(id, reason) => json!({
            "type": "worker-lost",
            "id": id,
            "reason": reason
        }),
        EventPayload::WorkerOverviewReceived(overview) => {
            let WorkerOverview {
                id,
                hw_state,
                data_node,
                ..
            } = *overview;
            json!({
                "type": "worker-overview",
                "id": id,
                "hw-state": hw_state,
                "data-node": data_node,
            })
        }
        EventPayload::AllocationQueueCreated(id, params) => {
            json!({
                "type": "autoalloc-queue-created",
                "queue-id": id,
                "params": params,
            })
        }
        EventPayload::AllocationQueueRemoved(id) => {
            json!({
                "type": "autoalloc-queue-removed",
                "queue-id": id
            })
        }
        EventPayload::AllocationQueued {
            queue_id,
            allocation_id,
            worker_count,
        } => {
            json!({
                "type": "autoalloc-allocation-queued",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
                "worker-count": worker_count
            })
        }
        EventPayload::AllocationStarted(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-started",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        EventPayload::AllocationFinished(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-finished",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        EventPayload::TaskStarted {
            task_id,
            instance_id,
            workers,
        } => {
            let mut event = json!({
                "type": "task-started",
                "job": task_id.job_id(),
                "task": task_id.job_task_id(),
                "instance": instance_id,
            });
            let map = event.as_object_mut().unwrap();
            if workers.len() == 1 {
                map.insert("worker".to_string(), json!(workers[0]));
            } else {
                map.insert("workers".to_string(), json!(workers));
            }
            event
        }
        EventPayload::TaskFinished { task_id } => json!({
            "type": "task-finished",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
        }),
        EventPayload::TasksCanceled { task_ids } => json!({
            "type": "task-canceled",
            "tasks": task_ids,
        }),
        EventPayload::TaskFailed { task_id, error } => json!({
            "type": "task-failed",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
            "error": error
        }),
        EventPayload::Submit {
            job_id,
            closed_job,
            serialized_desc,
        } => {
            let submit: SubmitRequest = serialized_desc.deserialize().expect("Invalid submit data");
            json!({
                "type": "job-created",
                "job": job_id,
                "closed_job": closed_job,
                "job_desc": &submit.job_desc,
                "submit_desc": SubmitDescFormatter(&submit.submit_desc).to_json()
            })
        }
        EventPayload::JobCompleted(job_id) => json!({
            "type": "job-completed",
            "job": job_id,
        }),
        EventPayload::ServerStart { server_uid } => {
            json!({
                "type": "server-start",
                "server_uid": server_uid
            })
        }
        EventPayload::ServerStop => {
            json!({
                "type": "server-stop",
            })
        }
        EventPayload::JobOpen(job_id, job_desc) => {
            json!({
                "type": "job-open",
                "job_id": job_id,
                "name": job_desc.name,
                "max_fails": job_desc.max_fails,
            })
        }
        EventPayload::JobClose(job_id) => {
            json!({
                "type": "job-close",
                "job_id": job_id
            })
        }
        EventPayload::TaskNotify(notify) => {
            json!({
                "task_id": notify.task_id,
                "worker_id": notify.worker_id,
                "message": notify.message,
            })
        }
    }
}

struct SubmitDescFormatter<'a>(&'a JobSubmitDescription);

impl SubmitDescFormatter<'_> {
    fn to_json(&self) -> serde_json::Value {
        let JobSubmitDescription {
            task_desc,
            submit_dir,
            stream_path,
        } = self.0;

        let task_desc = match task_desc {
            JobTaskDescription::Array {
                ids,
                entries: _,
                task_desc,
            } => {
                let TaskDescription {
                    kind: _,
                    resources,
                    time_limit,
                    priority,
                    crash_limit,
                } = task_desc;
                json!({
                    "ids": ids,
                    "resources": resources,
                    "time_limit": time_limit,
                    "priority": priority,
                    "crash_limit": crash_limit
                })
            }
            JobTaskDescription::Graph { tasks } => {
                json!({
                    "n_tasks": tasks.len()
                })
            }
        };

        json!({
            "submit_dir": submit_dir,
            "stream_path": stream_path,
            "task_desc": task_desc,
        })
    }
}
