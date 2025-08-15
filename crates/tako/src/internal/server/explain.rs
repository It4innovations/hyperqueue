use crate::WorkerId;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::server::workergroup::WorkerGroup;
use crate::resources::{NumOfNodes, ResourceAmount, ResourceMap};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExplanation {
    pub n_task_deps: u32,
    pub n_waiting_deps: u32,
    pub workers: Vec<TaskExplanationForWorker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExplanationForWorker {
    pub worker_id: WorkerId,
    pub variants: Vec<Vec<TaskExplainItem>>,
}

impl TaskExplanationForWorker {
    pub fn n_enabled_variants(&self) -> u32 {
        self.variants
            .iter()
            .map(|v| {
                if v.iter().any(|item| item.is_blocking()) {
                    0
                } else {
                    1
                }
            })
            .sum()
    }

    pub fn n_variants(&self) -> u32 {
        self.variants.len() as u32
    }

    pub fn is_enabled(&self) -> bool {
        self.n_enabled_variants() > 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskExplainItem {
    Time {
        min_time: Duration,
        remaining_time: Option<Duration>,
    },
    Resources {
        resource: String,
        request_amount: ResourceAmount,
        worker_amount: ResourceAmount,
    },
    WorkerGroup {
        n_nodes: NumOfNodes,
        group_size: NumOfNodes,
    },
}

impl TaskExplainItem {
    pub fn is_blocking(&self) -> bool {
        match self {
            TaskExplainItem::Time {
                min_time,
                remaining_time: Some(remaining_time),
            } => min_time > remaining_time,
            TaskExplainItem::Time {
                min_time: _,
                remaining_time: None,
            } => false,
            TaskExplainItem::Resources {
                resource: _,
                request_amount,
                worker_amount,
            } => request_amount > worker_amount,
            TaskExplainItem::WorkerGroup {
                n_nodes,
                group_size,
            } => n_nodes > group_size,
        }
    }
}

pub fn task_explain_init(task: &Task) -> TaskExplanation {
    TaskExplanation {
        n_task_deps: task.task_deps.len() as u32,
        n_waiting_deps: match &task.state {
            TaskRuntimeState::Waiting(w) => w.unfinished_deps,
            _ => 0,
        },
        workers: Vec::new(),
    }
}

pub fn task_explain_for_worker(
    resource_map: &ResourceMap,
    task: &Task,
    worker: &Worker,
    worker_group: &WorkerGroup,
    now: std::time::Instant,
) -> TaskExplanationForWorker {
    TaskExplanationForWorker {
        worker_id: worker.id,
        variants: task
            .configuration
            .resources
            .requests()
            .iter()
            .map(|rq| {
                let mut result = Vec::new();
                if !rq.min_time().is_zero() {
                    result.push(TaskExplainItem::Time {
                        min_time: rq.min_time(),
                        remaining_time: worker.remaining_time(now),
                    });
                }
                if rq.is_multi_node() {
                    result.push(TaskExplainItem::WorkerGroup {
                        n_nodes: rq.n_nodes(),
                        group_size: worker_group.size() as NumOfNodes,
                    })
                } else {
                    for entry in rq.entries() {
                        let request_amount = entry.request.min_amount();
                        let worker_amount = worker.resources.get(entry.resource_id);
                        result.push(TaskExplainItem::Resources {
                            resource: resource_map
                                .get_name(entry.resource_id)
                                .unwrap()
                                .to_string(),
                            request_amount,
                            worker_amount,
                        })
                    }
                }
                result
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::server::explain::{TaskExplainItem, task_explain_for_worker};
    use crate::internal::server::worker::Worker;
    use crate::internal::server::workergroup::WorkerGroup;
    use crate::internal::tests::utils::schedule::create_test_worker_config;
    use crate::internal::tests::utils::task::TaskBuilder;
    use crate::resources::{
        ResourceAmount, ResourceDescriptor, ResourceDescriptorItem, ResourceMap,
    };
    use crate::{Set, WorkerId};
    use std::time::{Duration, Instant};

    #[test]
    fn explain_single_node() {
        let resource_map = ResourceMap::from_vec(vec!["cpus".to_string(), "gpus".to_string()]);
        let now = Instant::now();

        let wcfg = create_test_worker_config(1.into(), ResourceDescriptor::simple_cpus(4));
        let worker1 = Worker::new(1.into(), wcfg, &resource_map, now);

        let mut wcfg = create_test_worker_config(
            2.into(),
            ResourceDescriptor::new(
                vec![
                    ResourceDescriptorItem::range("cpus", 1, 10),
                    ResourceDescriptorItem::range("gpus", 1, 4),
                ],
                Default::default(),
            ),
        );
        wcfg.time_limit = Some(Duration::from_secs(40_000));
        let worker2 = Worker::new(2.into(), wcfg, &resource_map, now);

        let explain = |task, worker, now| {
            let group = WorkerGroup::new(Set::new());
            task_explain_for_worker(&resource_map, task, worker, &group, now)
        };

        let task_id = 1;
        let task = TaskBuilder::new(task_id).build();
        let r = explain(&task, &worker1, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 1);
        assert_eq!(r.n_enabled_variants(), 1);

        let task = TaskBuilder::new(task_id).time_request(20_000).build();
        let r = explain(&task, &worker1, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 2);
        assert_eq!(r.n_enabled_variants(), 1);

        let r = explain(&task, &worker2, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 2);
        assert_eq!(r.n_enabled_variants(), 1);

        let now2 = now + Duration::from_secs(21_000);
        let r = explain(&task, &worker1, now2);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 2);
        assert_eq!(r.n_enabled_variants(), 1);

        let r = explain(&task, &worker2, now2);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 2);
        assert!(matches!(
            r.variants[0][0],
            TaskExplainItem::Time {
                min_time,
                remaining_time,
            } if min_time == Duration::from_secs(20_000) && remaining_time == Some(Duration::from_secs(19_000))
        ));
        assert_eq!(r.n_enabled_variants(), 0);

        let task = TaskBuilder::new(task_id)
            .time_request(20_000)
            .cpus_compact(30)
            .add_resource(1, 3)
            .build();
        let r = explain(&task, &worker2, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 3);
        assert!(matches!(
            &r.variants[0][1],
            TaskExplainItem::Resources {
                resource, request_amount, worker_amount
            } if resource == "cpus" && *request_amount == ResourceAmount::new_units(30) && *worker_amount == ResourceAmount::new_units(10)
        ));
        assert_eq!(r.n_enabled_variants(), 0);

        let task = TaskBuilder::new(task_id)
            .time_request(30_000)
            .cpus_compact(15)
            .add_resource(1, 8)
            .next_resources()
            .cpus_compact(2)
            .add_resource(1, 32)
            .build();
        let r = explain(&task, &worker2, now2);
        assert_eq!(r.variants.len(), 2);
        assert_eq!(r.variants[0].len(), 3);
        assert_eq!(r.variants[1].len(), 2);
        assert!(matches!(
            r.variants[0][0],
            TaskExplainItem::Time {
                min_time,
                remaining_time,
            } if min_time == Duration::from_secs(30_000) && remaining_time == Some(Duration::from_secs(19_000))
        ));
        assert!(matches!(
            &r.variants[0][1],
            TaskExplainItem::Resources {
                resource, request_amount, worker_amount
            } if resource == "cpus" && *request_amount == ResourceAmount::new_units(15) && *worker_amount == ResourceAmount::new_units(10)
        ));
        assert!(matches!(
            &r.variants[0][2],
            TaskExplainItem::Resources {
                resource, request_amount, worker_amount
            } if resource == "gpus" && *request_amount == ResourceAmount::new_units(8) && *worker_amount == ResourceAmount::new_units(4)
        ));
        assert!(matches!(
            &r.variants[1][1],
            TaskExplainItem::Resources {
                resource, request_amount, worker_amount
            } if resource == "gpus" && *request_amount == ResourceAmount::new_units(32) && *worker_amount == ResourceAmount::new_units(4)
        ));
    }

    #[test]
    fn explain_multi_node() {
        let resource_map = ResourceMap::from_vec(vec!["cpus".to_string(), "gpus".to_string()]);
        let now = Instant::now();

        let wcfg = create_test_worker_config(1.into(), ResourceDescriptor::simple_cpus(4));
        let worker = Worker::new(1.into(), wcfg, &resource_map, now);
        let task = TaskBuilder::new(1).n_nodes(4).build();
        let mut wset = Set::new();
        wset.insert(WorkerId::new(1));
        wset.insert(WorkerId::new(2));
        wset.insert(WorkerId::new(3));
        wset.insert(WorkerId::new(132));
        let group = WorkerGroup::new(wset);
        let r = task_explain_for_worker(&resource_map, &task, &worker, &group, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 1);
        assert_eq!(r.n_enabled_variants(), 1);

        let mut wset = Set::new();
        wset.insert(WorkerId::new(1));
        wset.insert(WorkerId::new(132));
        let group = WorkerGroup::new(wset);
        let r = task_explain_for_worker(&resource_map, &task, &worker, &group, now);
        assert_eq!(r.variants.len(), 1);
        assert_eq!(r.variants[0].len(), 1);
        assert!(matches!(
            &r.variants[0][0],
            TaskExplainItem::WorkerGroup {
                n_nodes: 4,
                group_size: 2
            }
        ));
        assert_eq!(r.n_enabled_variants(), 0);
    }
}
