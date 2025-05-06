use crate::internal::server::task::Task;
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::Worker;
use crate::internal::server::workergroup::WorkerGroup;
use crate::internal::server::workermap::WorkerMap;
use crate::resources::{NumOfNodes, ResourceRequest};
use crate::{Map, PriorityTuple, TaskId, WorkerId};
use priority_queue::PriorityQueue;

struct QueueForRequest {
    queue: PriorityQueue<TaskId, PriorityTuple>,
    sleeping: bool,
}

impl QueueForRequest {
    pub fn peek(&self) -> Option<(TaskId, PriorityTuple)> {
        self.queue
            .peek()
            .map(|(task_id, priority)| (*task_id, (priority.0, priority.1)))
    }
    pub fn current_priority(&self) -> Option<PriorityTuple> {
        self.peek().map(|x| x.1)
    }
}

#[derive(Default)]
pub(crate) struct MultiNodeQueue {
    queues: Map<ResourceRequest, QueueForRequest>,
    requests: Vec<ResourceRequest>,
}

fn task_priority_tuple(task: &Task) -> PriorityTuple {
    (
        task.configuration.user_priority,
        task.get_scheduler_priority(),
    )
}

impl MultiNodeQueue {
    pub fn shrink_to_fit(&mut self) {
        self.queues.shrink_to_fit();
        self.requests.shrink_to_fit();
    }

    pub fn get_profiles(&self) -> Map<NumOfNodes, u32> {
        let mut result = Map::new();
        for (rq, queue) in &self.queues {
            *result.entry(rq.n_nodes()).or_insert(0) += queue.queue.len() as u32
        }
        result
    }

    pub fn add_task(&mut self, task: &Task) {
        let queue = if let Some(qfr) = self
            .queues
            .get_mut(task.configuration.resources.unwrap_first())
        {
            &mut qfr.queue
        } else {
            self.requests
                .push(task.configuration.resources.unwrap_first().clone());
            self.requests
                .sort_unstable_by_key(|x| std::cmp::Reverse((x.n_nodes(), x.min_time())));
            &mut self
                .queues
                .entry(task.configuration.resources.unwrap_first().clone())
                .or_insert(QueueForRequest {
                    queue: PriorityQueue::new(),
                    sleeping: false,
                })
                .queue
        };
        queue.push(task.id, task_priority_tuple(task));
    }

    pub fn wakeup_sleeping_tasks(&mut self) {
        for queue in self.queues.values_mut() {
            queue.sleeping = false;
        }
    }

    #[cfg(test)]
    pub fn is_sleeping(&self, rq: &ResourceRequest) -> bool {
        self.queues.get(rq).unwrap().sleeping
    }
}

enum TaskFindWorkersResult {
    Ready(Vec<WorkerId>),
    NotReady,
    NoWorkers,
}

pub(crate) struct MultiNodeAllocator<'a> {
    mn_queue: &'a mut MultiNodeQueue,
    task_map: &'a mut TaskMap,
    worker_map: &'a mut WorkerMap,
    worker_groups: &'a Map<String, WorkerGroup>,
    now: std::time::Instant,
}

fn find_workers_for_task(
    request: &ResourceRequest,
    worker_map: &mut WorkerMap,
    worker_groups: &Map<String, WorkerGroup>,
    now: std::time::Instant,
) -> TaskFindWorkersResult {
    let mut suitable_workers_exits = false;
    for group in worker_groups.values() {
        if group.size() < request.n_nodes() as usize {
            continue;
        }
        let mut ready_count = 0;
        let mut not_ready_but_compatible = 0;
        for worker_id in group.worker_ids() {
            let worker = worker_map.get_worker(worker_id);
            if is_compatible_worker(request, worker, now) {
                if worker.is_free() {
                    ready_count += 1;
                    if ready_count == request.n_nodes() {
                        let mut result = Vec::with_capacity(request.n_nodes() as usize);
                        for worker_id in group.worker_ids() {
                            let worker = worker_map.get_worker(worker_id);
                            if worker.is_free() && is_compatible_worker(request, worker, now) {
                                result.push(worker_id);
                                if result.len() == request.n_nodes() as usize {
                                    return TaskFindWorkersResult::Ready(result);
                                }
                            }
                        }
                    }
                } else {
                    not_ready_but_compatible += 1;
                }
            }
        }
        if ready_count + not_ready_but_compatible >= request.n_nodes() {
            suitable_workers_exits = true;
        }
    }
    if suitable_workers_exits {
        TaskFindWorkersResult::NotReady
    } else {
        TaskFindWorkersResult::NoWorkers
    }
}

fn is_compatible_worker(
    request: &ResourceRequest,
    worker: &Worker,
    now: std::time::Instant,
) -> bool {
    if let Some(time) = worker.remaining_time(now) {
        request.min_time() < time
    } else {
        true
    }
}

impl<'a> MultiNodeAllocator<'a> {
    pub fn new(
        mn_queue: &'a mut MultiNodeQueue,
        task_map: &'a mut TaskMap,
        worker_map: &'a mut WorkerMap,
        worker_groups: &'a Map<String, WorkerGroup>,
        now: std::time::Instant,
    ) -> Self {
        MultiNodeAllocator {
            mn_queue,
            task_map,
            worker_map,
            worker_groups,
            now,
        }
    }

    pub fn try_allocate_task(self) -> Option<(TaskId, Vec<WorkerId>)> {
        'outer: loop {
            let current_priority: PriorityTuple = if let Some(Some(priority)) = self
                .mn_queue
                .queues
                .values()
                .filter(|qfr| !qfr.sleeping)
                .map(|qfr| qfr.current_priority())
                .max()
            {
                priority
            } else {
                return None;
            };
            for rq in &self.mn_queue.requests {
                let qfr = self.mn_queue.queues.get_mut(rq).unwrap();
                if qfr.sleeping {
                    continue;
                }
                if let Some((task_id, priority)) = qfr.peek() {
                    if current_priority != priority {
                        continue;
                    }
                    if self.task_map.find_task(task_id).is_none() {
                        qfr.queue.pop();
                        continue;
                    }
                    match find_workers_for_task(rq, self.worker_map, self.worker_groups, self.now) {
                        TaskFindWorkersResult::Ready(workers) => {
                            let task_id = qfr.queue.pop().unwrap().0;
                            return Some((task_id, workers));
                        }
                        TaskFindWorkersResult::NotReady => { /* Do nothing */ }
                        TaskFindWorkersResult::NoWorkers => {
                            qfr.sleeping = true;
                            log::debug!("Multi-node task {:?} put into sleep", rq,);
                            continue 'outer;
                        }
                    }
                }
            }
            return None;
        }
    }
}
