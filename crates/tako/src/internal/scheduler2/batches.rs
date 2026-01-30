use crate::internal::scheduler2::TaskQueue;
use crate::internal::server::core::Core;
use crate::internal::server::worker::Worker;
use crate::resources::ResourceRqId;
use crate::{Priority, TaskId};
use futures::StreamExt;
use priority_queue::PriorityQueue;
use std::cmp::{Ordering, min};
use std::time::Instant;

struct SchedulerState {}

impl SchedulerState {
    pub fn new() -> Self {
        SchedulerState {}
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub(crate) struct PriorityCut {
    pub size: u32,
    pub blockers: Vec<(ResourceRqId, Option<u32>)>,
}

#[derive(Debug)]
pub(crate) struct TaskBatch {
    pub resource_rq_id: ResourceRqId,
    pub cuts: Vec<PriorityCut>,
    pub size: u32,
    pub limit_reached: bool,
}

impl TaskBatch {
    pub fn new(resource_rq_id: ResourceRqId, limit_reached: bool) -> Self {
        TaskBatch {
            resource_rq_id,
            cuts: Vec::new(),
            size: 0,
            limit_reached,
        }
    }
}

pub fn run_scheduling(core: &mut Core, now: std::time::Instant) -> () {
    todo!()

    // let groups: Vec<_> = queues
    //     .iter()
    //     .map(|q| {
    //         let resource = resource_map.get(q.resource_rq_id);
    //         let limit = worker_map
    //             .get_workers()
    //             .map(|w| w.load().estimate_max_count_running(&resource))
    //             .sum();
    //         let mut groups = Vec::new();
    //         let mut count = 0;
    //         for (priority, size) in q.iterate_chunks() {
    //             count += size;
    //             groups.push((priority, count - size));
    //             if count > limit {
    //                 break;
    //             }
    //         }
    //     })
    //     .collect();
}

enum Found {
    None,
    Unique(usize),
    Many,
}

pub(crate) fn create_task_batches(core: &mut Core, now: Instant) -> Vec<TaskBatch> {
    let (task_map, worker_map, task_queues, resource_map, _) = core.split_all();
    let queues: Vec<_> = task_queues
        .iter()
        .enumerate()
        .filter_map(|(idx, q)| {
            if !q.is_empty()
                && worker_map.get_workers().any(|w| {
                    w.is_capable_to_run_rqv(resource_map.get(ResourceRqId::new(idx as u32)), now)
                })
            {
                Some(q)
            } else {
                None
            }
        })
        .collect();
    if queues.is_empty() {
        return Vec::new();
    }
    let limits: Vec<u32> = queues
        .iter()
        .map(|q| {
            let resource = resource_map.get(q.resource_rq_id);
            worker_map
                .get_workers()
                .map(|w| {
                    w.sn_assignment()
                        .unwrap()
                        .free_resources
                        .task_max_count(&resource)
                })
                .sum::<u32>()
        })
        .collect();

    let mut iters: Vec<_> = queues.iter().map(|q| q.iter_priority_sizes()).collect();
    let mut current: Vec<Option<_>> = iters.iter_mut().map(|it| it.next()).collect();
    /*let mut sizes = vec![0usize; iters.len()];
    let mut groups: Vec<TaskBatch> = Vec::new();
    let mut last_priority = Priority::new(u64::MAX);
    let mut is_alone = false;*/
    let mut unique = None;
    let mut found = Vec::new();
    let mut batches: Vec<_> = queues
        .iter()
        .map(|q| TaskBatch::new(q.resource_rq_id, false))
        .collect();
    loop {
        found.clear();
        let mut highest_p = Priority::new(0);
        for (idx, c) in current.iter().enumerate() {
            if let Some((priority, size)) = c {
                match highest_p.cmp(priority) {
                    Ordering::Equal => {
                        found.push(idx);
                    }
                    Ordering::Less => {
                        highest_p = *priority;
                        found.clear();
                        found.push(idx);
                    }
                    Ordering::Greater => { /* Do nothing */ }
                }
            }
        }
        if found.len() == 1 && unique == Some(found[0]) {
            let idx = found[0];
            let size = current[idx].unwrap().1;
            if unique == Some(idx) {
                batches[idx].size += size;
                if batches[idx].size > limits[idx] {
                    batches[idx].size = limits[idx];
                    batches[idx].limit_reached = true;
                    current[idx] = None;
                } else {
                    current[idx] = iters[idx].next();
                }
            }
        } else if found.is_empty() {
            break;
        } else {
            for idx in &found {
                let size = batches[*idx].size;
                let higher_priorities: Vec<_> = batches
                    .iter()
                    .enumerate()
                    .filter(|(i, b)| i != idx && (b.size > 0 || b.limit_reached))
                    .map(|(_, b)| (b.resource_rq_id, (!b.limit_reached).then(|| b.size)))
                    .collect();
                if !higher_priorities.is_empty() {
                    let cut = PriorityCut {
                        size,
                        blockers: higher_priorities,
                    };
                    batches[*idx].cuts.push(cut);
                }
            }
            for idx in &found {
                batches[*idx].size += current[*idx].unwrap().1;
                if batches[*idx].size > limits[*idx] {
                    batches[*idx].size = limits[*idx];
                    batches[*idx].limit_reached = true;
                    current[*idx] = None;
                } else {
                    current[*idx] = iters[*idx].next();
                }
            }
            unique = if found.len() == 1 {
                Some(found[0])
            } else {
                None
            };
        }
    }
    batches.retain(|b| b.size > 0);
    batches
}
