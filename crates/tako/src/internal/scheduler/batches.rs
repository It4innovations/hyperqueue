use crate::Priority;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::worker::Worker;
use crate::resources::ResourceRqId;
use std::cmp::Ordering;
use std::time::Instant;

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

pub(crate) fn create_task_batches(
    core: &mut Core,
    now: Instant,
    custom_workers: Option<&[Worker]>,
) -> Vec<TaskBatch> {
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        worker_groups,
        ..
    } = core.split_mut();

    let queues: Vec<_> = task_queues
        .iter()
        .enumerate()
        .filter_map(|(idx, q)| (!q.is_empty()).then(|| q))
        .collect();
    if queues.is_empty() {
        return Vec::new();
    }
    let limits: Vec<u32> = queues
        .iter()
        .map(|q| {
            let rqv = request_map.get(q.resource_rq_id);
            if rqv.is_multi_node() {
                let n_nodes = rqv.unwrap_first().n_nodes();
                let n_frees = worker_groups
                    .values()
                    .map(|g| {
                        g.worker_ids()
                            .map(|w_id| {
                                let worker = worker_map.get_worker(w_id);
                                if worker.is_free() { 1 } else { 0 }
                            })
                            .sum::<u32>()
                    })
                    .sum::<u32>();
                n_frees / n_nodes
            } else {
                custom_workers
                    .map(|ws| itertools::Either::Right(ws.iter()))
                    .unwrap_or(itertools::Either::Left(worker_map.get_workers()))
                    .map(|w| {
                        w.sn_assignment()
                            .map(|a| a.free_resources.task_max_count(&rqv))
                            .unwrap_or(0)
                    })
                    .sum::<u32>()
            }
        })
        .collect();

    let mut iters: Vec<_> = queues.iter().map(|q| q.iter_priority_sizes()).collect();
    let mut current: Vec<Option<_>> = iters.iter_mut().map(|it| it.next()).collect();
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
            if let Some((priority, _size)) = c {
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
