use crate::Priority;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::worker::Worker;
use crate::resources::ResourceRqId;
use std::cmp::Ordering;
use std::time::Instant;

const BATCH_PRUNING_MAX_SIZE: usize = 32;
const BATCH_PRUNING_FIXED_PREFIX: usize = 4;

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
    pub limit: u32,
    pub limit_reached: bool,
    /// True if there is a cut in another batch that may be blocked by this batch
    pub is_blocker: bool,
}

impl TaskBatch {
    pub fn new(resource_rq_id: ResourceRqId, limit: u32, limit_reached: bool) -> Self {
        TaskBatch {
            resource_rq_id,
            cuts: Vec::new(),
            size: 0,
            limit,
            limit_reached,
            is_blocker: false,
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

    let mut batches: Vec<_> = queues
        .iter()
        .map(|q| {
            let rqv = request_map.get(q.resource_rq_id);
            let limit = if rqv.is_multi_node() {
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
                    .filter(|w| w.is_capable_to_run_rqv(&rqv, now))
                    .map(|w| {
                        let runnable = w
                            .sn_assignment()
                            .map(|a| a.free_resources.task_max_count(&rqv))
                            .unwrap_or(0);
                        if runnable > 0 { runnable } else { 1 }
                    })
                    .sum::<u32>()
            };
            TaskBatch::new(q.resource_rq_id, limit, false)
        })
        .collect();

    let mut iters: Vec<_> = queues.iter().map(|q| q.iter_priority_sizes()).collect();
    let mut current: Vec<Option<_>> = iters.iter_mut().map(|it| it.next()).collect();
    let mut unique = None;
    let mut found = Vec::new();

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
                if batches[idx].size > batches[idx].limit {
                    batches[idx].size = batches[idx].limit;
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
                    .iter_mut()
                    .enumerate()
                    .filter(|(i, b)| i != idx && (b.size > 0 || b.limit_reached))
                    .map(|(_, b)| {
                        b.is_blocker = true;
                        (b.resource_rq_id, (!b.limit_reached).then(|| b.size))
                    })
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
                if batches[*idx].size > batches[*idx].limit {
                    batches[*idx].size = batches[*idx].limit;
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
    batches.retain_mut(|b| {
        prune_progressive(
            &mut b.cuts,
            BATCH_PRUNING_FIXED_PREFIX,
            BATCH_PRUNING_MAX_SIZE,
        );
        b.size > 0
    });
    batches
}

fn prune_progressive<T>(vec: &mut Vec<T>, prefix_size: usize, size_limit: usize) {
    let original_len = vec.len();

    if original_len <= size_limit {
        return;
    }

    let remaining_slots = size_limit - prefix_size;

    let mut indices = Vec::with_capacity(size_limit);
    for i in 0..prefix_size {
        indices.push(i);
    }

    // Map the remaining slots using a quadratic function:
    // index = prefix_size + (normalized_step^2 * (source_pool_size - 1))
    let source_pool_size = original_len - prefix_size;
    let mut last = prefix_size - 1;
    for i in 0..remaining_slots {
        let t = i as f64 / (remaining_slots - 1) as f64; // Normalized 0.0 to 1.0
        let mut index = prefix_size + (t * t * (source_pool_size - 1) as f64).round() as usize;
        if index <= last {
            index = last + 1;
        }
        indices.push(index);
        last = index;
    }

    // To prune in-place
    for i in 0..size_limit {
        let target_idx = indices[i];
        vec.swap(i, target_idx);
    }

    vec.truncate(size_limit);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prune_progressive() {
        let mut vec = (0..40).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 100);
        assert_eq!(vec, (0..40).collect::<Vec<_>>());

        let mut vec = (0..1000).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 32);
        assert_eq!(vec.len(), 32);
        assert_eq!(
            vec,
            vec![
                0, 1, 2, 3, 4, 5, 9, 16, 26, 38, 53, 71, 91, 115, 140, 169, 201, 235, 272, 311,
                353, 398, 446, 497, 550, 606, 665, 726, 790, 857, 927, 999
            ]
        );

        let mut vec = (0..40).collect::<Vec<_>>();
        prune_progressive(&mut vec, 4, 32);
        assert_eq!(vec.len(), 32);
        assert_eq!(
            vec,
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 27, 29, 32, 34, 36, 39
            ]
        );
    }
}
