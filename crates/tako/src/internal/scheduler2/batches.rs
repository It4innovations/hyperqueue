use crate::internal::scheduler2::TaskQueue;
use crate::internal::server::core::Core;
use crate::internal::server::worker::Worker;
use crate::resources::ResourceRqId;
use crate::{Map, Priority, Set, TaskId};
use futures::StreamExt;
use priority_queue::PriorityQueue;
use std::cmp::{Ordering, Reverse, min};
use std::collections::{BTreeMap, BTreeSet};
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

enum MergeIterState {
    Fresh,
    LastFirst(Priority, u32),
    LastSecond(Priority, u32),
    OnlyFirst,
    OnlySecond,
}

struct MergePrioritySizeIterator<T1, T2> {
    iter1: T1,
    iter2: T2,
    state: MergeIterState,
}

impl<T1, T2> MergePrioritySizeIterator<T1, T2> {
    pub fn new(iter1: T1, iter2: T2) -> Self {
        MergePrioritySizeIterator {
            iter1,
            iter2,
            state: MergeIterState::Fresh,
        }
    }
}

impl<T1: Iterator<Item = (Priority, u32)>, T2: Iterator<Item = (Priority, u32)>> Iterator
    for MergePrioritySizeIterator<T1, T2>
{
    type Item = (Priority, u32);
    fn next(&mut self) -> Option<Self::Item> {
        let (a, b) = match self.state {
            MergeIterState::Fresh => (self.iter1.next(), self.iter2.next()),
            MergeIterState::LastFirst(priority, size) => {
                (Some((priority, size)), self.iter2.next())
            }

            MergeIterState::LastSecond(priority, size) => {
                (self.iter1.next(), Some((priority, size)))
            }
            MergeIterState::OnlyFirst => return self.iter1.next(),
            MergeIterState::OnlySecond => return self.iter2.next(),
        };
        dbg!(&a, &b);
        match (a, b) {
            (Some((p1, s1)), Some((p2, s2))) => {
                if p1 == p2 {
                    self.state = MergeIterState::Fresh;
                    Some((p1, s1 + s2))
                } else if p1 < p2 {
                    self.state = MergeIterState::LastSecond(p2, s2);
                    Some((p1, s1))
                } else {
                    self.state = MergeIterState::LastFirst(p1, s1);
                    Some((p2, s2))
                }
            }
            (Some((p1, s1)), None) => {
                self.state = MergeIterState::OnlyFirst;
                Some((p1, s1))
            }
            (None, Some((p2, s2))) => {
                self.state = MergeIterState::OnlySecond;
                Some((p2, s2))
            }
            (None, None) => None,
        }
    }
}

enum Found {
    None,
    Unique(usize),
    Many,
}

pub(crate) fn create_task_batches(
    core: &mut Core,
    assigned_not_running: &[TaskId],
    now: Instant,
) -> Vec<TaskBatch> {
    let (task_map, worker_map, task_queues, resource_map, _) = core.split_all_mut();

    let mut anor_counts: Map<ResourceRqId, BTreeMap<Reverse<Priority>, u32>> = Map::new();

    for task_id in assigned_not_running {
        let task = task_map.get_task(*task_id);
        *anor_counts
            .entry(task.resource_rq_id)
            .or_default()
            .entry(Reverse(task.priority()))
            .or_default() += 1;
    }

    dbg!(&anor_counts);

    let queues: Vec<_> = task_queues
        .iter()
        .enumerate()
        .filter_map(|(idx, q)| {
            if (!q.is_empty() || anor_counts.contains_key(&q.resource_rq_id))
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

    let mut iters: Vec<_> = queues
        .iter()
        .map(|q| {
            MergePrioritySizeIterator::new(
                q.iter_priority_sizes(),
                anor_counts
                    .remove(&q.resource_rq_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k.0, v)),
            )
        })
        .collect();
    let mut current: Vec<Option<_>> = iters.iter_mut().map(|it| it.next()).collect();
    dbg!(&current);
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
