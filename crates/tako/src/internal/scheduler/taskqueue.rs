use crate::internal::common::SmallSet;
use crate::internal::server::task::Task;
use crate::internal::server::workermap::WorkerMap;
use crate::resources::{ResourceRqId, ResourceRqMap};
use crate::{Priority, Set, TaskId, WorkerId};
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::btree_map::{Entry, OccupiedEntry};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Debug)]
pub(crate) enum OneOrMoreTaskIds {
    One(TaskId),
    More(Box<BTreeSet<TaskId>>),
}

impl OneOrMoreTaskIds {
    pub fn size(&self) -> u32 {
        match self {
            OneOrMoreTaskIds::One(_) => 1,
            OneOrMoreTaskIds::More(tasks) => tasks.len() as u32,
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct TaskQueues {
    queues: Vec<TaskQueue>,
}

impl TaskQueues {
    pub fn add_task_queue(&mut self) {
        let resource_rq_id = ResourceRqId::new(self.queues.len() as u32);
        self.queues.push(TaskQueue::new(resource_rq_id));
    }

    pub fn add_ready_task(&mut self, task: &Task) {
        self.queues[task.resource_rq_id.as_usize()].add(task);
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskQueue> {
        self.queues.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut TaskQueue> {
        self.queues.iter_mut()
    }

    pub fn get(&self, resource_rq_id: ResourceRqId) -> &TaskQueue {
        &self.queues[resource_rq_id.as_usize()]
    }

    pub fn get_mut(&mut self, resource_rq_id: ResourceRqId) -> &mut TaskQueue {
        &mut self.queues[resource_rq_id.as_usize()]
    }

    pub fn shrink_to_fit(&mut self) {
        for task_queue in self.queues.iter_mut() {
            task_queue.shrink_to_fit();
        }
    }
}

#[derive(Debug)]
pub(crate) struct TaskQueue {
    pub queue: BTreeMap<Reverse<Priority>, OneOrMoreTaskIds>,
    pub resource_rq_id: ResourceRqId,
}

impl TaskQueue {
    pub fn new(resource_rq_id: ResourceRqId) -> Self {
        Self {
            resource_rq_id,
            queue: Default::default(),
        }
    }

    pub fn add(&mut self, task: &Task) {
        match self.queue.entry(Reverse(task.priority())) {
            Entry::Vacant(e) => {
                e.insert(OneOrMoreTaskIds::One(task.id));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                OneOrMoreTaskIds::One(task_id) => {
                    let mut task_ids: BTreeSet<_> = Default::default();
                    task_ids.insert(*task_id);
                    task_ids.insert(task.id);
                    e.insert(OneOrMoreTaskIds::More(Box::new(task_ids)));
                }
                OneOrMoreTaskIds::More(tasks) => {
                    tasks.insert(task.id);
                }
            },
        }
    }

    pub fn remove(&mut self, task_id: TaskId, priority: Priority) {
        match self.queue.entry(Reverse(priority)) {
            Entry::Vacant(_) => {}
            Entry::Occupied(mut e) => match e.get_mut() {
                OneOrMoreTaskIds::One(v) => {
                    assert_eq!(*v, task_id);
                    e.remove();
                }
                OneOrMoreTaskIds::More(tasks) => {
                    tasks.remove(&task_id);
                    if tasks.is_empty() {
                        e.remove();
                    }
                }
            },
        }
    }

    pub fn shrink_to_fit(&mut self) {
        todo!()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline]
    pub fn min_priority(&self) -> Option<Priority> {
        self.queue
            .first_key_value()
            .map(|(&priority, _)| priority.0)
    }

    pub fn iter_priority_sizes(&self) -> impl Iterator<Item = (Priority, u32)> {
        self.queue.iter().map(|(k, v)| (k.0, v.size()))
    }

    pub fn take_tasks(&mut self, mut count: u32) -> Vec<TaskId> {
        let mut result = Vec::with_capacity(count as usize);
        while count > 0 {
            let entry = self.queue.first_entry().unwrap();
            take_from_entry(entry, &mut count, &mut result);
        }
        result
    }

    pub fn take_one(&mut self) -> Option<TaskId> {
        let Some(mut entry) = self.queue.first_entry() else {
            return None;
        };
        match entry.get_mut() {
            OneOrMoreTaskIds::One(x) => {
                let r = *x;
                entry.remove();
                Some(r)
            }
            OneOrMoreTaskIds::More(xs) => {
                let r = xs.pop_first().unwrap();
                if xs.is_empty() {
                    entry.remove();
                }
                Some(r)
            }
        }
    }
}

fn take_from_entry(
    mut entry: OccupiedEntry<Reverse<Priority>, OneOrMoreTaskIds>,
    count: &mut u32,
    result: &mut Vec<TaskId>,
) {
    match entry.get_mut() {
        OneOrMoreTaskIds::One(x) => {
            *count -= 1;
            result.push(*x);
            entry.remove();
        }
        OneOrMoreTaskIds::More(xs) => {
            while *count > 0 {
                if let Some(x) = xs.pop_first() {
                    *count -= 1;
                    result.push(x)
                } else {
                    break;
                }
            }
            if xs.is_empty() {
                entry.remove();
            }
        }
    }
}
