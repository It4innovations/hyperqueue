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

    pub fn take_tasks(&mut self, count: u32) -> Vec<TaskId> {
        let count = count as usize;
        let mut result = Vec::with_capacity(count);
        while result.len() < count {
            let mut entry = self.queue.first_entry().unwrap();
            match entry.get_mut() {
                OneOrMoreTaskIds::One(x) => {
                    result.push(*x);
                    entry.remove();
                }
                OneOrMoreTaskIds::More(xs) => {
                    while result.len() < count {
                        if let Some(x) = xs.pop_first() {
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
        result
    }
}
