use crate::internal::server::task::Task;
use crate::resources::ResourceRqId;
use crate::{Priority, Set, TaskId};
use itertools::Either;
use std::cmp::Reverse;
use std::collections::btree_map::{Entry, OccupiedEntry};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
/// Do not allocate BTreeSet if there is just one task (per priority level)
/// There may be a many tasks but each with different priority
pub(crate) enum OneOrMoreTaskIds {
    One(TaskId),
    More(BTreeSet<TaskId>),
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

    pub fn add_ready_task(&mut self, task: &Task, retracted: &mut Vec<TaskId>) {
        let priority = task.priority();
        for queue in self.queues.iter_mut() {
            queue.check_dispose_prefill(priority, retracted)
        }
        self.get_mut(task.resource_rq_id).add(task.id, priority);
    }

    pub fn iter(&self) -> impl Iterator<Item = &TaskQueue> {
        self.queues.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut TaskQueue> {
        self.queues.iter_mut()
    }

    pub fn get_mut(&mut self, resource_rq_id: ResourceRqId) -> &mut TaskQueue {
        &mut self.queues[resource_rq_id.as_usize()]
    }

    #[cfg(test)]
    pub fn get(&self, resource_rq_id: ResourceRqId) -> &TaskQueue {
        &self.queues[resource_rq_id.as_usize()]
    }

    pub fn top_priority(&self) -> Priority {
        self.queues
            .iter()
            .filter_map(|q| q.top_priority())
            .max()
            .unwrap_or(Priority::new(0))
    }

    pub fn shrink_to_fit(&mut self) {
        for task_queue in self.queues.iter_mut() {
            task_queue.shrink_to_fit();
        }
    }

    #[cfg(test)]
    pub fn sanity_check(
        &self,
        task_map: &crate::internal::server::taskmap::TaskMap,
        worker_map: &crate::internal::server::workermap::WorkerMap,
    ) {
        for queue in &self.queues {
            for ts in queue.queue.values() {
                match ts {
                    OneOrMoreTaskIds::One(t) => {
                        let task = task_map.get_task(*t);
                        assert!(task.is_waiting())
                    }
                    OneOrMoreTaskIds::More(ts) => {
                        for t in ts.iter() {
                            let task = task_map.get_task(*t);
                            assert!(task.is_waiting() || task.is_retracting())
                        }
                    }
                }
            }
            if let Some((_, ts)) = &queue.prefill {
                for t in ts {
                    let task = task_map.get_task(*t);
                    match &task.state {
                        crate::internal::server::task::TaskRuntimeState::Prefilled {
                            worker_id,
                        } => {
                            let _worker = worker_map.get_worker(*worker_id);
                        }
                        _ => panic!("Invalid task state"),
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct TaskQueue {
    pub resource_rq_id: ResourceRqId,
    pub queue: BTreeMap<Reverse<Priority>, OneOrMoreTaskIds>,
    pub prefill: Option<(Priority, Set<TaskId>)>,
}

impl TaskQueue {
    pub fn new(resource_rq_id: ResourceRqId) -> Self {
        Self {
            resource_rq_id,
            queue: Default::default(),
            prefill: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_ready(&self, task_id: TaskId, priority: Priority) -> bool {
        let Some(item) = self.queue.get(&Reverse(priority)) else {
            return false;
        };
        match item {
            OneOrMoreTaskIds::One(t) => *t == task_id,
            OneOrMoreTaskIds::More(ts) => ts.contains(&task_id),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_prefilled(&self, task_id: TaskId) -> bool {
        self.prefill.as_ref().unwrap().1.contains(&task_id)
    }

    fn check_dispose_prefill(&mut self, priority: Priority, retracted: &mut Vec<TaskId>) {
        if self.prefill.as_ref().is_some_and(|(p, _)| *p < priority) {
            let (p, ts) = self.prefill.take().unwrap();
            self.add_many(&ts, p);
            retracted.extend(ts.iter().copied());
        }
    }

    fn add(&mut self, task_id: TaskId, priority: Priority) {
        match self.queue.entry(Reverse(priority)) {
            Entry::Vacant(e) => {
                e.insert(OneOrMoreTaskIds::One(task_id));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                OneOrMoreTaskIds::One(t_id) => {
                    let mut task_ids: BTreeSet<_> = Default::default();
                    task_ids.insert(*t_id);
                    task_ids.insert(task_id);
                    e.insert(OneOrMoreTaskIds::More(task_ids));
                }
                OneOrMoreTaskIds::More(tasks) => {
                    tasks.insert(task_id);
                }
            },
        }
    }

    fn add_many(&mut self, task_ids: &Set<TaskId>, priority: Priority) {
        if task_ids.is_empty() {
            return;
        }
        match self.queue.entry(Reverse(priority)) {
            Entry::Vacant(e) => {
                e.insert(OneOrMoreTaskIds::More(task_ids.iter().copied().collect()));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                OneOrMoreTaskIds::One(t_id) => {
                    let mut new_ids: BTreeSet<_> = Default::default();
                    new_ids.insert(*t_id);
                    new_ids.extend(task_ids.iter().copied());
                    e.insert(OneOrMoreTaskIds::More(new_ids));
                }
                OneOrMoreTaskIds::More(tasks) => {
                    tasks.extend(task_ids.iter().copied());
                }
            },
        }
    }

    pub fn remove(&mut self, task_id: TaskId, priority: Priority) {
        if let Some((p, ts)) = &mut self.prefill
            && priority == *p
            && ts.remove(&task_id)
        {
            return;
        }
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
        // Do nothing
    }

    pub fn size(&self) -> u32 {
        self.queue.values().map(|v| v.size()).sum()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline]
    pub fn top_priority(&self) -> Option<Priority> {
        self.queue
            .first_key_value()
            .map(|(&priority, _)| priority.0)
    }

    #[inline]
    pub fn top_size_no_prefill(&self) -> u32 {
        let Some((priority, value)) = self.queue.first_key_value() else {
            return 0;
        };
        if let Some((p, _)) = &self.prefill
            && *p != priority.0
        {
            return 0;
        }
        match value {
            OneOrMoreTaskIds::One(_) => 1,
            OneOrMoreTaskIds::More(ts) => ts.len() as u32,
        }
    }

    pub fn remove_prefilled(&mut self, task_id: TaskId) {
        let prefill = self.prefill.as_mut().unwrap();
        assert!(prefill.1.remove(&task_id));
        if prefill.1.is_empty() {
            self.prefill = None;
        }
    }

    pub fn move_prefilled_task_to_ready(&mut self, task_id: TaskId) {
        let prefill = self.prefill.as_mut().unwrap();
        let priority = prefill.0;
        assert!(prefill.1.remove(&task_id));
        if prefill.1.is_empty() {
            self.prefill = None;
        }
        self.add(task_id, priority);
    }

    pub fn iter_priority_sizes(&self) -> impl Iterator<Item = (Priority, u32)> {
        let mut iter = self.queue.iter().map(|(k, v)| (k.0, v.size()));

        match &self.prefill {
            None => Either::Left(iter),
            Some((prefill_priority, prefill_set)) => {
                let prefill_size = prefill_set.len() as u32;
                match iter.next() {
                    Some((first_priority, first_size)) if first_priority == *prefill_priority => {
                        // Same priority: merge into single first element
                        let merged = std::iter::once((first_priority, first_size + prefill_size));
                        Either::Right(Either::Left(merged.chain(iter)))
                    }
                    Some(first) => {
                        // Different priority: prepend prefill, then first, then rest
                        let prefill_item = std::iter::once((*prefill_priority, prefill_size));
                        Either::Right(Either::Right(
                            prefill_item.chain(std::iter::once(first)).chain(iter),
                        ))
                    }
                    None => {
                        // Empty queue: just return prefill
                        Either::Right(Either::Left(
                            std::iter::once((*prefill_priority, prefill_size)).chain(iter),
                        ))
                    }
                }
            }
        }
    }

    pub fn take_tasks_for_prefill(&mut self, mut count: u32) -> Vec<TaskId> {
        let entry = self.queue.first_entry().unwrap();
        let mut result = Vec::with_capacity(count as usize);
        let priority = entry.key().0;
        take_from_entry(entry, &mut count, &mut result);
        if let Some(prefill) = &mut self.prefill {
            assert_eq!(prefill.0, priority);
            for task_id in &result {
                prefill.1.insert(*task_id);
            }
        } else {
            self.prefill = Some((priority, result.iter().copied().collect()))
        }
        result
    }

    pub fn take_tasks(&mut self, mut count: u32) -> Vec<TaskId> {
        let mut result = Vec::with_capacity(count as usize);

        let Some((prefill_priority, _)) = &self.prefill else {
            while count > 0 {
                let entry = self.queue.first_entry().unwrap();
                take_from_entry(entry, &mut count, &mut result);
            }
            return result;
        };
        let prefill_priority = *prefill_priority;

        let queue_top_priority = self.queue.first_key_value().map(|(k, _)| k.0);

        if queue_top_priority == Some(prefill_priority) {
            // Same priority: take from queue's first entry first, then prefill, then rest of queue
            if count > 0 {
                let entry = self.queue.first_entry().unwrap();
                take_from_entry(entry, &mut count, &mut result);
            }
            drain_prefill(&mut self.prefill, &mut count, &mut result);
            while count > 0 {
                let entry = self.queue.first_entry().unwrap();
                take_from_entry(entry, &mut count, &mut result);
            }
        } else {
            // Different priority (or empty queue): take from prefill first, then queue
            drain_prefill(&mut self.prefill, &mut count, &mut result);
            while count > 0 {
                let entry = self.queue.first_entry().unwrap();
                take_from_entry(entry, &mut count, &mut result);
            }
        }

        result
    }

    pub fn take_one(&mut self) -> Option<TaskId> {
        let mut entry = self.queue.first_entry()?;
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

fn drain_prefill(
    prefill: &mut Option<(Priority, Set<TaskId>)>,
    count: &mut u32,
    result: &mut Vec<TaskId>,
) {
    let Some((_, tasks)) = prefill else { return };
    while *count > 0 {
        let Some(&task_id) = tasks.iter().next() else {
            break;
        };
        tasks.remove(&task_id);
        result.push(task_id);
        *count -= 1;
    }
    if tasks.is_empty() {
        *prefill = None;
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
