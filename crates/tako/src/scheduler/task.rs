use crate::common::{HasCycle, Map, Set};
use crate::scheduler::protocol::{NewFinishedTaskInfo, TaskInfo};
use crate::scheduler::worker::WorkerRef;
use crate::TaskId;

#[derive(Debug)]
pub enum SchedulerTaskState {
    Waiting,
    AssignedFresh,
    Assigned,
    AssignedPinned,
    Finished,
}

#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: SchedulerTaskState,
    pub inputs: Vec<TaskRef>,
    pub consumers: Vec<TaskRef>,
    pub computed_metric: i32,
    pub unfinished_deps: u32,
    pub assigned_worker: Option<WorkerRef>,
    pub placement: Set<WorkerRef>,
    pub future_placement: Map<WorkerRef, u32>,
    pub size: u64,
    pub take_flag: bool, // Used in algorithms, no meaning between calls
}

pub type OwningTaskRef = crate::common::CycleOwner<Task>;
pub type TaskRef = crate::common::WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_waiting(&self) -> bool {
        matches!(self.state, SchedulerTaskState::Waiting)
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        matches!(self.state, SchedulerTaskState::Finished)
    }

    #[inline]
    pub fn is_pinned(&self) -> bool {
        matches!(self.state, SchedulerTaskState::AssignedPinned)
    }

    #[inline]
    pub fn is_fresh(&self) -> bool {
        matches!(self.state, SchedulerTaskState::AssignedFresh)
    }

    #[inline]
    pub fn is_assigned(&self) -> bool {
        matches!(
            self.state,
            SchedulerTaskState::AssignedFresh
                | SchedulerTaskState::Assigned
                | SchedulerTaskState::AssignedPinned
        )
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_deps == 0
    }

    pub fn remove_future_placement(&mut self, worker_ref: &WorkerRef) {
        let count = self.future_placement.get_mut(worker_ref).unwrap();
        if *count <= 1 {
            assert_ne!(*count, 0);
            self.future_placement.remove(worker_ref);
        } else {
            *count -= 1;
        }
    }

    #[inline]
    pub fn set_future_placement(&mut self, worker_ref: WorkerRef) {
        (*self.future_placement.entry(worker_ref).or_insert(0)) += 1;
    }

    pub fn sanity_check(&self, task_ref: &TaskRef) {
        let mut unfinished = 0;
        for inp in &self.inputs {
            let ti = inp.get();
            if !ti.is_finished() {
                unfinished += 1;
            }
            assert!(ti.consumers.contains(task_ref));
        }
        assert_eq!(unfinished, self.unfinished_deps);

        match self.state {
            SchedulerTaskState::Waiting => {
                for c in &self.consumers {
                    assert!(c.get().is_waiting());
                }
            }
            SchedulerTaskState::Finished => {
                for inp in &self.inputs {
                    assert!(inp.get().is_finished());
                }
            }
            _ => { /* TODO */ }
        };
    }
}

impl HasCycle for Task {
    #[inline]
    fn clear_cycle(&mut self) {
        // consumers are not cleared, it's enough to break one direction of the cycle
        self.inputs.clear();
        self.assigned_worker = None;
        self.placement.clear();
        self.future_placement.clear();
    }
}

impl OwningTaskRef {
    pub fn new(ti: TaskInfo, inputs: Vec<TaskRef>) -> Self {
        let mut unfinished_deps = 0;
        for inp in &inputs {
            let t = inp.get();
            if !t.is_finished() {
                unfinished_deps += 1;
            }
        }
        let task_ref = Self::wrap(Task {
            id: ti.id,
            inputs,
            state: SchedulerTaskState::Waiting,
            computed_metric: 0,
            unfinished_deps,
            size: 0u64,
            consumers: Default::default(),
            assigned_worker: None,
            placement: Default::default(),
            future_placement: Default::default(),
            take_flag: false,
        });
        {
            let task = task_ref.get();
            for inp in &task.inputs {
                let mut t = inp.get_mut();
                t.consumers.push(task_ref.clone());
            }
        }
        task_ref
    }

    pub fn new_finished(ti: NewFinishedTaskInfo, placement: Set<WorkerRef>) -> Self {
        Self::wrap(Task {
            id: ti.id,
            inputs: Default::default(),
            state: SchedulerTaskState::Finished,
            computed_metric: 0,
            unfinished_deps: Default::default(),
            size: ti.size,
            consumers: Default::default(),
            assigned_worker: None,
            placement,
            future_placement: Default::default(),
            take_flag: false,
        })
    }
}
