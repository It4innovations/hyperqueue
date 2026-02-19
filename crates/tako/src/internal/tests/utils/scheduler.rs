use crate::gateway::ResourceRequest;
use crate::internal::common::resources::ResourceVec;
use crate::internal::scheduler::{
    PriorityCut, TaskBatch, TaskQueue, WorkerTaskMapping, create_task_batches,
};
use crate::resources::{ResourceAmount, ResourceRqId, ResourceRqMap};
use crate::tests::utils::env::TestEnv;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
use crate::{Map, Priority, ResourceVariantId, Set, TaskGroup, TaskId, WorkerId, WrappedRcRefCell};

type TestEnvRef = WrappedRcRefCell<TestEnv>;

pub(crate) struct TestCase {
    rt: TestEnvRef,
    workers: Vec<TestWorker>,
    checked: bool,
}

impl TestCase {
    pub fn new() -> Self {
        TestCase {
            rt: TestEnvRef::wrap(TestEnv::new()),
            workers: Vec::new(),
            checked: false,
        }
    }

    pub fn w(&mut self, builder: &WorkerBuilder) -> &mut TestWorker {
        let rt = self.rt.clone();
        let w = TestWorker::new(rt, self.rt.get_mut().new_worker(builder));
        self.workers.push(w);
        self.workers.last_mut().unwrap()
    }

    pub fn check(mut self) {
        self.checked = true;
        let mut mapping = self.rt.get_mut().schedule_mapping();

        mapping.dump();

        let eq_classes: Set<u32> = self
            .workers
            .iter()
            .filter_map(|w| w.get_eq_class())
            .collect();
        for eq_class in eq_classes {
            let worker_ids: Vec<_> = self
                .workers
                .iter()
                .filter(|w| w.get_eq_class() == Some(eq_class))
                .map(|w| w.get_worker_id())
                .collect();
            normalize_workers(&mut mapping, &worker_ids);
        }
        for worker in &self.workers {
            worker.check(
                &self.rt.get(),
                mapping
                    .sn_tasks_to_workers
                    .get(&worker.get_worker_id())
                    .as_deref()
                    .map_or(&[], |v| v),
            );
        }
    }

    pub fn resources(self, names: &[&str]) -> Self {
        for name in names {
            self.rt.get_mut().new_named_resource(name);
        }
        self
    }

    pub fn t(&mut self, task_builder: &TaskBuilder) -> TaskId {
        self.rt.get_mut().new_task(task_builder)
    }

    pub fn ts(&mut self, n: u32, task_builder: &TaskBuilder) -> Vec<TaskId> {
        (0..n).map(|_| self.t(task_builder)).collect()
    }

    // priority + cpu tasks
    pub fn c_tasks(&mut self, cpus: &[u32]) -> Vec<TaskId> {
        self.rt.get_mut().new_tasks_cpus(cpus)
    }

    // priority + cpu tasks
    pub fn pc_tasks(&mut self, priority_cpus: &[(i32, u32)]) -> Vec<TaskId> {
        priority_cpus
            .iter()
            .map(|(priority, cpus)| {
                self.rt
                    .get_mut()
                    .new_task(&TaskBuilder::new().cpus(*cpus).user_priority(*priority))
            })
            .collect()
    }
}

pub fn normalize_workers(mapping: &mut WorkerTaskMapping, workers: &[WorkerId]) {
    let mut tasks: Vec<_> = workers
        .iter()
        .map(|w| {
            mapping
                .sn_tasks_to_workers
                .get(w)
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    tasks.sort_unstable();
    for (w, tasks) in workers.iter().zip(tasks.into_iter()) {
        mapping.sn_tasks_to_workers.insert(*w, tasks);
    }
}

enum ExpectWorkerState {
    Empty,
    TaskIds(Vec<(TaskId, ResourceVariantId)>),
    RequestIds(Map<(ResourceRqId, ResourceVariantId), u32>),
}

pub(crate) struct TestWorker {
    rt: WrappedRcRefCell<TestEnv>,
    worker_id: WorkerId,
    expect: ExpectWorkerState,
    eq_class: Option<u32>,
}

impl TestWorker {
    pub fn new(rt: TestEnvRef, worker_id: WorkerId) -> Self {
        TestWorker {
            rt,
            worker_id,
            expect: ExpectWorkerState::Empty,
            eq_class: None,
        }
    }

    pub fn get_eq_class(&self) -> Option<u32> {
        self.eq_class.clone()
    }

    pub fn get_worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub fn eq_class(&mut self, class: u32) -> &mut Self {
        self.eq_class = Some(class);
        self
    }

    pub fn expect_tasks(&mut self, tasks: &[TaskId]) -> &mut Self {
        self.expect = ExpectWorkerState::TaskIds(
            tasks
                .iter()
                .map(|t| (*t, ResourceVariantId::new(0)))
                .collect(),
        );
        self
    }

    pub fn expect_request(&mut self, count: u32, builder: &TaskBuilder) -> &mut Self {
        self.expect_request_v(count, builder, 0)
    }

    pub fn expect_request_v(
        &mut self,
        count: u32,
        builder: &TaskBuilder,
        variant: u8,
    ) -> &mut Self {
        let rq_id = builder.build_resource_rq_id(self.rt.get_mut().core());
        match &self.expect {
            ExpectWorkerState::RequestIds(_) => {}
            _ => self.expect = ExpectWorkerState::RequestIds(Map::new()),
        };

        let map = match &mut self.expect {
            ExpectWorkerState::RequestIds(map) => map,
            _ => unreachable!(),
        };
        *map.entry((rq_id, ResourceVariantId::new(variant)))
            .or_insert(0) += count;
        self
    }

    pub fn check(&self, rt: &TestEnv, tasks: &[(TaskId, ResourceVariantId)]) {
        match &self.expect {
            ExpectWorkerState::Empty => {
                assert_eq!(tasks, &[]);
            }
            ExpectWorkerState::TaskIds(t) => {
                assert_eq!(tasks, t);
            }
            ExpectWorkerState::RequestIds(map) => {
                let mut new_map = Map::new();
                for (task_id, variant) in tasks {
                    let rq_id = rt.task(*task_id).resource_rq_id;
                    *new_map.entry((rq_id, *variant)).or_insert(0) += 1;
                }
                if &new_map != map {
                    panic!(
                        "Unexpected resource requests for workers {}; expects: {:?}, got: {:?}",
                        self.worker_id, map, new_map
                    );
                }
            }
        }
    }

    pub fn running(&mut self, builder: &TaskBuilder) -> &mut Self {
        self.rt.get_mut().new_task_running(builder, self.worker_id);
        self
    }

    pub fn running_c(&mut self, cpus: u32) -> &mut Self {
        self.running(&TaskBuilder::new().cpus(cpus))
    }
}

impl Drop for TestCase {
    fn drop(&mut self) {
        if !self.checked {
            panic!("TestCase created but not checked",)
        }
    }
}
