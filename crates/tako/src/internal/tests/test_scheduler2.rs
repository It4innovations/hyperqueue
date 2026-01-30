use crate::internal::common::resources::ResourceVec;
use crate::internal::scheduler2::{
    PriorityCut, TaskBatch, TaskQueue, WorkerTaskMapping, create_task_batches,
};
use crate::internal::server::core::Core;
use crate::internal::server::task::Task;
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::Worker;
use crate::resources::{ResourceAmount, ResourceRqId, ResourceRqMap};
use crate::tests::utils::env::{TestEnv, create_test_comm};
use crate::tests::utils::schedule::{create_test_scheduler, submit_test_tasks};
use crate::tests::utils::sorted_vec;
use crate::tests::utils::task::TaskBuilder;
use crate::{Priority, ResourceVariantId, TaskGroup, TaskId, WorkerId};
use std::collections::BTreeMap;

#[test]
fn test_task_grouping_basic() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now);
    assert!(a.is_empty());

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let a = create_task_batches(rt.core(), now);
    let task1 = rt.core().get_task(t1);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].resource_rq_id, task1.resource_rq_id);
    assert!(a[0].cuts.is_empty());
    assert_eq!(a[0].size, 1);
    assert!(!a[0].limit_reached);

    let t2 = rt.new_task(&TaskBuilder::new().user_priority(20));
    let t3 = rt.new_task(&TaskBuilder::new().user_priority(5));
    let t4 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let t5 = rt.new_task(&TaskBuilder::new().user_priority(20));

    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 1);
    let r1 = rt.task(t1).resource_rq_id;
    assert_eq!(a[0].resource_rq_id, r1);
    assert!(a[0].cuts.is_empty());
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);

    let t6 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let t7 = rt.new_task(&TaskBuilder::new().cpus(123).user_priority(123));
    let t8 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let t9 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));

    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 2);
    let task1 = rt.task(t1);
    let task6 = rt.task(t6);
    assert_eq!(a[0].resource_rq_id, task1.resource_rq_id);
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);
    assert_eq!(
        a[0].cuts,
        vec![PriorityCut {
            size: 2,
            blockers: vec![(ResourceRqId::new(1), Some(3))],
        }]
    );
    assert_eq!(a[1].resource_rq_id, task6.resource_rq_id);
    assert_eq!(a[1].size, 3);
    assert!(!a[1].limit_reached);
    assert_eq!(a[1].cuts, vec![]);
}

#[test]
fn test_task_group_saturation() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let t1 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let t3 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let t4 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let t5 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let t6 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert!(a[0].cuts.is_empty());

    let t10 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(5));
    let t11 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(0));

    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 2);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert_eq!(
        a[0].cuts,
        vec![PriorityCut {
            size: 2,
            blockers: vec![(ResourceRqId::new(1), Some(1))],
        },]
    );
    assert_eq!(a[1].size, 2);
    assert!(!a[1].limit_reached);
    assert_eq!(
        a[1].cuts,
        vec![
            PriorityCut {
                size: 0,
                blockers: vec![(ResourceRqId::new(0), Some(2))],
            },
            PriorityCut {
                size: 1,
                blockers: vec![(ResourceRqId::new(0), None)],
            }
        ]
    );
}

#[test]
fn test_schedule_no_priorities() {
    for (tasks, expected) in [
        (vec![2], vec![(100, vec![1])]), // 1x 2cpus -> all to worker 100
        (vec![2, 2], vec![(100, vec![1, 2])]), // 2x 2cpus -> all to worker 100
        (vec![2, 2, 2], vec![(100, vec![1, 3]), (101, vec![2])]),
        (vec![2, 2, 2, 2], vec![(100, vec![1, 3]), (101, vec![2, 4])]),
        (
            vec![2, 2, 2, 2, 2],
            vec![(100, vec![1, 3]), (101, vec![2, 4])],
        ),
        (vec![2, 3], vec![(100, vec![2]), (101, vec![1])]),
        (vec![4], vec![(100, vec![1])]),
        (vec![4, 4], vec![(100, vec![1]), (101, vec![2])]),
        (vec![5], vec![]),
        (vec![], vec![]),
        (vec![5, 1, 1, 1, 1], vec![(100, vec![2, 3, 4, 5])]),
        (
            vec![5, 1, 1, 1, 1, 1],
            vec![(100, vec![2, 4, 5, 6]), (101, vec![3])],
        ),
        (vec![3, 4, 2], vec![(100, vec![2]), (101, vec![1])]),
    ] {
        let mut rt = TestEnv::new();
        rt.new_workers_cpus(&[4, 4]);
        for (idx, cpus) in tasks.iter().enumerate() {
            let task_id = TaskId::new_test(idx as u32 + 1);
            rt.new_task_cpus(task_id, *cpus);
        }

        let map = rt.schedule_mapping();
        assert_eq!(map.task_to_workers.len(), expected.len());
        todo!()
        /*for (w_id, tasks) in expected {
            assert_eq!(
                map.task_to_workers.get(&WorkerId::new(w_id)).unwrap(),
                &tasks
                    .iter()
                    .copied()
                    .map(TaskId::new_test)
                    .collect::<Vec<_>>()
            );
        }*/
    }
}

// fn res_counts(
//     task_map: &TaskMap,
//     resource_rq_map: &ResourceRqMap,
//     n_resources: usize,
//     task_id: &[(TaskId, ResourceVariantId)],
//     worker: &Worker,
// ) -> ResourceVec<ResourceAmount> {
//     let mut result: ResourceVec<_> = vec![ResourceAmount::ZERO; n_resources].into();
//     for (task_id, variant) in task_id.iter() {
//         let rq = resource_rq_map
//             .get(task_map.get_task(*task_id).resource_rq_id)
//             .get(*variant);
//         for entry in rq.entries() {
//             result[entry.resource_id] = result[entry.resource_id]
//                 + entry
//                     .request
//                     .amount(worker.resources.get(entry.resource_id));
//         }
//     }
//     result
// }
//
// fn normalize_mapping(resource_rq_map: &ResourceRqMap, task_map: &TaskMap, map: &WorkerTaskMapping) {
//     let mut items: Vec<_> = map
//         .task_to_workers
//         .iter()
//         .map(|(w_id, tasks)| (*w_id, tasks.clone()))
//         .collect();
//     items.sort_unstable_by_key(|x| x.0);
//     for (idx, item) in items.iter().enumerate().skip(1) {}
// }

#[must_use]
struct TestCase {
    rt: TestEnv,
    task_id: u32,
}

impl TestCase {
    pub fn new(rt: TestEnv) -> Self {
        TestCase { rt, task_id: 0 }
    }

    pub fn task(mut self, builder: &TaskBuilder) -> Self {
        self.task_id += 1;
        self.rt.new_task(self.task_id, builder);
        self
    }

    // priority + cpu tasks
    pub fn pc_tasks(mut self, tasks: &[(i32, u32)]) -> Self {
        let mut tc = self;
        for (p, c) in tasks {
            tc = tc.task(&TaskBuilder::new().cpus(*c).user_priority(*p));
        }
        tc
    }

    pub fn expect(self, tasks: &[u32]) -> WorkerAssert {
        WorkerAssert::new(self.rt).expect(tasks)
    }

    pub fn check(self) {
        WorkerAssert::new(self.rt).check();
    }
}

#[must_use]
struct WorkerAssert {
    mapping: WorkerTaskMapping,
    worker_id: u32,
    checked: bool,
}

impl WorkerAssert {
    pub fn new(mut rt: TestEnv) -> Self {
        WorkerAssert {
            mapping: rt.schedule_mapping(),
            worker_id: 100,
            checked: false,
        }
    }

    pub fn expect(mut self, tasks: &[u32]) -> Self {
        let worker_tasks = self
            .mapping
            .task_to_workers
            .remove(&WorkerId::new(self.worker_id))
            .unwrap_or_else(|| panic!("Mapping for worker {} not found", self.worker_id))
            .iter()
            .map(|x| x.0)
            .collect::<Vec<_>>();
        let expected_tasks = tasks
            .iter()
            .map(|id| TaskId::new_test(*id))
            .collect::<Vec<_>>();
        assert_eq!(worker_tasks, expected_tasks);
        self.worker_id += 1;
        self
    }

    pub fn check(mut self) {
        self.checked = true;
        if !self.mapping.task_to_workers.is_empty() {
            panic!(
                "Unchecked worker mapping: {:?}",
                self.mapping.task_to_workers
            )
        }
    }
}

impl Drop for WorkerAssert {
    fn drop(&mut self) {
        if !self.checked {
            panic!("Assert created but not checked",)
        }
    }
}

#[test]
fn test_schedule_priorities1() {
    fn setup() -> TestCase {
        let mut rt = TestEnv::new();
        rt.new_workers_cpus(&[4, 4]);
        TestCase::new(rt)
    }

    setup().pc_tasks(&[(1, 2), (1, 2)]).expect(&[1, 2]).check();
    setup().pc_tasks(&[(1, 2), (2, 2)]).expect(&[2, 1]).check();

    setup()
        .pc_tasks(&[(0, 4), (0, 4), (1, 2), (2, 3)])
        .expect(&[4])
        .expect(&[3])
        .check();

    setup()
        .pc_tasks(&[(0, 4), (0, 4), (1, 2), (1, 3)])
        .expect(&[4])
        .expect(&[3])
        .check();

    setup()
        .pc_tasks(&[(1, 4), (1, 4), (1, 2), (1, 3)])
        .expect(&[1])
        .expect(&[2])
        .check();

    setup()
        .pc_tasks(&[(0, 2), (4, 2), (3, 1), (2, 3)])
        .expect(&[3, 4])
        .expect(&[2, 1])
        .check();

    setup()
        .pc_tasks(&[(0, 2), (4, 2), (3, 1), (2, 3)])
        .expect(&[3, 4])
        .expect(&[2, 1])
        .check();

    /*
    //T().w(4)
    // tasks = [(priority, cpus), ...]
    // expected = [(worker_id, task_ids), ...]
    for (tasks, expected) in [
        (vec![(1, 2), (1, 2)], vec![(100, vec![1, 2])]),
        (vec![(1, 2), (2, 2)], vec![(100, vec![2, 1])]),
        (
            vec![(0, 4), (0, 4), (1, 2), (2, 3)],
            vec![(100, vec![4]), (101, vec![3])],
        ),
        (
            vec![(0, 4), (0, 4), (1, 2), (1, 3)],
            vec![(100, vec![4]), (101, vec![3])],
        ),
        (
            vec![(1, 4), (1, 4), (1, 2), (1, 3)],
            vec![(100, vec![1]), (101, vec![2])],
        ),
        (
            vec![(0, 2), (4, 2), (3, 1), (2, 3)],
            vec![(100, vec![3, 4]), (101, vec![2, 1])],
        ),
        (vec![(1, 5), (0, 4)], vec![(100, vec![2])]),
        (
            vec![(1, 3), (1, 3), (1, 3), (2, 4), (0, 1)],
            vec![(100, vec![])],
        ),
    ] {
        let mut rt = TestEnv::new();
        rt.new_workers_cpus(&[4, 4]);
        for (idx, (p, cpus)) in tasks.iter().enumerate() {
            let task_id = TaskId::new_test(idx as u32 + 1);
            rt.new_task(task_id, &TaskBuilder::new().cpus(*cpus).user_priority(*p));
        }

        let map = rt.schedule_mapping();
        dbg!(&map);
        assert_eq!(map.task_to_workers.len(), expected.len());
        for (w_id, assigned) in expected {
            assert_eq!(
                &map.task_to_workers
                    .get(&WorkerId::new(w_id))
                    .unwrap()
                    .iter()
                    .map(|(t, _)| *t)
                    .collect::<Vec<_>>(),
                &assigned
                    .iter()
                    .copied()
                    .map(TaskId::new_test)
                    .collect::<Vec<_>>()
            );
        }
    }*/
}

#[test]
fn test_schedule_priorities2() {
    // tasks = [(priority, cpus), ...]
    // expected = [[(worker_id, task_ids)...], ...]
    for (tasks, expected) in [
        (
            vec![(0, 2), (4, 2), (3, 1), (2, 3)],
            vec![
                vec![(100, vec![3, 4]), (101, vec![2, 1])],
                vec![(100, vec![3, 4]), (101, vec![2, 1])],
            ],
        ),
        (
            vec![(0, 2), (4, 2), (2, 4)],
            vec![
                vec![(100, vec![1, 2]), (101, vec![3])],
                vec![(100, vec![3]), (101, vec![2, 1])],
            ],
        ),
        (
            vec![(0, 3), (4, 3), (2, 4), (0, 3)],
            vec![vec![(100, vec![3]), (101, vec![2])]],
        ),
    ] {
        let mut rt = TestEnv::new();
        rt.new_workers_cpus(&[4, 4]);
        for (idx, (p, cpus)) in tasks.iter().enumerate() {
            let task_id = TaskId::new_test(idx as u32 + 1);
            rt.new_task(task_id, &TaskBuilder::new().cpus(*cpus).user_priority(*p));
        }

        let map = rt.schedule_mapping();
        dbg!(&expected);
        dbg!(&map);
        assert!(expected.iter().any(|e| {
            assert_eq!(map.task_to_workers.len(), e.len());
            e.iter().all(|(w_id, assigned)| {
                &map.task_to_workers
                    .get(&WorkerId::new(*w_id))
                    .unwrap()
                    .iter()
                    .map(|(t, _)| *t)
                    .collect::<Vec<_>>()
                    == &assigned
                        .iter()
                        .copied()
                        .map(TaskId::new_test)
                        .collect::<Vec<_>>()
            })
        }))
    }
}

#[test]
fn test_schedule_priorities3() {
    // tasks = [(priority, cpus), ...]
    // expected = [[(worker_id, task_ids)...], ...]
    for (tasks, expected) in [
        (
            vec![(0, 2), (4, 2), (3, 1), (2, 3)],
            vec![
                vec![(100, vec![3, 4]), (101, vec![2, 1])],
                vec![(100, vec![3, 4]), (101, vec![2, 1])],
            ],
        ),
        (
            vec![(0, 2), (4, 2), (2, 4)],
            vec![
                vec![(100, vec![1, 2]), (101, vec![3])],
                vec![(100, vec![3]), (101, vec![2, 1])],
            ],
        ),
        (
            vec![(0, 3), (4, 3), (2, 4), (0, 3)],
            vec![vec![(100, vec![3]), (101, vec![2])]],
        ),
    ] {
        let mut rt = TestEnv::new();
        rt.new_workers_cpus(&[4, 4]);
        for (idx, (p, cpus)) in tasks.iter().enumerate() {
            let task_id = TaskId::new_test(idx as u32 + 1);
            rt.new_task(task_id, &TaskBuilder::new().cpus(*cpus).user_priority(*p));
        }

        let map = rt.schedule_mapping();
        assert!(expected.iter().any(|e| {
            assert_eq!(map.task_to_workers.len(), e.len());
            e.iter().all(|(w_id, assigned)| {
                &map.task_to_workers
                    .get(&WorkerId::new(*w_id))
                    .unwrap()
                    .iter()
                    .map(|(t, _)| *t)
                    .collect::<Vec<_>>()
                    == &assigned
                        .iter()
                        .copied()
                        .map(TaskId::new_test)
                        .collect::<Vec<_>>()
            })
        }))
    }
}
