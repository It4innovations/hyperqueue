use crate::internal::server::task::TaskRuntimeState;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::scheduler::TestCase;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;

#[test]
fn test_schedule_apply_mapping() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(5);
    rt.new_task(&TaskBuilder::new().cpus(5));
    let mut comm = TestComm::new();
    rt.schedule_with_comm(&mut comm);
    comm.take_worker_msgs(w1, 1);
    comm.emptiness_check();
}

#[test]
fn test_schedule_mapping() {
    let mut rt = TestEnv::new();
    rt.new_named_resource("gpus");
    let w1 = rt.new_worker(&WorkerBuilder::new(6).res_sum("gpus", 2));
    let w2 = rt.new_worker_cpus(3);
    let t1 = rt.new_task(&TaskBuilder::new().cpus(5));

    rt.schedule();

    match rt.task(t1).state {
        TaskRuntimeState::Assigned(w) => {
            assert_eq!(w, w1);
        }
        _ => unreachable!(),
    }
    rt.worker(w1)
        .sn_assignment()
        .unwrap()
        .assign_tasks
        .contains_key(&t1);

    let m = rt.schedule_mapping();
    assert!(m.sn_steals.is_empty());
    assert!(m.sn_tasks_to_workers.is_empty());

    let w3 = rt.new_worker_cpus(5);
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).add_resource(1, 2));
    let m = rt.schedule_mapping();
    assert!(m.sn_steals.is_empty());
    dbg!(&m);
    assert!(m.sn_tasks_to_workers.is_empty());
}
