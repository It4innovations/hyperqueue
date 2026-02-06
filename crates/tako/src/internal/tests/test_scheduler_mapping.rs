use crate::internal::server::task::TaskRuntimeState;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::scheduler::TestCase;
use crate::tests::utils::task::TaskBuilder;

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
    let w1 = rt.new_worker_cpus(6);
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
        .contains(&t1);

    let m = rt.schedule_mapping();
    dbg!(&m);
}
