use crate::WorkerId;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::server::task::Task;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::schedule::{create_test_scheduler, finish_on_worker};
use crate::tests::utils::scheduler::TestCase;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;

#[test]
fn test_schedule_mn_simple() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5, 5, 5]);
    let t1 = rt.new_task(&TaskBuilder::new().user_priority(1).n_nodes(2));
    let t2 = rt.new_task(&TaskBuilder::new().user_priority(2).n_nodes(2));
    let t3 = rt.new_task(&TaskBuilder::new().user_priority(3).n_nodes(2));
    let t4 = rt.new_task(&TaskBuilder::new().user_priority(4).n_nodes(2));

    let mut comm = TestComm::new();
    rt.schedule_with_comm(&mut comm);
    rt.sanity_check();

    let test_mn_task = |task: &Task, comm: &mut TestComm| -> Vec<WorkerId> {
        let ws = task.mn_placement().unwrap().to_vec();
        assert_eq!(ws.len(), 2);
        if let ToWorkerMessage::ComputeTasks(m) = &comm.take_worker_msgs(ws[0], 1)[0] {
            assert_eq!(&m.tasks[0].node_list, &ws);
        } else {
            unreachable!()
        }
        ws
    };

    let ws3 = test_mn_task(rt.task(t3), &mut comm);
    let ws4 = test_mn_task(rt.task(t4), &mut comm);
    for w in &ws4 {
        assert!(!ws3.contains(w));
    }
    assert!(rt.task(t2).is_waiting());
    assert!(rt.task(t1).is_waiting());
    comm.emptiness_check();

    finish_on_worker(rt.core(), t3, ws3[0]);
    rt.sanity_check();

    assert!(!rt.task_exists(t3));
    for w in ws3 {
        assert!(
            rt.core()
                .get_worker_by_id_or_panic(w)
                .mn_assignment()
                .is_none()
        );
    }

    rt.schedule_with_comm(&mut comm);
    rt.sanity_check();

    let ws2 = test_mn_task(rt.task(t2), &mut comm);
    comm.emptiness_check();

    finish_on_worker(rt.core(), t3, ws2[0]);
    rt.sanity_check();
}
