use crate::TaskId;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::tests::utils::env::TestEnv;

pub fn submit_example_1(rt: &mut TestEnv) -> Vec<TaskId> {
    /*
        0   1
        \  / \
         2   3
         /\  /
        5   4
        |
        6
    */
    let t0 = rt.new_task_default();
    let t1 = rt.new_task_default();
    let t2 = rt.new_task(&TaskBuilder::new().task_deps(&[t0, t1]));
    let t3 = rt.new_task(&TaskBuilder::new().task_deps(&[t1]));
    let t4 = rt.new_task(&TaskBuilder::new().task_deps(&[t2, t3]));
    let t5 = rt.new_task(&TaskBuilder::new().task_deps(&[t2]));
    let t6 = rt.new_task(&TaskBuilder::new().task_deps(&[t5]));
    [t0, t1, t2, t3, t4, t5, t6].to_vec()
}

pub fn submit_example_3(rt: &mut TestEnv) -> Vec<TaskId> {
    /* Task deps
         T0   T1
        / |\ /  \
       T2 | T3  T4
         \|    /
          \   /
           T5
    */

    let t0 = rt.new_task_default();
    let t1 = rt.new_task_default();
    let t2 = rt.new_task(&TaskBuilder::new().task_deps(&[t0]));
    let t3 = rt.new_task(&TaskBuilder::new().task_deps(&[t0, t1]));
    let t4 = rt.new_task(&TaskBuilder::new().task_deps(&[t1]));
    let t5 = rt.new_task(&TaskBuilder::new().task_deps(&[t0, t2, t4]));
    [t0, t1, t2, t3, t4, t5].to_vec()
}

pub fn submit_example_4(rt: &mut TestEnv) -> Vec<TaskId> {
    /* Task DATA deps
       T0  T1
        |  |\
        0  0 1
        \ / /
         T2
    */

    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();
    let t3 = rt.new_task(
        &TaskBuilder::new()
            .data_dep(t1, 0)
            .data_dep(t2, 0)
            .data_dep(t2, 1),
    );
    [t1, t2, t3].to_vec()
}
