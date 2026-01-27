use crate::internal::server::core::Core;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::tests::utils::env::TestEnv;

pub fn submit_example_1(rt: &mut TestEnv) {
    /*
       11  12
        \  / \
         13  14
         /\  /
        16 15
        |
        17
    */
    let t1 = rt.new_task_default(11);
    let t2 = rt.new_task_default(12);
    let t3 = rt.new_task(13, &TaskBuilder::new().task_deps(&[t1, t2]));
    let t4 = rt.new_task(14, &TaskBuilder::new().task_deps(&[t2]));
    let _t5 = rt.new_task(15, &TaskBuilder::new().task_deps(&[t3, t4]));
    let t6 = rt.new_task(16, &TaskBuilder::new().task_deps(&[t3]));
    let _t7 = rt.new_task(17, &TaskBuilder::new().task_deps(&[t6]));
}

pub fn submit_example_3(rt: &mut TestEnv) {
    /* Task deps
         T1   T2
        / |\ /  \
       T3 | T4  T5
         \|    /
          \   /
           T6
    */

    let t1 = rt.new_task_default(1);
    let t2 = rt.new_task_default(2);
    let t3 = rt.new_task(3, &TaskBuilder::new().task_deps(&[t1]));
    let _t4 = rt.new_task(4, &TaskBuilder::new().task_deps(&[t1, t2]));
    let t5 = rt.new_task(5, &TaskBuilder::new().task_deps(&[t2]));
    let _t6 = rt.new_task(6, &TaskBuilder::new().task_deps(&[t1, t5, t3]));
}

pub fn submit_example_4(rt: &mut TestEnv) {
    /* Task DATA deps
       T1  T2
        |  |\
        0  0 1
        \ / /
         T3
    */

    let t1 = rt.new_task_default(1);
    let t2 = rt.new_task_default(2);
    let _t3 = rt.new_task(
        3,
        &TaskBuilder::new()
            .data_dep(t1, 0)
            .data_dep(t2, 0)
            .data_dep(t2, 1),
    );
}
