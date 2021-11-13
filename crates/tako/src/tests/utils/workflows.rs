use crate::server::core::Core;
use crate::tests::utils::schedule::submit_test_tasks;
use crate::tests::utils::task;
use task::task_with_deps;

pub fn submit_example_1(core: &mut Core) {
    /*
       11  12 <- keep
        \  / \
         13  14
         /\  /
        16 15 <- keep
        |
        17
    */

    let t1 = task::task(11);
    let t2 = task::task(12);
    t2.get_mut().set_keep_flag(true);
    let t3 = task_with_deps(13, &[&t1, &t2], 1);
    let t4 = task_with_deps(14, &[&t2], 1);
    let t5 = task_with_deps(15, &[&t3, &t4], 1);
    t5.get_mut().set_keep_flag(true);
    let t6 = task_with_deps(16, &[&t3], 1);
    let t7 = task_with_deps(17, &[&t6], 1);
    submit_test_tasks(core, &[&t1, &t2, &t3, &t4, &t5, &t6, &t7]);
}

pub fn submit_example_2(core: &mut Core) {
    /* Graph simple
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5
    */

    let t1 = task_with_deps(1, &[], 1);
    let t2 = task_with_deps(2, &[&t1], 1);
    let t3 = task_with_deps(3, &[&t1], 1);
    let t4 = task_with_deps(4, &[&t2, &t3], 1);
    let t5 = task_with_deps(5, &[&t4], 1);
    let t6 = task_with_deps(6, &[&t3], 1);
    let t7 = task_with_deps(7, &[&t6], 1);

    submit_test_tasks(core, &[&t1, &t2, &t3, &t4, &t5, &t6, &t7]);
}
