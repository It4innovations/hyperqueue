use crate::internal::server::core::Core;
use crate::internal::tests::utils::schedule::submit_test_tasks;
use crate::internal::tests::utils::task;
use crate::internal::tests::utils::task::TaskBuilder;
use task::task_with_deps;

pub fn submit_example_1(core: &mut Core) {
    /*
       11  12
        \  / \
         13  14
         /\  /
        16 15
        |
        17
    */

    let t1 = task::task(11);
    let t2 = task::task(12);
    let t3 = task_with_deps(13, &[&t1, &t2]);
    let t4 = task_with_deps(14, &[&t2]);
    let t5 = task_with_deps(15, &[&t3, &t4]);
    let t6 = task_with_deps(16, &[&t3]);
    let t7 = task_with_deps(17, &[&t6]);
    submit_test_tasks(core, vec![t1, t2, t3, t4, t5, t6, t7]);
}

pub fn submit_example_2(core: &mut Core) {
    /*
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5
    */

    let t1 = task_with_deps(1, &[]);
    let t2 = task_with_deps(2, &[&t1]);
    let t3 = task_with_deps(3, &[&t1]);
    let t4 = task_with_deps(4, &[&t2, &t3]);
    let t5 = task_with_deps(5, &[&t4]);
    let t6 = task_with_deps(6, &[&t3]);
    let t7 = task_with_deps(7, &[&t6]);

    submit_test_tasks(core, vec![t1, t2, t3, t4, t5, t6, t7]);
}

pub fn submit_example_3(core: &mut Core) {
    /* Task deps
         T1   T2
        / |\ /  \
       T3 | T4  T5
         \|    /
          \   /
           T6
    */

    let t1 = TaskBuilder::new(1).task_deps(&[]).build();
    let t2 = TaskBuilder::new(2).task_deps(&[]).build();
    let t3 = TaskBuilder::new(3).task_deps(&[&t1]).build();
    let t4 = TaskBuilder::new(4).task_deps(&[&t1, &t2]).build();
    let t5 = TaskBuilder::new(5).task_deps(&[&t2]).build();
    let t6 = TaskBuilder::new(6).task_deps(&[&t1, &t5, &t3]).build();

    submit_test_tasks(core, vec![t1, t2, t3, t4, t5, t6]);
}
