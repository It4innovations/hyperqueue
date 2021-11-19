use tako::server::core::Core;
use tako::server::task::TaskRef;

pub fn create_task(id: u64) -> TaskRef {
    TaskRef::new(id.into(), vec![], Default::default(), 0, false, false)
}

pub fn add_tasks(core: &mut Core, count: usize) -> Vec<TaskRef> {
    let mut tasks = Vec::with_capacity(count);
    for id in 0..count {
        let task = create_task(id as u64);
        core.add_task(task.clone());
        tasks.push(task);
    }
    tasks
}
