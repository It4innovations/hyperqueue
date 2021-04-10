#![cfg(test)]

use std::io::Cursor;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::common::{Map, WrappedRcRefCell};
use crate::server::core::Core;

use crate::server::comm::Comm;
use crate::server::worker::{WorkerId, Worker};
use crate::{TaskId, OutputId};
use crate::messages::worker::{ToWorkerMessage, TaskFinishedMsg, StealResponseMsg, StealResponse};
use crate::server::task::TaskRef;
use crate::messages::common::TaskFailInfo;
use crate::scheduler::scheduler::SchedulerState;
use crate::scheduler::scheduler::tests::{create_test_scheduler};
use crate::server::reactor::{on_new_worker, on_new_tasks, on_task_finished, on_steal_response};

/// Memory stream for reading and writing at the same time.
pub struct MemoryStream {
    input: Cursor<Vec<u8>>,
    pub output: WrappedRcRefCell<Vec<u8>>,
}

/*impl MemoryStream {
    pub fn new(input: Vec<u8>) -> (Self, WrappedRcRefCell<Vec<u8>>) {
        let output = WrappedRcRefCell::wrap(Default::default());
        (
            Self {
                input: Cursor::new(input),
                output: output.clone(),
            },
            output,
        )
    }
}*/

impl AsyncRead for MemoryStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.input).poll_read(cx, buf)
    }
}
impl AsyncWrite for MemoryStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(self.output.get_mut().deref_mut()).poll_shutdown(cx)
    }
}

pub fn task(id: TaskId) -> TaskRef {
    task_with_deps(id, &[], 1)
}

pub fn task_with_deps(id: TaskId, deps: &[&TaskRef], n_outputs: OutputId) -> TaskRef {
    let inputs : Vec<TaskRef> = deps.iter().map(|&tr| tr.clone()).collect();

    TaskRef::new(
        id,
        0,
        Vec::new(),
        inputs,
        n_outputs,
        Default::default(),
        false,
        false
    )
}

/*
pub fn load_bin_test_data(path: &str) -> Vec<u8> {
    let path = get_test_path(path);
    std::fs::read(path).unwrap()
}*/

pub fn get_test_path(path: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(path)
        .to_str()
        .unwrap()
        .to_owned()
}

#[derive(Default, Debug)]
pub struct TestComm {
    pub worker_msgs: Map<WorkerId, Vec<ToWorkerMessage>>,
    pub broadcast_msgs: Vec<ToWorkerMessage>,

    pub client_task_finished: Vec<TaskId>,
    pub client_task_errors: Vec<(TaskId, Vec<TaskId>, TaskFailInfo)>,

    pub need_scheduling: bool,
}

impl TestComm {
    pub fn take_worker_msgs(&mut self, worker_id: WorkerId, len: usize) -> Vec<ToWorkerMessage> {
        let msgs = match self.worker_msgs.remove(&worker_id) {
            None => {
                panic!("No messages for worker {}", worker_id)
            }
            Some(x) => x,
        };
        if len != 0 {
            assert_eq!(msgs.len(), len);
        }
        msgs
    }

    pub fn take_broadcasts(&mut self, len: usize) -> Vec<ToWorkerMessage> {
        assert_eq!(self.broadcast_msgs.len(), len);
        std::mem::take(&mut self.broadcast_msgs)
    }

    pub fn take_client_task_finished(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.client_task_finished.len(), len);
        std::mem::take(&mut self.client_task_finished)
    }

    pub fn take_client_task_errors(&mut self, len: usize) -> Vec<(TaskId, Vec<TaskId>, TaskFailInfo)> {
        assert_eq!(self.client_task_errors.len(), len);
        std::mem::take(&mut self.client_task_errors)
    }

    pub fn check_need_scheduling(&mut self) {
        assert!(self.need_scheduling);
        self.need_scheduling = false;
    }

    pub fn emptiness_check(&self) {
        if !self.worker_msgs.is_empty() {
            let ids: Vec<_> = self.worker_msgs.keys().collect();
            panic!("Unexpected worker messages for workers: {:?}", ids);
        }
        assert!(self.broadcast_msgs.is_empty());

        assert!(self.client_task_finished.is_empty());
        assert!(self.client_task_errors.is_empty());

        assert!(!self.need_scheduling);
    }
}

impl Comm for TestComm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        let message = rmp_serde::from_slice(&data).unwrap();
        self.worker_msgs.entry(worker_id).or_default().push(message);
    }

    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        let message = rmp_serde::from_slice(&data).unwrap();
        self.broadcast_msgs.push(message);
    }

    fn ask_for_scheduling(&mut self) {
        self.need_scheduling = true;
    }

    fn send_client_task_finished(&mut self, task_id: TaskId) {
        self.client_task_finished.push(task_id);
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) {
        self.client_task_errors
            .push((task_id, consumers, error_info));
    }
}

pub fn create_test_comm() -> TestComm {
    TestComm::default()
}

pub fn create_test_workers(core: &mut Core, cpus: &[u32]) {
    for (i, c) in cpus.iter().enumerate() {
        let worker_id = (100 + i) as WorkerId;
        let worker = Worker::new(worker_id, *c, format!("test{}:123", i));
        on_new_worker(core, &mut TestComm::default(), worker);
    }
}

pub fn submit_test_tasks(core: &mut Core, tasks: &[&TaskRef]) {
    on_new_tasks(core, &mut TestComm::default(), tasks.iter().map(|&tr| tr.clone()).collect());
}

pub fn force_assign(core: &mut Core, scheduler: &mut SchedulerState, task_id: TaskId, worker_id: WorkerId) {
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    core.remove_from_ready_to_assign(&task_ref);
    let mut task = task_ref.get_mut();
    scheduler.assign(core, &mut task, task_ref.clone(), worker_id);
}

pub fn force_reassign(core: &mut Core, scheduler: &mut SchedulerState, task_id: TaskId, worker_id: WorkerId) {
    // The same as force_assign, but do not expect that task in ready_to_assign array
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    let mut task = task_ref.get_mut();
    scheduler.assign(core, &mut task, task_ref.clone(), worker_id);
}

pub fn fail_steal(core: &mut Core, task_id: TaskId, worker_id: WorkerId, target_worker_id: WorkerId) {
    start_stealing(core, task_id, target_worker_id);
    let mut comm = create_test_comm();
    on_steal_response(core, &mut comm, worker_id, StealResponseMsg { responses: vec![(task_id, StealResponse::Running)] })
}

pub fn start_stealing(core: &mut Core, task_id: TaskId, new_worker_id: WorkerId) {
    let mut scheduler = create_test_scheduler();
    force_reassign(core, &mut scheduler, task_id, new_worker_id);
    let mut comm = create_test_comm();
    scheduler.finish_scheduling(&mut comm);
}

pub fn start_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id, worker_id);
    scheduler.finish_scheduling(&mut comm);
}

pub fn finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId, size: u64) {
    let mut comm = TestComm::default();
    on_task_finished(
        core,
        &mut comm,
        worker_id,
        TaskFinishedMsg {
            id: task_id,
            size,
        },
    );
}

pub fn start_and_finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId, size: u64) {
    start_on_worker(core, task_id, worker_id);
    finish_on_worker(core, task_id, worker_id, size);
}


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

    let t1 = task(11);
    let t2 = task(12);
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

pub fn sorted_vec<T: Ord>(mut vec: Vec<T>) -> Vec<T> {
    vec.sort();
    vec
}