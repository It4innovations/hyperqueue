use std::fmt;
use std::fmt::{Arguments, Write};
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::FmtSubscriber;

use crate::messages::common::WorkerConfiguration;
use crate::TaskId;
use crate::WorkerId;

pub struct ScopedTimer<'a> {
    process: &'a str,
    method: &'static str,
}

impl<'a> ScopedTimer<'a> {
    pub fn new(process: &'a str, method: &'static str) -> Self {
        tracing::info!(
            action = "measure",
            process = process,
            method = method,
            event = "start"
        );
        Self { process, method }
    }
}

impl<'a> Drop for ScopedTimer<'a> {
    fn drop(&mut self) {
        tracing::info!(
            action = "measure",
            method = self.method,
            process = self.process,
            event = "end"
        );
    }
}

macro_rules! trace_time {
    ($process:tt, $method:tt, $block:expr) => {{
        let _timer = $crate::common::trace::ScopedTimer::new($process, $method);
        $block
    }};
}

#[inline(always)]
pub fn trace_task_new(task_id: TaskId, inputs: impl Iterator<Item = u64>) {
    let make_inputs = || {
        let (_, bound) = inputs.size_hint();
        let mut input_str = String::with_capacity(5 * bound.unwrap());
        for input in inputs {
            write!(input_str, "{},", input).ok();
        }
        input_str
    };

    tracing::info!(
        action = "task",
        event = "create",
        task = task_id.as_num(),
        inputs = make_inputs().as_str()
    );
}
#[inline(always)]
pub fn trace_task_new_finished(task_id: TaskId, size: u64, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "create",
        task = task_id.as_num(),
        worker = worker_id.as_num(),
        size = size
    );
}
#[inline(always)]
pub fn trace_task_assign(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "assign",
        worker = worker_id.as_num(),
        task = task_id.as_num()
    );
}
#[inline(always)]
pub fn trace_task_send(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "send",
        task = task_id.as_num(),
        worker = worker_id.as_num(),
    );
}
#[inline(always)]
pub fn trace_task_place(task_id: TaskId, worker_id: WorkerId) {
    tracing::info!(
        action = "task",
        event = "place",
        task = task_id.as_num(),
        worker = worker_id.as_num(),
    );
}
#[inline(always)]
pub fn trace_task_finish(task_id: TaskId, worker_id: WorkerId, size: u64, duration: (u64, u64)) {
    tracing::info!(
        action = "task",
        event = "finish",
        task = task_id.as_num(),
        worker = worker_id.as_num(),
        start = duration.0,
        stop = duration.1,
        size = size
    );
}
#[inline(always)]
pub fn trace_task_remove(task_id: TaskId) {
    tracing::info!(action = "task", event = "remove", task = task_id.as_num(),);
}
#[inline(always)]
pub fn trace_worker_new(worker_id: WorkerId, configuration: &WorkerConfiguration) {
    tracing::info!(
        action = "new-worker",
        worker_id = worker_id.as_num(),
        resources = format!("{:?}", &configuration.resources).as_str(),
        address = configuration.listen_address.as_str(),
    );
}
#[inline(always)]
pub fn trace_worker_steal(task_id: TaskId, from: WorkerId, to: WorkerId) {
    tracing::info!(
        action = "steal",
        task = task_id.as_num(),
        from = from.as_num(),
        to = to.as_num()
    );
}
#[inline(always)]
pub fn trace_worker_steal_response(task_id: TaskId, from: WorkerId, to: WorkerId, result: &str) {
    tracing::info!(
        action = "steal-response",
        task = task_id.as_num(),
        from = from.as_num(),
        to = to.as_num(),
        result = result
    );
}
#[inline(always)]
pub fn trace_worker_steal_response_missing(task_id: TaskId, from: WorkerId) {
    tracing::info!(
        action = "steal-response",
        task = task_id.as_num(),
        from = from.as_num(),
        to = 0,
        result = "missing"
    );
}
#[inline(always)]
pub fn trace_packet_send(size: usize) {
    tracing::info!(action = "packet-send", size = size);
}
#[inline(always)]
pub fn trace_packet_receive(size: usize) {
    tracing::info!(action = "packet-receive", size = size);
}

struct FileGuard(Arc<Mutex<BufWriter<std::fs::File>>>);
impl std::io::Write for FileGuard {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
    }
    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().write_all(buf)
    }
    #[inline]
    fn write_fmt(&mut self, fmt: Arguments<'_>) -> std::io::Result<()> {
        self.0.lock().unwrap().write_fmt(fmt)
    }
}

struct Timestamp;
impl FormatTime for Timestamp {
    fn format_time(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        write!(
            w,
            "{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        )
    }
}

pub fn setup_file_trace(path: String) {
    let file = BufWriter::new(File::create(&path).expect("Unable to create trace file"));
    let file = Arc::new(Mutex::new(file));

    log::info!(
        "Writing trace to {}",
        std::path::PathBuf::from(path)
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
    );

    let make_writer = move || FileGuard(file.clone());

    let subscriber = FmtSubscriber::builder()
        .with_writer(make_writer)
        .json()
        .with_target(false)
        .with_ansi(false)
        .with_timer(Timestamp)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Unable to set global tracing subscriber");
}
