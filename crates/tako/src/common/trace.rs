use std::fmt;
use std::fmt::Arguments;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::FmtSubscriber;

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
