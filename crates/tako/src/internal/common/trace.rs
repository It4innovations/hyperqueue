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

impl Drop for ScopedTimer<'_> {
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
        let _timer = $crate::internal::common::trace::ScopedTimer::new($process, $method);
        $block
    }};
}
