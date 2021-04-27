use std::sync::Arc;
use tokio::sync::Notify;

pub fn setup_logging() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::builder().format_timestamp_millis().init();
}

pub fn setup_interrupt() -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    let sender = notify.clone();
    ctrlc::set_handler(move || {
        log::debug!("Received SIGINT, attempting to stop");
        sender.notify_one();
    })
    .expect("Error setting Ctrl-C handler");
    notify
}
