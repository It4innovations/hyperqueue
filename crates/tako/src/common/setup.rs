use tokio::sync::mpsc::UnboundedReceiver;

pub fn setup_logging() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::builder().format_timestamp_millis().init();
}

pub fn setup_interrupt() -> UnboundedReceiver<()> {
    let (end_tx, end_rx) = tokio::sync::mpsc::unbounded_channel();
    ctrlc::set_handler(move || {
        log::debug!("Received SIGINT, attempting to stop");
        end_tx
            .send(())
            .unwrap_or_else(|_| log::error!("Sending signal failed"))
    })
    .expect("Error setting Ctrl-C handler");
    end_rx
}
