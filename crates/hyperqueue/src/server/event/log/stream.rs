use crate::common::utils::str::pluralize;
use crate::server::event::log::write::EventLogWriter;
use crate::server::event::payload::EventPayload;
use crate::server::event::Event;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;

pub type EventStreamSender = mpsc::UnboundedSender<Event>;
pub type EventStreamReceiver = mpsc::UnboundedReceiver<Event>;

fn create_event_stream_queue() -> (EventStreamSender, EventStreamReceiver) {
    mpsc::unbounded_channel()
}

/// Start event streaming into a log file.
/// Streaming is running on another thread to reduce overhead and interference.
///
/// Returns a future that resolves once the event streaming thread finishes.
/// The thread will finish if there is some I/O error or if the `receiver` is closed.
pub fn start_event_streaming(
    writer: EventLogWriter,
) -> (EventStreamSender, impl Future<Output = ()>) {
    let (tx, rx) = create_event_stream_queue();

    let handle = std::thread::spawn(move || {
        let process = streaming_process(writer, rx);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        if let Err(error) = runtime.block_on(process) {
            log::error!("Event streaming has ended with an error: {error:?}");
        } else {
            log::debug!("Event streaming has finished successfully");
        }
    });
    let end_fut = async move {
        handle.join().expect("Event streaming thread has crashed");
    };
    (tx, end_fut)
}

const FLUSH_PERIOD: Duration = Duration::from_secs(30);

async fn streaming_process(
    mut writer: EventLogWriter,
    mut receiver: EventStreamReceiver,
) -> anyhow::Result<()> {
    let mut flush_fut = tokio::time::interval(FLUSH_PERIOD);
    let mut events = 0;
    loop {
        tokio::select! {
            _ = flush_fut.tick() => {
                writer.flush()?;
            }
            res = receiver.recv() => {
                match res {
                    Some(event) => {
                        log::trace!("Event: {event:?}");
                        let end = matches!(event.payload, EventPayload::ServerStop);
                        writer.store(event)?;
                        events += 1;
                        if end {
                            writer.flush()?;
                            break
                        }
                    }
                    None => break
                }
            }
        }
    }
    writer.finish()?;

    log::debug!(
        "Written {events} {} into the event log.",
        pluralize("event", events)
    );

    Ok(())
}
