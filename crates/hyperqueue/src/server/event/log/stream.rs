use crate::common::utils::str::pluralize;
use crate::server::event::log::write::EventLogWriter;
use crate::server::event::log::EventLogReader;
use crate::server::event::payload::EventPayload;
use crate::server::event::Event;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

pub enum EventStreamMessage {
    Event(Event),
    RegisterListener(mpsc::UnboundedSender<Event>),
}

pub type EventStreamSender = mpsc::UnboundedSender<EventStreamMessage>;
pub type EventStreamReceiver = mpsc::UnboundedReceiver<EventStreamMessage>;

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
    log_path: &Path,
) -> (EventStreamSender, impl Future<Output = ()>) {
    let (tx, rx) = create_event_stream_queue();
    let log_path = log_path.to_path_buf();
    let handle = std::thread::spawn(move || {
        let process = streaming_process(writer, rx, &log_path);

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
    log_path: &Path,
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
                    Some(EventStreamMessage::Event(event)) => {
                        log::trace!("Event: {event:?}");
                        let end = matches!(event.payload, EventPayload::ServerStop);
                        writer.store(event)?;
                        events += 1;
                        if end {
                            writer.flush()?;
                            break
                        }
                    }
                    Some(EventStreamMessage::RegisterListener(tx)) => {
                        /* We are blocking the thread here, but it is intended.
                           But we are blocking just a thread managing log file, not the whole HQ
                           And while this read is performed, we cannot allow modification of the file,
                           so the writing to the file has to be paused anyway */
                        writer.flush()?;
                        let mut reader = EventLogReader::open(log_path)?;
                        for event in &mut reader {
                            tx.send(event?).unwrap()
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
