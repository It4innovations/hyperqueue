use crate::server::event::Event;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{FromClientMessage, StreamEvents, ToClientMessage};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

pub async fn create_data_fetch_process(
    mut session: ClientSession,
    sender: Sender<Vec<Event>>,
) -> anyhow::Result<()> {
    session
        .connection()
        .send(FromClientMessage::StreamEvents(StreamEvents {
            history_only: false,
        }))
        .await?;

    const CAPACITY: usize = 1024;

    let mut events = Vec::with_capacity(CAPACITY);
    let mut tick = tokio::time::interval(Duration::from_millis(500));

    let conn = session.connection();

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !events.is_empty() {
                    sender.send(events).await?;
                    events = Vec::with_capacity(CAPACITY);
                }
            }
            // Hopefully this is cancellation safe...
            message = conn.receive() => {
                let Some(message) = message else { break; };

                let message = message?;
                let ToClientMessage::Event(event) = message else {
                    return Err(anyhow::anyhow!(
                        "Dashboard received unexpected message {message:?}"
                    ));
                };
                events.push(event);
                if events.len() == CAPACITY {
                    sender.send(events).await?;
                    events = Vec::with_capacity(CAPACITY);
                }
            }
        }
    }
    if !events.is_empty() {
        sender.send(events).await?;
    }
    Ok(())
}
