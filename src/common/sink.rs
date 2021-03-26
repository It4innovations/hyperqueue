use futures::{Sink, SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn forward_queue_to_sink<T, E, S: Sink<T, Error = E> + Unpin>(
    mut queue: UnboundedReceiver<T>,
    mut sink: S,
) -> Result<(), E> {
    while let Some(data) = queue.next().await {
        if let Err(e) = sink.send(data).await {
            log::error!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}
