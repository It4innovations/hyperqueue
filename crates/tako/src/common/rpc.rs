use futures::{Sink, SinkExt};
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn forward_queue_to_sink<T, E, S: Sink<T, Error = E> + Unpin>(
    mut queue: UnboundedReceiver<T>,
    mut sink: S,
) -> Result<(), E> {
    while let Some(data) = queue.recv().await {
        if let Err(e) = sink.send(data).await {
            log::error!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}

pub async fn forward_queue_to_sink_with_map<T1, T2, E, S: Sink<T2, Error = E> + Unpin, F: Fn(T1) -> T2>(
    mut queue: UnboundedReceiver<T1>,
    mut sink: S,
    function: F
) -> Result<(), E> {
    while let Some(data) = queue.recv().await {
        if let Err(e) = sink.send(function(data)).await {
            log::error!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}
