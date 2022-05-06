/*pub(crate) async fn forward_queue_to_sink<
    T1,
    T2,
    E,
    S: Sink<T2, Error = E> + Unpin,
    F: Fn(T1) -> T2,
>(
    mut queue: UnboundedReceiver<T1>,
    mut sink: S,
    map_fn: F,
) -> Result<(), E> {
    while let Some(data) = queue.recv().await {
        if let Err(e) = sink.send(map_fn(data)).await {
            log::error!("Forwarding from queue failed");
            return Err(e);
        }
    }
    Ok(())
}*/
