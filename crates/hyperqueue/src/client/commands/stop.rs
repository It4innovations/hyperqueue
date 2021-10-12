use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::FromClientMessage;

pub async fn stop_server(connection: &mut ClientConnection) -> crate::Result<()> {
    connection.send(FromClientMessage::Stop).await?;
    log::info!("Stopping server");
    Ok(())
}
