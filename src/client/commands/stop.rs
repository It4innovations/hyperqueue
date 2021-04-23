use std::path::PathBuf;

use crate::server::bootstrap::get_client_connection;
use crate::transfer::messages::FromClientMessage;
use crate::transfer::connection::ClientConnection;

pub async fn stop_server(connection: &mut ClientConnection) -> crate::Result<()> {
    connection.send(FromClientMessage::Stop).await?;
    log::info!("Stopping server");
    Ok(())
}
