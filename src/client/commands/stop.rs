use std::path::PathBuf;

use crate::server::bootstrap::get_client_connection;
use crate::transfer::messages::FromClientMessage;

pub async fn stop_server(rundir_path: PathBuf) -> crate::Result<()> {
    let mut connection = get_client_connection(rundir_path).await?;
    connection.send(FromClientMessage::Stop).await?;
    log::info!("Stopping server");
    Ok(())
}
