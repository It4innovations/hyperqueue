use crate::client::globalsettings::GlobalSettings;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, StatsResponse, ToClientMessage};

pub async fn print_server_stats(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
) -> anyhow::Result<()> {
    let response: StatsResponse = rpc_call!(
        connection,
        FromClientMessage::Stats,
        ToClientMessage::StatsResponse(r) => r
    )
    .await?;

    gsettings.printer().print_server_stats(response);
    Ok(())
}
