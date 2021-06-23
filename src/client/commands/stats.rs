use crate::client::globalsettings::GlobalSettings;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, StatsResponse, ToClientMessage};
use cli_table::{print_stdout, Cell, Style, Table};

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

    let rows = vec![
        vec![
            "Stream connections".cell().bold(true),
            response.stream_stats.connections.join("\n").cell(),
        ],
        vec![
            "Stream registrations".cell().bold(true),
            response
                .stream_stats
                .registrations
                .iter()
                .map(|(job_id, path)| format!("{}: {}", job_id, path.display()))
                .collect::<Vec<_>>()
                .join("\n")
                .cell(),
        ],
    ];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
    Ok(())
}
