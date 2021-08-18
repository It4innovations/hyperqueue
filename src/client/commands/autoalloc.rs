use crate::client::globalsettings::GlobalSettings;
use crate::rpc_call;
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    AutoAllocRequest, AutoAllocResponse, FromClientMessage, ToClientMessage,
};
use clap::Clap;
use cli_table::{print_stdout, Cell, Style, Table};

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(setting = clap::AppSettings::Hidden)] // TODO: remove once autoalloc is ready to be used
pub struct AutoAllocOpts {
    #[clap(subcommand)]
    subcmd: AutoAllocCommand,
}

#[derive(Clap)]
enum AutoAllocCommand {
    /// Display information about autoalloc state
    Info,
}

pub async fn command_autoalloc(
    gsettings: GlobalSettings,
    opts: AutoAllocOpts,
) -> anyhow::Result<()> {
    let connection = get_client_connection(gsettings.server_directory()).await?;
    match opts.subcmd {
        AutoAllocCommand::Info => {
            print_autoalloc_info(gsettings, connection).await?;
        }
    }
    Ok(())
}

async fn print_autoalloc_info(
    gsettings: GlobalSettings,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info);
    let response = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(r)) => r
    )
    .await?;

    let rows = vec![vec![
        "Refresh interval".cell().bold(true),
        humantime::format_duration(response.refresh_interval).cell(),
    ]];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
    Ok(())
}
