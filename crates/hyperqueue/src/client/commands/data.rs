use crate::client::commands::notify::get_local_connection;
use crate::client::globalsettings::GlobalSettings;
use clap::Parser;
use std::path::PathBuf;
use tako::datasrv::{DataInputId, LocalDataClient, OutputId};
use tako::internal::worker::localclient::LocalConnectionType;

#[derive(Parser)]
pub struct DataOpts {
    #[clap(subcommand)]
    subcmd: DataCommand,
}

#[derive(Parser)]
enum DataCommand {
    /// Inside a task, put a data object into the local datanode
    Put(PutOpts),
    /// Inside a task, get an input data object into the local datanode
    Get(GetOpts),
}

#[derive(Parser)]
pub struct PutOpts {
    /// DataId of task output
    data_id: OutputId,
    /// Path of file/directory that should be uploaded
    path: PathBuf,
    /// DataId of task output
    #[arg(long)]
    mime_type: Option<String>,
}

#[derive(Parser)]
pub struct GetOpts {
    /// Input ID
    input_id: DataInputId,
    /// Path of file/directory that should be downloaded
    path: PathBuf,
}

async fn create_local_data_client() -> crate::Result<LocalDataClient> {
    let connection = get_local_connection(LocalConnectionType::Data).await?;
    Ok(LocalDataClient::new(connection))
}

pub async fn command_task_data(_gsettings: &GlobalSettings, opts: DataOpts) -> anyhow::Result<()> {
    match opts.subcmd {
        DataCommand::Put(put_opts) => {
            let mut client = create_local_data_client().await?;
            client
                .put_data_object_from_file(put_opts.data_id, put_opts.mime_type, &put_opts.path)
                .await?;
        }
        DataCommand::Get(get_opts) => {
            let mut client = create_local_data_client().await?;
            client
                .get_input_to_file(get_opts.input_id, &get_opts.path)
                .await?;
        }
    }
    Ok(())
}
