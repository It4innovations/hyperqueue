use crate::client::globalsettings::GlobalSettings;
use crate::common::error::HqError;
use clap::Parser;
use std::path::{Path, PathBuf};
use tako::datasrv::{DataInputId, LocalDataClient, OutputId};

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
    let data_access = std::env::var("HQ_DATA_ACCESS").map_err(|_| {
        HqError::GenericError("HQ_DATA_ACCESS variable not found. Are you running this command inside a task with data layer enabled?".to_string())
    })?;
    let (path, token) = data_access.split_once(':').ok_or_else(|| {
        HqError::GenericError("Value of HQ_DATA_ACCESS has a wrong format".to_string())
    })?;
    Ok(LocalDataClient::connect(Path::new(path), token.into()).await?)
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
