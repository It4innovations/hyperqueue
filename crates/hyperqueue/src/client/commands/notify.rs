use crate::client::globalsettings::GlobalSettings;
use crate::client::task::NotifyOpts;
use anyhow::{anyhow, bail};
use std::path::Path;
use tako::internal::worker::localclient::{LocalClientConnection, LocalConnectionType};
use tako::internal::worker::notifications::Notification;

const NOTIFY_MESSAGE_LIMIT: usize = 1024;

pub async fn command_task_notify(
    _gsettings: &GlobalSettings,
    opts: NotifyOpts,
) -> anyhow::Result<()> {
    let message = opts.message;
    if message.len() > NOTIFY_MESSAGE_LIMIT {
        bail!(
            "Notify message limit exceeded, message len: {}, limit: {}",
            message.len(),
            NOTIFY_MESSAGE_LIMIT
        );
    }
    let mut connection = get_local_connection(LocalConnectionType::Notifier).await?;
    connection
        .send_message(Notification {
            message: message.as_slice().into(),
        })
        .await?;
    Ok(())
}

pub async fn get_local_connection(
    connection_type: LocalConnectionType,
) -> anyhow::Result<LocalClientConnection> {
    let data_access = std::env::var("HQ_ACCESS_KEY").map_err(|_| {
        anyhow!("HQ_ACCESS_KEY variable not found. Are you running this command inside a task?")
    })?;
    let (path, token) = data_access
        .split_once(':')
        .ok_or_else(|| anyhow!("Value of HQ_ACCESS_KEY has a wrong format".to_string()))?;
    Ok(LocalClientConnection::connect(Path::new(path), token.into(), connection_type).await?)
}
