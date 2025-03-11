use std::sync::Arc;

use crate::common::serverdir::ClientAccessRecord;
use crate::common::utils::network::get_hostname;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use orion::kdf::SecretKey;
use tako::connection::Connection;
use tokio::net::TcpStream;

const COMM_PROTOCOL: u32 = 0;

pub type ClientConnection = Connection<ToClientMessage, FromClientMessage>;
pub type ServerConnection = Connection<FromClientMessage, ToClientMessage>;

pub struct ClientSession {
    connection: ClientConnection,
}

/// Client -> server connection
impl ClientSession {
    pub async fn connect_to_server(record: &ClientAccessRecord) -> crate::Result<ClientSession> {
        let connection = try_connect_to_server(record).await?;

        let key = record.client.secret_key.clone();
        Ok(ClientSession {
            connection: Connection::init(connection, COMM_PROTOCOL, "hq-client", "hq-server", key)
                .await?,
            //server_uid: record.server_uid().to_string(),
        })
    }

    pub fn connection(&mut self) -> &mut ClientConnection {
        &mut self.connection
    }
}

async fn try_connect_to_server(record: &ClientAccessRecord) -> crate::Result<TcpStream> {
    let address = format!("{}:{}", record.client.host, record.client.port);
    if record.client.secret_key.is_none() {
        log::warn!("No client key: Unauthenticated and unencrypted connection to server");
    }
    match TcpStream::connect(&address).await {
        Ok(conn) => Ok(conn),
        // 113 = EHOSTUNREACH on Linux. Replace with ErrorKind::HostUnreachable once it's stabilized
        Err(error) if matches!(error.raw_os_error(), Some(113)) => {
            let hostname = get_hostname(None);
            if hostname == record.client.host {
                let localhost_address = format!("localhost:{}", record.client.port);
                log::debug!(
                    "Could not reach {address}. It's host matches the local hostname, retrying\
with `{localhost_address}`.",
                );
                Ok(TcpStream::connect(localhost_address).await?)
            } else {
                Err(error.into())
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Server -> client connection
pub async fn accept_client(
    socket: TcpStream,
    key: Option<Arc<SecretKey>>,
) -> crate::Result<ServerConnection> {
    Connection::init(socket, COMM_PROTOCOL, "hq-server", "hq-client", key)
        .await
        .map_err(|e| e.into())
}
