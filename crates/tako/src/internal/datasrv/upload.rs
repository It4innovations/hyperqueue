use crate::comm::do_authentication;
use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::messages::{FromDataClientMessage, ToDataClientMessage};
use crate::internal::transfer::transport::make_protocol_builder;
use futures::StreamExt;
use orion::kdf::SecretKey;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;

pub(crate) type ToDataClientConnection = Connection<FromDataClientMessage, ToDataClientMessage>;

pub(crate) trait UploadInterface: Clone {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef>;
}

pub(crate) fn start_data_upload_service<I: UploadInterface + 'static>(
    listener: TcpListener,
    secret_key: Option<Arc<SecretKey>>,
    interface: I,
) {
    /*let listener = TcpListener::bind(listen_address).await?;
    let listener_port = listener.local_addr().unwrap().port();*/
    spawn_local(async move {
        loop {
            let (stream, _address) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    log::error!("Accepting data connection failed: {e}");
                    continue;
                }
            };
            let secret_key2 = secret_key.clone();
            let interface2 = interface.clone();
            spawn_local(async move {
                if let Err(e) = handle_data_connection(stream, secret_key2, interface2).await {
                    log::error!("Handling data connection failed: {e}")
                }
            });
        }
    });
}

pub(crate) async fn handle_data_connection<I: UploadInterface>(
    stream: TcpStream,
    secret_key: Option<Arc<SecretKey>>,
    interface: I,
) -> crate::Result<()> {
    let mut connection =
        ToDataClientConnection::init(stream, 0, "data-server", "data-client", secret_key).await?;
    while let Some(message) = connection.receive().await {
        match message? {
            FromDataClientMessage::GetObject { data_id } => {
                connection
                    .send(if let Some(data_obj) = interface.get_object(data_id) {
                        ToDataClientMessage::DataObject(data_obj)
                    } else {
                        ToDataClientMessage::DataObjectNotFound
                    })
                    .await?;
            }
        }
    }
    Ok(())
}
