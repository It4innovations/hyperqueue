use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::messages::{FromDataClientMessage, ToDataClientMessageUp};
use crate::internal::datasrv::utils::DataObjectDecomposer;
use orion::kdf::SecretKey;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::spawn_local;

pub(crate) type ToDataClientConnection = Connection<FromDataClientMessage, ToDataClientMessageUp>;

pub(crate) trait UploadInterface: Clone {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef>;
}

pub(crate) async fn data_upload_service<I: UploadInterface + 'static>(
    listener: TcpListener,
    secret_key: Option<Arc<SecretKey>>,
    interface: I,
) {
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
                if let Some(data_obj) = interface.get_object(data_id) {
                    log::debug!("Uploading object {}", data_id);
                    let (mut decomposer, first_data) = DataObjectDecomposer::new(data_obj.clone());
                    connection
                        .send(ToDataClientMessageUp::DataObject {
                            mime_type: data_obj.mime_type().to_string(),
                            size: data_obj.size(),
                            data: first_data,
                        })
                        .await?;
                    while let Some(data) = decomposer.next() {
                        log::debug!(
                            "Uploading object part {}, start={}, end={}",
                            data_id,
                            data.start,
                            data.end
                        );
                        connection
                            .send(ToDataClientMessageUp::DataObjectPart(data))
                            .await?;
                    }
                } else {
                    log::debug!("Request for invalid object {}", data_id);
                    connection.send(ToDataClientMessageUp::NotFound).await?;
                }
            }
        }
    }
    Ok(())
}
