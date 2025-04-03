use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::messages::{FromDataClientMessage, ToDataClientMessage};
use crate::internal::datasrv::DataObjectRef;
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub(crate) type ToDataClientConnection = Connection<FromDataClientMessage, ToDataClientMessage>;

trait UploadInterface {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef>;
}

pub(crate) async fn start_upload_service<I: UploadInterface>(
    listen_address: SocketAddr,
    interface: I,
) -> u16 {
    let listener = TcpListener::bind(listen_address).await?;
    let listener_port = listener.local_addr().unwrap().port();
}
