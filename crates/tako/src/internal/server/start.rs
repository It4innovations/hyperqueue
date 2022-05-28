use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use orion::aead::SecretKey;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;

use crate::gateway::{ErrorResponse, FromGatewayMessage, ToGatewayMessage};
use crate::internal::scheduler::state::scheduler_loop;
use crate::internal::server::client::process_client_message;
use crate::internal::server::comm::CommSenderRef;
use crate::internal::server::core::{CoreRef, CustomConnectionHandler};

pub struct ServerRef {
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
}

impl ServerRef {
    pub fn get_worker_listen_port(&self) -> u16 {
        self.core_ref.get().get_worker_listen_port()
    }

    pub async fn process_messages(
        self,
        mut receiver: UnboundedReceiver<FromGatewayMessage>,
        sender: UnboundedSender<ToGatewayMessage>,
    ) {
        while let Some(message) = receiver.recv().await {
            let error =
                process_client_message(&self.core_ref, &self.comm_ref, &sender, message).await;
            if let Some(message) = error {
                sender
                    .send(ToGatewayMessage::Error(ErrorResponse { message }))
                    .unwrap();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn split(self) -> (CoreRef, CommSenderRef) {
        (self.core_ref, self.comm_ref)
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn server_start(
    listen_address: SocketAddr,
    secret_key: Option<Arc<SecretKey>>,
    msd: Duration,
    client_sender: UnboundedSender<ToGatewayMessage>,
    panic_on_worker_lost: bool,
    idle_timeout: Option<Duration>,
    custom_conn_handler: Option<CustomConnectionHandler>,
) -> crate::Result<(ServerRef, impl Future<Output = crate::Result<()>>)> {
    log::debug!("Waiting for workers on {:?}", listen_address);
    let listener = TcpListener::bind(listen_address).await?;
    let listener_port = listener.local_addr().unwrap().port();

    let scheduler_wakeup = Rc::new(Notify::new());

    let comm_ref = CommSenderRef::new(
        scheduler_wakeup.clone(),
        client_sender,
        panic_on_worker_lost,
    );

    let core_ref = CoreRef::new(listener_port, secret_key, idle_timeout, custom_conn_handler);
    let connections = crate::internal::server::rpc::connection_initiator(
        listener,
        core_ref.clone(),
        comm_ref.clone(),
    );

    let scheduler = scheduler_loop(core_ref.clone(), comm_ref.clone(), scheduler_wakeup, msd);

    let future = async move {
        tokio::select! {
            () = scheduler => {},
            r = connections => r?,
        };
        log::debug!("Waiting for scheduler to shut down...");
        log::info!("tako ends");
        Ok(())
    };

    Ok((ServerRef { core_ref, comm_ref }, future))
}
