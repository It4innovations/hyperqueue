use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use orion::aead::SecretKey;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use crate::messages::gateway::ToGatewayMessage;
use crate::scheduler::state::scheduler_loop;
use crate::server::comm::CommSenderRef;
use crate::server::core::{CoreRef, CustomConnectionHandler};

#[allow(clippy::too_many_arguments)]
pub async fn server_start(
    listen_address: SocketAddr,
    secret_key: Option<Arc<SecretKey>>,
    msd: Duration,
    client_sender: UnboundedSender<ToGatewayMessage>,
    panic_on_worker_lost: bool,
    idle_timeout: Option<Duration>,
    custom_conn_handler: Option<CustomConnectionHandler>,
) -> crate::Result<(
    CoreRef,
    CommSenderRef,
    impl Future<Output = crate::Result<()>>,
)> {
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
    let connections =
        crate::server::rpc::connection_initiator(listener, core_ref.clone(), comm_ref.clone());

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

    Ok((core_ref, comm_ref, future))
}
