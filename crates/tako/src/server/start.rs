use crate::scheduler::{Scheduler, SchedulerComm, drive_scheduler, prepare_scheduler_comm};
use std::time::Duration;
use std::thread;
use crate::server::comm::CommSenderRef;
use crate::server::core::CoreRef;
use crate::server::scheduler::observe_scheduler;
use tokio::net::TcpListener;
use std::net::SocketAddr;
use std::future::Future;
use tokio::sync::mpsc::{UnboundedSender};
use crate::messages::gateway::ToGatewayMessage;


fn start_scheduler<S: Scheduler>(comm: SchedulerComm, scheduler_builder: fn() -> S, msd: Duration) -> thread::JoinHandle<()> where S: 'static{
    thread::spawn(move || {
        let mut runtime = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .expect("Runtime creation failed");
        runtime
            .block_on(drive_scheduler(
                scheduler_builder(),
                comm,
                msd,
            ))
            .expect("Scheduler failed");
    })
}

pub async fn server_start<S: Scheduler>(listen_address: SocketAddr, scheduler_builder: fn() -> S, msd: Duration, client_sender: UnboundedSender<ToGatewayMessage>) -> crate::Result<(CoreRef, CommSenderRef, impl Future<Output=crate::Result<()>>)> where S: 'static {
    log::debug!("Waiting for workers on {:?}", listen_address);
    let listener = TcpListener::bind(listen_address).await?;
    let listener_port = listener.local_addr().unwrap().port();
    let (comm, scheduler_sender, scheduler_receiver) = prepare_scheduler_comm();
    let scheduler_thread = start_scheduler(comm, scheduler_builder, msd);

    let comm_ref = CommSenderRef::new(scheduler_sender, client_sender);
    let core_ref = CoreRef::new();
    core_ref.get_mut().set_worker_listen_port(listener_port);

    let scheduler = observe_scheduler(core_ref.clone(), comm_ref.clone(), scheduler_receiver);
    let connections = crate::server::rpc::connection_initiator(listener, core_ref.clone(), comm_ref.clone());

    let future = async move {
        tokio::select ! {
            r = scheduler => r ?,
            r = connections => r ?,
        };
        log::debug!("Waiting for scheduler to shut down...");
        scheduler_thread.join().expect("Scheduler thread failed");
        log::info!("tako ends");
        Ok(())
    };

    Ok((core_ref, comm_ref, future))
}