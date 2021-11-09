use tako::common::setup::{setup_logging, setup_interrupt};

use tokio::net::UnixListener;
use clap::Clap;
use std::time::Duration;

use std::net::{SocketAddr, Ipv4Addr};
use tokio::task::LocalSet;
use tokio::sync::mpsc::unbounded_channel;
use tako::messages::gateway::ToGatewayMessage;
use tako::server::client::client_connection_handler;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;


#[derive(Clap)]
#[clap(version = "1.0")]
struct Opts {
    socket_path: String,
    #[clap(long, default_value = "7760")]  // TODO: Auto-assign of port as default
    port: u16,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let opts: Opts = Opts::parse();
    setup_logging();

    let listen_address = SocketAddr::new(
        Ipv4Addr::UNSPECIFIED.into(),
        opts.port,
    );
    let msd = Duration::from_millis(20);

    let mut end_rx = setup_interrupt();
    let end_flag = async move {
        end_rx.recv().await;
    };

    let client_listener = UnixListener::bind(opts.socket_path).unwrap();
    let (client_sender, client_receiver) = unbounded_channel::<ToGatewayMessage>();
    let (core_ref, comm_ref, server_future) = tako::server::server_start(listen_address, msd, client_sender.clone()).await.unwrap();
    let client_handler = client_connection_handler(core_ref, comm_ref, client_listener, client_sender, client_receiver);

    let local_set = LocalSet::new();
    local_set.run_until(async move {
    tokio::select! {
        _ = end_flag => {},
        r = server_future => { r.unwrap(); },
        () = client_handler => {}
    }}).await;
}
