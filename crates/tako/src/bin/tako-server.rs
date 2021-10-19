use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::UnixListener;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::LocalSet;

use tako::common::secret::read_secret_file;
use tako::common::setup::{setup_interrupt, setup_logging};
use tako::messages::gateway::ToGatewayMessage;
use tako::server::client::client_connection_handler;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(version = "1.0")]
struct Opts {
    socket_path: String,
    #[clap(long, default_value = "7760")] // TODO: Auto-assign of port as default
    port: u16,

    #[clap(long)]
    panic_on_worker_lost: bool,

    #[clap(long)]
    secret_file: Option<PathBuf>,

    #[clap(long)]
    idle_timeout: Option<u64>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let opts: Opts = Opts::parse();
    setup_logging();

    let secret_key = opts.secret_file.map(|key_file| {
        Arc::new(read_secret_file(&key_file).unwrap_or_else(|e| {
            log::error!("Reading secret file {}: {:?}", key_file.display(), e);
            std::process::exit(1);
        }))
    });

    let listen_address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opts.port);
    let msd = Duration::from_millis(20);

    let mut end_rx = setup_interrupt();
    let end_flag = async move {
        end_rx.recv().await;
    };

    let client_listener = UnixListener::bind(opts.socket_path).unwrap();
    let (client_sender, client_receiver) = unbounded_channel::<ToGatewayMessage>();
    let (core_ref, comm_ref, server_future) = tako::server::server_start(
        listen_address,
        secret_key,
        msd,
        client_sender.clone(),
        opts.panic_on_worker_lost,
        opts.idle_timeout.map(Duration::from_secs),
        None,
    )
    .await
    .unwrap();
    let client_handler = client_connection_handler(
        core_ref,
        comm_ref,
        client_listener,
        client_sender,
        client_receiver,
    );

    let local_set = LocalSet::new();
    local_set
        .run_until(async move {
            tokio::select! {
                _ = end_flag => {},
                r = server_future => { r.unwrap(); },
                () = client_handler => {}
            }
        })
        .await;
}
