use hyperqueue::common::setup::{setup_logging, setup_interrupt};
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;
use futures::StreamExt;
use clap::Clap;
use std::time::Duration;
use std::thread;
use std::net::{SocketAddr, Ipv4Addr};
use tokio::task::LocalSet;
use hyperqueue::server::state::StateRef;
use hyperqueue::server::rpc::TakoServer;
use std::fs::File;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;


#[derive(Clap)]
#[clap(version = "1.0")]
struct Opts {
    socket_path: PathBuf,

    #[clap(long, default_value = "7760")]  // TODO: Auto-assign of port as default
    worker_port: u16,
}


#[tokio::main(basic_scheduler)]
async fn main() {
    let opts: Opts = Opts::parse();
    setup_logging();

    let msd = Duration::from_millis(0);

    let mut end_rx = setup_interrupt();
    let end_flag = async move {
        end_rx.next().await;
    };

    let mut client_listener = match UnixListener::bind(&opts.socket_path) {
        Ok(listener) => listener,
        Err(e) => {
            log::error!("Cannot create unix socket {:?}: {}", opts.socket_path, e);
            return;
        }
    };

    let local_set = LocalSet::new();
    let state_ref = StateRef::new();

    let tako_log = File::create("tako.log").unwrap();
    let (tako_ref, tako_future) = TakoServer::start(state_ref.clone(), &Path::new("tako.socket"), Some(tako_log), opts.worker_port);

    local_set.run_until(async move {
    tokio::select! {
        _ = end_flag => {},
        () = hyperqueue::server::client::handle_client_connections(state_ref, tako_ref, client_listener) => { }
        r = tako_future => { match r {
            Ok(()) => {}
            Err(e) => {
                log::error!("{}", e);
                std::process::exit(1)
            }
        }},
    }
    }).await;
}
