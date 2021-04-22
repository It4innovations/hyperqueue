use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use clap::Clap;
use futures::SinkExt;
use serde::Deserialize;
use tokio::net::UnixStream;
use tokio_util::codec::Decoder;

use hyperqueue::client::commands::print_job_stats;
use hyperqueue::common::error::error;
use hyperqueue::common::error::HqError::GenericError;
use hyperqueue::transfer::protocol::make_protocol_builder;
use hyperqueue::common::rundir::{load_runfile, RunDirectory, Runfile, store_runfile};
use hyperqueue::common::setup::setup_logging;
use hyperqueue::transfer::messages::{FromClientMessage, StatsResponse, SubmitMessage, SubmitResponse, ToClientMessage};
use hyperqueue::server::bootstrap::{hyperqueue_start, hyperqueue_stop};
use hyperqueue::server::rpc::TakoServer;
use hyperqueue::server::state::StateRef;
use hyperqueue::utils::absolute_path;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Connection = tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>;

#[derive(Clap)]
#[clap(version = "0.1")]
#[clap(setting = clap::AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
struct CommonOpts {
    #[clap(long)]
    rundir: Option<PathBuf>,
}

impl CommonOpts {
    fn get_rundir(&self) -> PathBuf {
        absolute_path(self.rundir.clone().unwrap_or_else(|| default_rundir()))
    }
}

#[derive(Clap)]
struct StartOpts {
    #[clap(flatten)]
    common: CommonOpts,
    #[clap(long)]
    no_auth: bool
}

#[derive(Clap)]
struct StopOpts {
    #[clap(flatten)]
    common: CommonOpts,
}

#[derive(Clap)]
struct StatsOpts {
    #[clap(flatten)]
    common: CommonOpts,
}

#[derive(Clap)]
struct SubmitOpts {
    #[clap(flatten)]
    common: CommonOpts,
    commands: Vec<String>,
}

#[derive(Clap)]
enum SubCommand {
    Start(StartOpts),
    Stop(StopOpts)
    // Stats(StatsOpts),
    // Submit(SubmitOpts)
}

/*fn handle_server_error(message: &str) -> ! {
    eprintln!("Server error: {}", message);
    std::process::exit(1);
}

async fn send_and_receive(connection: &mut Connection, message: FromClientMessage) -> BytesMut {
    let msg_data = rmp_serde::to_vec_named(&message).unwrap();
    connection.send(msg_data.into()).await.unwrap_or_else(|e| handle_server_error(&format!("Sending a message to the server failed {}", e)));
    connection.next().await.unwrap_or_else(|| handle_server_error("Unexpected end of the connection")).unwrap()
}

async fn create_server_connection(runfile_path: &PathBuf) -> hyperqueue::Result<Connection> {
    let runfile = load_runfile(runfile_path)?;

    /*let server_socket = runfile.server_socket_path();
    match UnixStream::connect(&server_socket).await {
        Ok(socket) => Ok(make_protocol_builder().new_framed(socket)),
        Err(e) => Err(GenericError(format!("Cannot connect to socket {}: {}", server_socket.display(), e)))
    }*/
    return error("test".to_string());
}*/

/*async fn command_stats(runfile_path: PathBuf) -> hyperqueue::Result<()> {
    let mut connection = create_server_connection(&runfile_path).await?;

    let message = FromClientMessage::Stats;
    let data = send_and_receive(&mut connection, message).await;
    let response : ToClientMessage = rmp_serde::from_slice(&data).unwrap();
    match response {
        ToClientMessage::StatsResponse(stats) => {
            print_job_stats(stats.jobs);
        }
        ToClientMessage::Error(e) => { handle_server_error(&e); }
        _ => { handle_server_error("Received an invalid message"); }
    };
    Ok(())
}

async fn command_submit(cmd_opts: SubmitOpts, runfile_path: PathBuf) -> hyperqueue::Result<()> {
    let mut connection = create_server_connection(&runfile_path).await?;

    // TODO: Strip path
    let name = cmd_opts.commands.get(0).map(|t| t.to_string()).unwrap_or_else(|| "job".to_string());

    let message = FromClientMessage::Submit(SubmitMessage {
        name: name.clone(),
        cwd: std::env::current_dir().unwrap(),
        spec: ProgramDefinition {
            args: cmd_opts.commands,
            env: Default::default(),
            stdout: None,
            stderr: None
        }
    });
    let data = send_and_receive(&mut connection, message).await;
    let response : ToClientMessage = rmp_serde::from_slice(&data).unwrap();
    match response {
        ToClientMessage::SubmitResponse(sr) => {
            print_job_stats(vec![sr.job]);
        }
        ToClientMessage::Error(e) => { handle_server_error(&e); }
        _ => { handle_server_error("Received an invalid message"); }
    };
    Ok(())
}*/

async fn command_start(opts: StartOpts) -> hyperqueue::Result<()> {
    let rundir_path = opts.common.get_rundir();
    hyperqueue_start(rundir_path, !opts.no_auth).await
}
async fn command_stop(opts: StopOpts) -> hyperqueue::Result<()> {
    hyperqueue_stop(opts.common.get_rundir()).await
}

fn default_rundir() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(|| std::env::temp_dir());
    home.push(".hq-rundir");
    home
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let opts: Opts = Opts::parse();
    setup_logging();

    let result = match opts.subcmd {
        SubCommand::Start(opts) => command_start(opts).await,
        SubCommand::Stop(opts) => command_stop(opts).await,
        /*SubCommand::Stats(opts) => command_stats(opts.common.get_rundir()).await,
        SubCommand::Submit(opts) => {
            let rundir = opts.common.get_rundir();
            command_submit(opts, rundir).await
        },*/
    };
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }

    Ok(())
}
