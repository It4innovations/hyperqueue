use clap::Clap;
use std::path::{Path, PathBuf};
use tokio_util::codec::Decoder;
use tokio::net::UnixStream;
use hyperqueue::messages::{FromClientMessage, StatsResponse, SubmitResponse, ToClientMessage, SubmitMessage};
use futures::SinkExt;
use tokio::stream::StreamExt;
use serde::Deserialize;
use bytes::BytesMut;
use std::error::Error;
use hyperqueue::common::protocol::make_protocol_builder;
use hyperqueue::client::commands::{print_job_stats};
use hyperqueue::tako::common::ProgramDefinition;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Connection = tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>;

#[derive(Clap)]
#[clap(version = "1.0")]
struct Opts {
    #[clap(long)]
    server_socket: Option<PathBuf>,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

/*#[derive(Clap)]
struct StatsOpts {

}*/

#[derive(Clap)]
struct SubmitOpts {
    commands: Vec<String>,
}

#[derive(Clap)]
enum SubCommand {
    Stats,
    Submit(SubmitOpts)
}

fn handle_server_error(message: &str) -> ! {
    eprintln!("Server error: {}", message);
    std::process::exit(1);
}

async fn send_and_receive(connection: &mut Connection, message: FromClientMessage) -> BytesMut {
    let msg_data = rmp_serde::to_vec_named(&message).unwrap();
    connection.send(msg_data.into()).await.unwrap_or_else(|e| handle_server_error(&format!("Sending a message to the server failed {}", e)));
    connection.next().await.unwrap_or_else(|| handle_server_error("Unexpected end of the connection")).unwrap()
}

async fn command_stats(connection: &mut Connection) {
    let message = FromClientMessage::Stats;
    let data = send_and_receive(connection, message).await;
    let response : ToClientMessage = rmp_serde::from_slice(&data).unwrap();
    match response {
        ToClientMessage::StatsResponse(stats) => {
            print_job_stats(stats.jobs);
        }
        ToClientMessage::Error(e) => { handle_server_error(&e); }
        _ => { handle_server_error("Received an invalid message"); }
    }
}


async fn command_submit(cmd_opts: SubmitOpts, connection: &mut Connection) {
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
    let data = send_and_receive(connection, message).await;
    let response : ToClientMessage = rmp_serde::from_slice(&data).unwrap();
    match response {
        ToClientMessage::SubmitResponse(sr) => {
            print_job_stats(vec![sr.job]);
        }
        ToClientMessage::Error(e) => { handle_server_error(&e); }
        _ => { handle_server_error("Received an invalid message"); }
    }
}


#[tokio::main(basic_scheduler)]
async fn main() {
    let opts: Opts = Opts::parse();

    let server_socket = opts.server_socket.unwrap_or("hq-server.socket".into());
    let mut protocol = match UnixStream::connect(&server_socket).await {
        Ok(socket) => make_protocol_builder().new_framed(socket),
        Err(e) => {
            eprintln!("Cannot connect to socket {}: {}", server_socket.display(), e);
            std::process::exit(1);
        }
    };
    match opts.subcmd {
        SubCommand::Stats => command_stats(&mut protocol).await,
        SubCommand::Submit(o) => command_submit(o, &mut protocol).await,
    }
}