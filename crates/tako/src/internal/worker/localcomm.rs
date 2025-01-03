use crate::internal::datasrv::datanode_connection_handler;
use crate::internal::worker::state::WorkerStateRef;
use crate::{Map, TaskId};
use bstr::{BStr, BString, ByteSlice};
use futures::StreamExt;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::rc::Rc;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::length_delimited::Builder;
use tokio_util::codec::LengthDelimitedCodec;

struct DataConnectionRegistration {
    task_id: TaskId,
}

enum Registration {
    DataConnection(DataConnectionRegistration),
    // TODO: SubworkerConnection
}

pub(crate) struct LocalCommState {
    registered_tokens: Map<BString, Registration>,
}

fn new_token() -> BString {
    "hq0-"
        .bytes()
        .chain(rand::thread_rng().sample_iter(&Alphanumeric).take(12))
        .collect()
}

impl LocalCommState {
    pub fn new() -> Self {
        LocalCommState {
            registered_tokens: Map::new(),
        }
    }
    pub fn register_token(&mut self, registration: Registration) -> BString {
        loop {
            let token = new_token();
            if self.registered_tokens.contains_key(&token) {
                continue;
            }
            self.registered_tokens.insert(token.clone(), registration);
            return token;
        }
    }

    pub fn unregister_token(&mut self, token: &BStr) {
        self.registered_tokens.remove(token);
    }

    pub fn check_token(&self, token: &BStr) -> Option<&Registration> {
        self.registered_tokens.get(token)
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IntroMessage {
    pub token: BString,
}

pub(crate) fn make_protocol_builder() -> Builder {
    *LengthDelimitedCodec::builder().little_endian()
}

async fn handle_connection(state_ref: WorkerStateRef, stream: UnixStream) -> crate::Result<()> {
    let (tx, mut rx) = make_protocol_builder().new_framed(stream).split();
    if let Some(data) = rx.next().await {
        let data = data?;
        let message: IntroMessage = bincode::deserialize(&data)?;
        let state = state_ref.get_mut();
        let registration = state
            .lc_state
            .check_token(message.token.as_bstr())
            .ok_or_else(|| crate::Error::GenericError("Invalid token".to_string()))?;
        match registration {
            Registration::DataConnection(reg) => {
                let task_id = reg.task_id;
                let data_node_ref = state.data_node_ref.clone();
                drop(state);
                datanode_connection_handler(data_node_ref, rx, tx, task_id).await?;
            }
        }
    } else {
        log::debug!("Local connection: closed without providing intro message")
    }
    Ok(())
}

async fn run_local_comm(state_ref: WorkerStateRef, listener: UnixListener) -> crate::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                log::debug!("New local connection via unix socket: {addr:?}");
                let state_ref = state_ref.clone();
                tokio::task::spawn_local(async move {
                    if let Err(err) = handle_connection(state_ref, stream).await {
                        log::error!("Local connection error: {err}");
                    }
                });
            }
            Err(e) => {
                log::debug!("Accepting a new data client via unix socket failed: {e}")
            }
        }
    }
}
