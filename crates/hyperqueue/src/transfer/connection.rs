use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::ready;
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use orion::kdf::SecretKey;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tako::transfer::auth::{do_authentication, open_message, seal_message};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::error::error;
use crate::common::serverdir::AccessRecord;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use crate::transfer::protocol::make_protocol_builder;

type Codec = Framed<TcpStream, LengthDelimitedCodec>;

const COMM_PROTOCOL: u32 = 0;

pub struct HqConnection<ReceiveMsg, SendMsg> {
    writer: SplitSink<Codec, Bytes>,
    reader: SplitStream<Codec>,
    sealer: Option<StreamSealer>,
    opener: Option<StreamOpener>,
    _r: PhantomData<ReceiveMsg>,
    _s: PhantomData<SendMsg>,
}

impl<R: DeserializeOwned, S: Serialize> HqConnection<R, S> {
    pub async fn send(&mut self, item: S) -> crate::Result<()> {
        let data = serialize_message(item, &mut self.sealer)?;
        self.writer.send(data).await?;
        Ok(())
    }
    pub async fn receive(&mut self) -> Option<crate::Result<R>> {
        self.reader.next().await.map(|msg| {
            msg.map_err(|e| e.into())
                .and_then(|m| deserialize_message(Ok(m), &mut self.opener))
        })
    }

    pub async fn send_and_receive(&mut self, item: S) -> crate::Result<R> {
        self.send(item).await?;
        match self.receive().await {
            Some(msg) => msg,
            None => error("Expected response was not received".into()),
        }
    }

    pub fn split(
        self,
    ) -> (
        impl Sink<S, Error = crate::Error>,
        impl Stream<Item = crate::Result<R>>,
    ) {
        let HqConnection {
            reader,
            writer,
            mut sealer,
            mut opener,
            ..
        } = self;

        let sink = writer.with(move |msg| ready(serialize_message(msg, &mut sealer)));

        let stream = reader.map(move |message| deserialize_message(message, &mut opener));

        (sink, stream)
    }

    async fn init(socket: TcpStream, server: bool, key: Arc<SecretKey>) -> crate::Result<Self> {
        let connection = make_protocol_builder().new_framed(socket);
        let (mut tx, mut rx) = connection.split();

        let mut my_role = "hq-server".to_string();
        let mut peer_role = "hq-client".to_string();
        if !server {
            std::mem::swap(&mut my_role, &mut peer_role);
        }

        let (sealer, opener) = do_authentication(
            COMM_PROTOCOL,
            my_role,
            peer_role,
            Some(key),
            &mut tx,
            &mut rx,
        )
        .await?;

        Ok(Self {
            writer: tx,
            reader: rx,
            sealer,
            opener,
            _r: Default::default(),
            _s: Default::default(),
        })
    }
}

pub type ClientConnection = HqConnection<ToClientMessage, FromClientMessage>;
pub type ServerConnection = HqConnection<FromClientMessage, ToClientMessage>;

/// Client -> server connection
impl ClientConnection {
    pub async fn connect_to_server(record: &AccessRecord) -> crate::Result<ClientConnection> {
        let address = format!("{}:{}", record.host(), record.server_port());
        let connection = TcpStream::connect(address).await?;

        let key = record.hq_secret_key().clone();
        HqConnection::init(connection, false, key).await
    }
}

/// Server -> client connection
impl ServerConnection {
    pub async fn accept_client(
        socket: TcpStream,
        key: Arc<SecretKey>,
    ) -> crate::Result<ServerConnection> {
        HqConnection::init(socket, true, key).await
    }
}

fn serialize_message<S: Serialize>(
    item: S,
    sealer: &mut Option<StreamSealer>,
) -> crate::Result<Bytes> {
    let data = tako::transfer::auth::serialize(&item)?;
    Ok(seal_message(sealer, data.into()))
}

fn deserialize_message<R: DeserializeOwned>(
    message: Result<BytesMut, std::io::Error>,
    opener: &mut Option<StreamOpener>,
) -> crate::Result<R> {
    let message = message?;
    let item = open_message(opener, &message)?;
    Ok(item)
}
