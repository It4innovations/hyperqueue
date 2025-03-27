use crate::comm::{do_authentication, open_message, seal_message};
use crate::internal::transfer::transport::make_protocol_builder;
use bytes::{Bytes, BytesMut};
use futures::future::ready;
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use orion::kdf::SecretKey;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Codec = Framed<TcpStream, LengthDelimitedCodec>;

pub struct Connection<ReceiveMsg, SendMsg> {
    writer: SplitSink<Codec, Bytes>,
    reader: SplitStream<Codec>,
    sealer: Option<StreamSealer>,
    opener: Option<StreamOpener>,
    _r: PhantomData<ReceiveMsg>,
    _s: PhantomData<SendMsg>,
}

impl<R: DeserializeOwned, S: Serialize> Connection<R, S> {
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
            None => Err(crate::Error::GenericError(
                "Expected response was not received".into(),
            )),
        }
    }

    pub fn split(
        self,
    ) -> (
        impl Sink<S, Error = crate::Error>,
        impl Stream<Item = crate::Result<R>>,
    ) {
        let Connection {
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

    pub async fn init(
        socket: TcpStream,
        protocol: u32,
        my_role: &'static str,
        peer_role: &'static str,
        key: Option<Arc<SecretKey>>,
    ) -> crate::Result<Self> {
        socket.set_nodelay(true)?;
        let connection = make_protocol_builder().new_framed(socket);
        let (mut tx, mut rx) = connection.split();

        let (sealer, opener) =
            do_authentication(protocol, my_role, peer_role, key, &mut tx, &mut rx).await?;

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

fn serialize_message<S: Serialize>(
    item: S,
    sealer: &mut Option<StreamSealer>,
) -> crate::Result<Bytes> {
    let data = crate::comm::serialize(&item)?;
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
