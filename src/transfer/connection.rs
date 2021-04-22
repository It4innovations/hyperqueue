use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;

use crate::transfer::protocol::make_protocol_builder;
use crate::common::rundir::Runfile;
use crate::transfer::messages::FromClientMessage;
use tako::transfer::auth::{do_authentication, seal_message};
use futures::stream::{SplitSink, SplitStream};
use bytes::Bytes;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use orion::aead::streaming::{StreamSealer, StreamOpener};

type Codec = Framed<TcpStream, LengthDelimitedCodec>;

pub struct HqConnection {
    writer: SplitSink<Codec, Bytes>,
    reader: SplitStream<Codec>,
    sealer: Option<StreamSealer>,
    opener: Option<StreamOpener>
}

impl HqConnection {
    pub async fn connect_to_server(runfile: &Runfile) -> crate::Result<Self> {
        let address = format!("{}:{}", runfile.hostname(), runfile.server_port());
        let connection = TcpStream::connect(address).await?;
        let connection = make_protocol_builder().new_framed(connection);
        let (mut tx, mut rx) = connection.split();

        // let (sealer, opener) = do_authentication(None, &mut tx, &mut rx).await?;

        Ok(Self {
            writer: tx,
            reader: rx,
            sealer: None,
            opener: None
        })
    }

    pub async fn send(&mut self, data: Bytes) -> crate::Result<()> {
        let data = seal_message(&mut self.sealer, data);
        self.writer.send(data).await?;
        Ok(())
    }

    pub async fn client_send(&mut self, message: FromClientMessage) -> crate::Result<()> {
        let msg_data = rmp_serde::to_vec_named(&message).unwrap();
        self.writer.send(msg_data.into()).await?;
        Ok(())
    }
}
