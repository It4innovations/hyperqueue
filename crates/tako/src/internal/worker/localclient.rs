use crate::comm::{deserialize, serialize};
use crate::internal::common::error::DsError;
use crate::internal::worker::localcomm::{ConnectionType, IntroMessage};
use bstr::BStr;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::Path;
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct LocalClientConnection {
    stream: Framed<UnixStream, LengthDelimitedCodec>,
}

impl LocalClientConnection {
    pub async fn connect(
        path: &Path,
        token: &BStr,
        connection_type: ConnectionType,
    ) -> crate::Result<Self> {
        log::debug!("Creating local connection to: {}", path.display());
        let stream = UnixStream::connect(path).await?;
        let mut stream =
            crate::internal::worker::localcomm::make_protocol_builder().new_framed(stream);
        let data = serialize(&IntroMessage {
            token: token.into(),
            connection_type,
        })?;
        stream.send(data.into()).await?;
        Ok(LocalClientConnection { stream })
    }

    pub async fn send_message<T: Serialize>(&mut self, message: T) -> crate::Result<()> {
        let data = serialize(&message)?;
        self.stream.send(data.into()).await?;
        Ok(())
    }

    pub async fn read_message<T: DeserializeOwned>(&mut self) -> crate::Result<T> {
        let data = self.stream.next().await;
        Ok(match data {
            Some(data) => {
                let data = data?;
                deserialize(&data)?
            }
            None => {
                return Err(DsError::GenericError(
                    "Unexpected end of stream".to_string(),
                ));
            }
        })
    }
}
