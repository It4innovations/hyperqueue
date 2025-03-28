use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use crate::internal::datasrv::messages::{FromLocalDataClientMessage, ToLocalDataClientMessage};
use crate::internal::datasrv::{DataObject, DataObjectRef};
use crate::internal::worker::localcomm::IntroMessage;
use bstr::BStr;
use futures::{SinkExt, StreamExt};
use std::path::Path;
use std::rc::Rc;
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct LocalDataClient {
    stream: Framed<UnixStream, LengthDelimitedCodec>,
}

impl LocalDataClient {
    pub async fn connect(path: &Path, token: &BStr) -> crate::Result<Self> {
        log::debug!("Creating local connection to: {}", path.display());
        let stream = UnixStream::connect(path).await?;
        let mut stream =
            crate::internal::worker::localcomm::make_protocol_builder().new_framed(stream);
        let data = bincode::serialize(&IntroMessage {
            token: token.into(),
        })?;
        stream.send(data.into()).await?;
        Ok(LocalDataClient { stream })
    }

    async fn send_message(&mut self, message: FromLocalDataClientMessage) -> crate::Result<()> {
        let data = bincode::serialize(&message)?;
        self.stream.send(data.into()).await?;
        Ok(())
    }

    async fn read_message(&mut self) -> crate::Result<ToLocalDataClientMessage> {
        let data = self.stream.next().await;
        Ok(match data {
            Some(data) => {
                let data = data?;
                bincode::deserialize(&data)?
            }
            None => {
                return Err(DsError::GenericError(
                    "Unexpected end of stream".to_string(),
                ));
            }
        })
    }

    pub async fn put_data_object(
        &mut self,
        data_id: OutputId,
        mime_type: String,
        data: Vec<u8>,
    ) -> crate::Result<()> {
        let message = FromLocalDataClientMessage::PutDataObject {
            data_id,
            data_object: DataObjectRef::new(DataObject::new(mime_type, data)),
        };
        self.send_message(message).await?;
        let message = self.read_message().await?;
        match message {
            ToLocalDataClientMessage::Uploaded(id) if id == data_id => Ok(()),
            ToLocalDataClientMessage::Error(message) => Err(crate::Error::GenericError(message)),
            _ => Err(DsError::GenericError("Invalid response".to_string())),
        }
    }

    pub async fn get_input(&mut self, input_id: DataInputId) -> crate::Result<DataObjectRef> {
        let message = FromLocalDataClientMessage::GetInput { input_id };
        self.send_message(message).await?;
        let message = self.read_message().await?;
        match message {
            ToLocalDataClientMessage::DataObject(dataobj) => Ok(dataobj),
            ToLocalDataClientMessage::Error(message) => Err(crate::Error::GenericError(message)),
            _ => Err(DsError::GenericError("Invalid response".to_string())),
        }
    }
}
