use crate::comm::{deserialize, serialize};
use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use crate::internal::datasrv::messages::{
    DataDown, FromLocalDataClientMessageUp, PutDataUp, ToLocalDataClientMessageDown,
};
use crate::internal::datasrv::utils::UPLOAD_CHUNK_SIZE;
use crate::internal::worker::localcomm::IntroMessage;
use bstr::BStr;
use futures::{SinkExt, StreamExt};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
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
        let data = serialize(&IntroMessage {
            token: token.into(),
        })?;
        stream.send(data.into()).await?;
        Ok(LocalDataClient { stream })
    }

    #[allow(clippy::needless_lifetimes)]
    async fn send_message<'a>(
        &mut self,
        message: FromLocalDataClientMessageUp<'a>,
    ) -> crate::Result<()> {
        let data = serialize(&message)?;
        self.stream.send(data.into()).await?;
        Ok(())
    }

    async fn read_message(&mut self) -> crate::Result<ToLocalDataClientMessageDown> {
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

    pub async fn put_data_object_from_file(
        &mut self,
        data_id: OutputId,
        mime_type: Option<String>,
        path: &Path,
    ) -> crate::Result<()> {
        log::debug!(
            "Uploading file {} as data_obj={} (mime_type={:?})",
            path.display(),
            data_id,
            &mime_type
        );
        let mut file = File::open(path)?;
        let size = file.metadata()?.len();
        let mut buffer = vec![0; UPLOAD_CHUNK_SIZE];
        let mut first = true;
        let mut written = 0;
        loop {
            let bytes_read = file.read(&mut buffer)?;
            let data = &buffer[0..bytes_read];
            written += data.len();
            log::debug!("Uploading: {}/{}", written, size);
            if first {
                self.send_message(FromLocalDataClientMessageUp::PutDataObject {
                    data_id,
                    mime_type: mime_type.as_ref().map(|x| x.as_str()),
                    size,
                    data: PutDataUp { data },
                })
                .await?;
                first = false;
            } else {
                if bytes_read == 0 {
                    return Err(DsError::GenericError(
                        "File changed size during upload".to_string(),
                    ));
                }
                self.send_message(FromLocalDataClientMessageUp::PutDataObjectPart(PutDataUp {
                    data,
                }))
                .await?;
            }
            if written as u64 >= size {
                if written as u64 > size {
                    return Err(DsError::GenericError(
                        "File changed size during upload".to_string(),
                    ));
                }
                break;
            }
        }
        log::debug!("Waiting for confirmation of upload");
        let message = self.read_message().await?;
        match message {
            ToLocalDataClientMessageDown::Uploaded(id) if id == data_id => Ok(()),
            ToLocalDataClientMessageDown::Error(message) => {
                Err(crate::Error::GenericError(message))
            }
            _ => Err(DsError::GenericError("Invalid response".to_string())),
        }
    }

    pub async fn get_input_to_file(
        &mut self,
        input_id: DataInputId,
        path: &Path,
    ) -> crate::Result<()> {
        let message = FromLocalDataClientMessageUp::GetInput { input_id };
        self.send_message(message).await?;
        let mut file = File::create(path)?;
        let message = self.read_message().await?;
        match message {
            ToLocalDataClientMessageDown::DataObject {
                mime_type: _,
                size,
                data: DataDown { data },
            } => {
                file.write_all(&data)?;
                let mut written = data.len();
                while written < size as usize {
                    let message = self.read_message().await?;
                    if let ToLocalDataClientMessageDown::DataObjectPart(DataDown { data }) = message
                    {
                        file.write_all(&data)?;
                        written += data.len();
                    }
                }
                Ok(())
            }
            ToLocalDataClientMessageDown::Error(message) => {
                Err(crate::Error::GenericError(message))
            }
            _ => Err(DsError::GenericError("Invalid response".to_string())),
        }
    }
}
