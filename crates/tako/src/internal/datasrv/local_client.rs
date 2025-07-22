use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use crate::internal::datasrv::messages::{
    DataDown, FromLocalDataClientMessageUp, PutDataUp, ToLocalDataClientMessageDown,
};
use crate::internal::datasrv::utils::UPLOAD_CHUNK_SIZE;
use crate::internal::worker::localclient::LocalClientConnection;
use crate::internal::worker::localcomm::ConnectionType;
use bstr::BStr;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub struct LocalDataClient {
    connection: LocalClientConnection,
}

impl LocalDataClient {
    pub async fn connect(path: &Path, token: &BStr) -> crate::Result<Self> {
        let connection = LocalClientConnection::connect(path, token, ConnectionType::Data).await?;
        Ok(LocalDataClient { connection })
    }

    #[allow(clippy::needless_lifetimes)]
    async fn send_message<'a>(
        &mut self,
        message: FromLocalDataClientMessageUp<'a>,
    ) -> crate::Result<()> {
        self.connection.send_message(message).await
    }

    async fn read_message(&mut self) -> crate::Result<ToLocalDataClientMessageDown> {
        self.connection.read_message().await
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
            log::debug!("Uploading: {written}/{size}");
            if first {
                self.send_message(FromLocalDataClientMessageUp::PutDataObject {
                    data_id,
                    mime_type: mime_type.as_deref(),
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
