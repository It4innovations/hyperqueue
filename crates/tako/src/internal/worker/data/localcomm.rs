use crate::TaskId;
use crate::comm::serialize;
use crate::datasrv::DataObjectId;
use crate::internal::common::error::DsError;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::messages::{
    DataDown, FromLocalDataClientMessageDown, ToLocalDataClientMessageUp,
};
use crate::internal::datasrv::utils::{DataObjectComposer, DataObjectDecomposer};
use crate::internal::messages::worker::TaskOutput;
use crate::internal::worker::state::WorkerStateRef;
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::rc::Rc;

async fn datanode_local_message_handler(
    state_ref: &WorkerStateRef,
    task_id: TaskId,
    input_map: &Option<Rc<Vec<DataObjectId>>>,
    message: FromLocalDataClientMessageDown,
    mut rx: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    tx: &mut (impl Sink<Bytes> + Unpin),
) -> crate::Result<()> {
    match message {
        FromLocalDataClientMessageDown::PutDataObject {
            data_id,
            mime_type,
            size,
            data: DataDown { data },
        } => {
            let message = {
                log::debug!(
                    "Getting object from local connection: data_id={data_id} mime_type={mime_type} size={size} first_data: {}",
                    data.len(),
                );
                log::debug!("Initial data size: {}", data.len());
                let mut composer = DataObjectComposer::new(size as usize, data);
                while !composer.is_finished() {
                    let data = rx.next().await.transpose()?;
                    if let Some(data) = data {
                        let message: FromLocalDataClientMessageDown = bincode::deserialize(&data)?;
                        if let FromLocalDataClientMessageDown::PutDataObjectPart(DataDown {
                            data,
                        }) = message
                        {
                            log::debug!(
                                "Received {} bytes from local connection for data_id={data_id}",
                                data.len()
                            );
                            composer.add(data);
                        } else {
                            return Err(crate::Error::GenericError(
                                "Expected to receive PuDataPart".to_string(),
                            ));
                        }
                    } else {
                        return Err(crate::Error::GenericError(
                            "Expected to receive PuDataPart, but connection is closed".to_string(),
                        ));
                    }
                }
                let data_object = DataObjectRef::new(composer.finish(mime_type));
                // Put into datanode has to be before "add_output", because it checks
                // that the output is not already there
                let mut state = state_ref.get_mut();
                let (tasks, storage) = state.tasks_and_storage();
                if let Some(task) = tasks.find_mut(&task_id) {
                    storage.put_object(DataObjectId::new(task_id, data_id), data_object)?;
                    storage.add_stats_local_upload(size);
                    task.add_output(TaskOutput { id: data_id, size });
                    ToLocalDataClientMessageUp::Uploaded(data_id)
                } else {
                    log::debug!("Task {} not found", task_id);
                    ToLocalDataClientMessageUp::Error(format!("Task {task_id} is no longer active"))
                }
            };
            send_message(tx, message).await?;
        }
        FromLocalDataClientMessageDown::GetInput { input_id } => {
            if let Some(data_id) = input_map
                .as_ref()
                .and_then(|map| map.get(input_id.as_num() as usize))
            {
                let data_obj = state_ref
                    .get_mut()
                    .data_storage
                    .get_object(*data_id)
                    .cloned();
                if let Some(data_obj) = data_obj {
                    let mime_type = data_obj.mime_type().to_string();
                    let size = data_obj.size();
                    let (mut decomposer, first_data) = DataObjectDecomposer::new(data_obj);
                    send_message(
                        tx,
                        ToLocalDataClientMessageUp::DataObject {
                            mime_type,
                            size,
                            data: first_data,
                        },
                    )
                    .await?;
                    while let Some(data) = decomposer.next() {
                        send_message(tx, ToLocalDataClientMessageUp::DataObjectPart(data)).await?;
                    }
                    let mut state = state_ref.get_mut();
                    state.data_storage.add_stats_local_download(size);
                } else {
                    return Err(DsError::GenericError(format!(
                        "DataObject {data_id} (input {input_id}) not found"
                    )));
                }
            } else {
                return Err(DsError::GenericError(format!("Input {input_id} not found")));
            }
        }
        FromLocalDataClientMessageDown::PutDataObjectPart(_) => {
            return Err(DsError::GenericError(
                "Unexpected PutDataObjectPart message".to_string(),
            ));
        }
    }
    Ok(())
}

async fn send_message(
    tx: &mut (impl Sink<Bytes> + Unpin),
    message: ToLocalDataClientMessageUp,
) -> crate::Result<()> {
    let data = serialize(&message)?;
    tx.send(data.into())
        .await
        .map_err(|_| DsError::GenericError("Data connection send message error".to_string()))?;
    Ok(())
}

pub(crate) async fn datanode_local_connection_handler(
    data_node_ref: WorkerStateRef,
    mut rx: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    mut tx: (impl Sink<Bytes> + Unpin),
    task_id: TaskId,
    input_map: Option<Rc<Vec<DataObjectId>>>,
) -> crate::Result<()> {
    while let Some(data) = rx.next().await {
        let data = data?;
        let message: FromLocalDataClientMessageDown = bincode::deserialize(&data)?;
        if let Err(e) = datanode_local_message_handler(
            &data_node_ref,
            task_id,
            &input_map,
            message,
            &mut rx,
            &mut tx,
        )
        .await
        {
            log::debug!("Data handler failed: {}", e);
            send_message(&mut tx, ToLocalDataClientMessageUp::Error(e.to_string())).await?;
        }
    }
    Ok(())
}
