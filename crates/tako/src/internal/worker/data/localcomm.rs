use crate::TaskId;
use crate::datasrv::DataObjectId;
use crate::internal::common::error::DsError;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::messages::{FromLocalDataClientMessage, ToLocalDataClientMessage};
use crate::internal::messages::worker::TaskOutput;
use crate::internal::worker::state::WorkerStateRef;
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::rc::Rc;

async fn datanode_local_message_handler(
    state_ref: &WorkerStateRef,
    task_id: TaskId,
    input_map: &Option<Rc<Vec<DataObjectId>>>,
    message: FromLocalDataClientMessage,
    tx: &mut (impl Sink<Bytes> + Unpin),
) -> crate::Result<()> {
    match message {
        FromLocalDataClientMessage::PutDataObject {
            data_id,
            data_object,
        } => {
            let message = {
                let mut state = state_ref.get_mut();
                let (tasks, data_node) = state.tasks_and_datanode();
                if let Some(task) = tasks.find_mut(&task_id) {
                    let size = data_object.size();
                    // Put into datanode has to be before "add_output", because it checks
                    // that the output is not already there
                    data_node.put_object(DataObjectId::new(task_id, data_id), data_object)?;
                    task.add_output(TaskOutput { id: data_id, size });
                    ToLocalDataClientMessage::Uploaded(data_id)
                } else {
                    log::debug!("Task {} not found", task_id);
                    ToLocalDataClientMessage::Error(format!("Task {task_id} is no longer active"))
                }
            };
            send_message(tx, message).await?;
        }
        FromLocalDataClientMessage::GetInput { input_id } => {
            if let Some(data_id) = input_map
                .as_ref()
                .and_then(|map| map.get(input_id.as_num() as usize))
            {
                if let Some(data_obj) = state_ref.get_mut().data_node.get_object(*data_id) {
                    send_message(tx, ToLocalDataClientMessage::DataObject(data_obj.clone()))
                        .await?;
                } else {
                    return Err(DsError::GenericError(format!(
                        "DataObject {data_id} (input {input_id}) not found"
                    )));
                }
            } else {
                return Err(DsError::GenericError(format!("Input {input_id} not found")));
            }
        }
    }
    Ok(())
}

async fn send_message(
    tx: &mut (impl Sink<Bytes> + Unpin),
    message: ToLocalDataClientMessage,
) -> crate::Result<()> {
    let data = bincode::serialize(&message)?;
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
        let message: FromLocalDataClientMessage = bincode::deserialize(&data)?;
        if let Err(e) =
            datanode_local_message_handler(&data_node_ref, task_id, &input_map, message, &mut tx)
                .await
        {
            log::debug!("Data handler failed: {}", e);
            send_message(&mut tx, ToLocalDataClientMessage::Error(e.to_string())).await?;
        }
    }
    Ok(())
}
