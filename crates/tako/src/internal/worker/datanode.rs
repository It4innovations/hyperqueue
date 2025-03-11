use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::DataObjectId;
use crate::internal::datasrv::messages::{
    DataObject, FromLocalDataClientMessage, ToLocalDataClientMessage,
};
use crate::internal::messages::worker::{
    DataNodeOverview, DataObjectOverview, TaskOutput,
};
use crate::internal::worker::datadownload::Downloads;
use crate::internal::worker::state::WorkerStateRef;
use crate::{Map, TaskId};
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use hashbrown::hash_map::Entry;
use std::rc::Rc;

pub(crate) struct DataConnectionSession {
    task_id: TaskId,
    inputs: Vec<DataObjectId>,
}

pub(crate) struct DataNode {
    store: Map<DataObjectId, Rc<DataObject>>,
    downloads: Downloads,
}

impl DataNode {
    pub fn new() -> Self {
        DataNode {
            store: Map::new(),
            downloads: Downloads::new(),
        }
    }

    pub fn downloads(&mut self) -> &mut Downloads {
        &mut self.downloads
    }

    pub fn get_object(&self, data_object_id: DataObjectId) -> Option<&Rc<DataObject>> {
        self.store.get(&data_object_id)
    }

    pub fn has_object(&self, data_object_id: DataObjectId) -> bool {
        self.store.contains_key(&data_object_id)
    }

    pub fn put_object(
        &mut self,
        data_object_id: DataObjectId,
        data_object: Rc<DataObject>,
    ) -> crate::Result<()> {
        match self.store.entry(data_object_id) {
            Entry::Occupied(_) => Err(DsError::GenericError(format!(
                "DataObject {data_object_id} already exists"
            ))),
            Entry::Vacant(entry) => {
                entry.insert(data_object);
                Ok(())
            }
        }
    }

    pub fn remove_object(&mut self, data_object_id: DataObjectId) {
        log::debug!("Removing data object {:?}", data_object_id);
        if self.store.remove(&data_object_id).is_none() {
            log::debug!("Data object {} not found", data_object_id);
        }
    }

    pub fn get_overview(&self) -> DataNodeOverview {
        let objects = self
            .store
            .iter()
            .map(|(id, obj)| DataObjectOverview {
                id: *id,
                size: obj.data.len(),
            })
            .collect();
        DataNodeOverview {
            objects,
            total_downloaded_count: 0,
            total_uploaded_count: 0,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
        }
    }
}

async fn datanode_message_handler(
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
                    let size = data_object.data.len();
                    // Put into datanode has to be before "add_output", because it checks
                    // that the output is not already there
                    data_node
                        .put_object(DataObjectId::new(task_id, data_id), Rc::new(data_object))?;
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

pub(crate) async fn datanode_connection_handler(
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
            datanode_message_handler(&data_node_ref, task_id, &input_map, message, &mut tx).await
        {
            log::debug!("Data handler failed: {}", e);
            send_message(&mut tx, ToLocalDataClientMessage::Error(e.to_string())).await?;
        }
    }
    Ok(())
}
