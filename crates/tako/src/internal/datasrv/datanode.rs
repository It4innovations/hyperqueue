use super::dataobj::{DataObject, DataObjectId};
use crate::internal::common::error::DsError;
use crate::{Map, WrappedRcRefCell};
use futures::StreamExt;
use hashbrown::hash_map::Entry;
use std::path::Path;
use std::rc::Rc;
use tokio::net::UnixListener;
use tokio_util::codec::length_delimited::Builder;
use tokio_util::codec::LengthDelimitedCodec;

pub(crate) struct DataNode {
    store: Map<DataObjectId, Rc<DataObject>>,
}

pub type DataNodeRef = WrappedRcRefCell<DataNode>;

impl DataNode {
    pub fn new() -> Self {
        DataNode { store: Map::new() }
    }

    pub fn get_object(&self, data_object_id: DataObjectId) -> Option<&Rc<DataObject>> {
        self.store.get(&data_object_id)
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
}

fn make_protocol_builder() -> Builder {
    *LengthDelimitedCodec::builder().little_endian()
}

// async fn process_datanode_message(data_node_ref: DataNodeRef, msg: ToDataNodeMessage, Box<dyn SplitSink>) {
//
// }

async fn run_datanode_over_unix_socket(
    listener: UnixListener,
    data_node_ref: DataNodeRef,
) -> crate::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                log::debug!("New data client connected via unix socket: {addr:?}");
                let data_node_ref = data_node_ref.clone();
                tokio::task::spawn_local(async move {
                    let (tx, mut rx) = make_protocol_builder().new_framed(stream).split();
                    while let Some(msg) = rx.next().await {
                        match msg {
                            Ok(msg) => {
                                todo!()
                            }
                            Err(e) => {
                                log::error!("Data connection error: {e}");
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                log::debug!("Accepting a new data client via unix socket failed: {e}")
            }
        }
    }
}
