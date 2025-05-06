use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::messages::{
    DataDown, FromDataClientMessage, ToDataClientMessageDown,
};
use crate::internal::datasrv::utils::DataObjectComposer;
use crate::{Map, WrappedRcRefCell};
use orion::kex::SecretKey;
use priority_queue::PriorityQueue;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{AbortHandle, JoinSet, spawn_local};
use tokio::time::Instant;

const PROTOCOL_VERSION: u32 = 0;
const MAX_IDLE_CONNECTIONS_PER_WORKER: usize = 4;

pub(crate) type DataClientConnection = Connection<ToDataClientMessageDown, FromDataClientMessage>;

pub(crate) trait DownloadInterface: Clone {
    fn find_placement(&self, data_id: DataObjectId) -> oneshot::Receiver<Option<String>>;
    fn on_download_finished(&self, data_id: DataObjectId, data_ref: DataObjectRef);
    fn on_download_failed(&self, data_id: DataObjectId);
}

#[derive(Default)]
pub(crate) struct DownloadInfo {
    ref_counter: u32,
    abort_handle: Option<AbortHandle>,
}

pub(crate) struct DownloadManager<I, P> {
    interface: I,
    download_queue: PriorityQueue<DataObjectId, P>,
    download_info: Map<DataObjectId, DownloadInfo>,
    notify_downloader: Rc<Notify>,
    idle_connections: Map<String, Vec<(DataClientConnection, Instant)>>,
    secret_key: Option<Arc<SecretKey>>,
}

pub(crate) type DownloadManagerRef<I, P> = WrappedRcRefCell<DownloadManager<I, P>>;

impl<I: DownloadInterface, P: Ord> DownloadManagerRef<I, P> {
    pub fn new(interface: I, secret_key: Option<Arc<SecretKey>>) -> Self {
        WrappedRcRefCell::wrap(DownloadManager {
            interface,
            secret_key,
            download_queue: Default::default(),
            download_info: Default::default(),
            notify_downloader: Rc::new(Notify::new()),
            idle_connections: Map::new(),
        })
    }
}

impl<I: DownloadInterface, P: Ord + Debug> DownloadManager<I, P> {
    fn get_idle_connection(&mut self, addr: &str) -> Option<DataClientConnection> {
        self.idle_connections
            .get_mut(addr)
            .and_then(|connections| connections.pop().map(|(conn, _)| conn))
    }

    fn return_connection(&mut self, addr: &str, connection: DataClientConnection) {
        log::debug!("Returning connection for worker {addr} to idle_connections");
        let connections = self.idle_connections.entry(addr.to_string()).or_default();
        if connections.len() < MAX_IDLE_CONNECTIONS_PER_WORKER {
            connections.push((connection, Instant::now()));
        } else {
            log::debug!(
                "Connection is closed, because there are too many connections for this address"
            );
        }
    }

    pub fn cancel_download(&mut self, data_id: DataObjectId) {
        if let Some(info) = self.download_info.get_mut(&data_id) {
            assert!(info.ref_counter > 0);
            info.ref_counter -= 1;
            if info.ref_counter == 0 {
                let info = self.download_info.remove(&data_id).unwrap();
                if let Some(abort_handle) = info.abort_handle {
                    abort_handle.abort();
                } else {
                    self.download_queue.remove(&data_id);
                }
            }
        }
    }

    pub fn download_object(&mut self, data_object_id: DataObjectId, priority: P) {
        log::debug!("Dataobj {data_object_id} requested; priority: {priority:?}");
        let entry = self.download_info.entry(data_object_id).or_default();
        entry.ref_counter += 1;
        if entry.abort_handle.is_some() {
            log::debug!("Dataobj is already downloading");
            return;
        }
        if let Some(old_priority) = self.download_queue.get_priority(&data_object_id) {
            log::debug!("Dataobj already in queue");
            if priority > *old_priority {
                log::debug!("Updating priority of download {data_object_id}");
                self.download_queue
                    .change_priority(&data_object_id, priority);
            }
            return;
        }
        self.download_queue.push(data_object_id, priority);
        if self.download_queue.len() == 1 {
            self.notify_downloader.notify_one();
        }
    }

    #[cfg(test)]
    pub fn queue(&self) -> &PriorityQueue<DataObjectId, P> {
        &self.download_queue
    }

    #[cfg(test)]
    pub fn download_info(&self) -> &Map<DataObjectId, DownloadInfo> {
        &self.download_info
    }

    #[cfg(test)]
    pub fn idle_connections(&self) -> &Map<String, Vec<(DataClientConnection, Instant)>> {
        &self.idle_connections
    }
}

async fn get_connection<I: DownloadInterface, P: Ord + Debug>(
    dm_ref: &DownloadManagerRef<I, P>,
    addr: &str,
) -> crate::Result<DataClientConnection> {
    {
        let mut dm = dm_ref.get_mut();
        if let Some(connection) = dm.get_idle_connection(addr) {
            log::debug!("Reusing connection {addr}");
            return Ok(connection);
        }
    };
    log::debug!("Connecting to datanode at '{addr}'");
    let socket = TcpStream::connect(&addr).await?;
    let secret_key = dm_ref.get().secret_key.clone();
    Connection::init(
        socket,
        PROTOCOL_VERSION,
        "data-client",
        "data-server",
        secret_key,
    )
    .await
}

async fn download_from_address<I: DownloadInterface, P: Ord + Debug>(
    dm_ref: &DownloadManagerRef<I, P>,
    addr: &str,
    data_id: DataObjectId,
) -> crate::Result<DataObjectRef> {
    let mut connection = get_connection(dm_ref, addr).await?;
    log::debug!("Sending download request for object {data_id}");
    let message = FromDataClientMessage::GetObject { data_id };
    let message = connection.send_and_receive(message).await?;
    match message {
        ToDataClientMessageDown::DataObject {
            mime_type,
            size,
            data: DataDown { data },
        } => {
            log::debug!(
                "Downloading data object {data_id}, size={size}, mime_type={mime_type:?}, initial_data={}",
                data.len(),
            );
            let mut composer = DataObjectComposer::new(size as usize, data);
            while !composer.is_finished() {
                match connection.receive().await.transpose()? {
                    Some(ToDataClientMessageDown::DataObjectPart(DataDown { data })) => {
                        let got = composer.add(data);
                        log::debug!("Download data part of data object {data_id}: {got}/{size}")
                    }
                    Some(_) => {
                        return Err(crate::Error::GenericError(
                            "Unexpected message in download connection".to_string(),
                        ));
                    }
                    None => {
                        return Err(crate::Error::GenericError(
                            "Unexpected close of download connection".to_string(),
                        ));
                    }
                }
            }
            dm_ref.get_mut().return_connection(addr, connection);
            Ok(DataObjectRef::new(composer.finish(mime_type)))
        }
        ToDataClientMessageDown::NotFound => Err(crate::Error::GenericError(
            "Object not found in remote side".to_string(),
        )),
        ToDataClientMessageDown::DataObjectPart(_) => {
            Err(crate::Error::GenericError("Invalid message".to_string()))
        }
    }
}

async fn download_process<I: DownloadInterface, P: Ord + Debug>(
    dm_ref: DownloadManagerRef<I, P>,
    data_id: DataObjectId,
    _permit: OwnedSemaphorePermit,
    max_download_tries: u32,
    wait_between_download_tries: Duration,
) {
    for i in 1..=max_download_tries {
        log::debug!("Trying to resolve placement for {data_id}, try {i}/{max_download_tries}");
        let resolver = {
            let interface = dm_ref.get().interface.clone();
            interface.find_placement(data_id)
        };
        if let Ok(addr) = resolver.await {
            log::debug!("Placement for {data_id} was resolved as {addr:?}");
            if let Some(addr) = addr {
                match download_from_address(&dm_ref, &addr, data_id).await {
                    Ok(data_obj) => {
                        log::debug!("Download of {data_id} completed; size={}", data_obj.size());
                        let interface = {
                            let mut dm = dm_ref.get_mut();
                            dm.download_info.remove(&data_id);
                            dm.interface.clone()
                        };
                        interface.on_download_finished(data_id, data_obj);
                        return;
                    }
                    Err(e) => {
                        log::debug!(
                            "Downloading of {data_id} failed (try {i}/{max_download_tries}): {e}"
                        );
                    }
                };
            } else {
                log::debug!("Placement for {data_id} is not resolvable.");
                let interface = {
                    let mut dm = dm_ref.get_mut();
                    dm.download_info.remove(&data_id);
                    dm.interface.clone()
                };
                interface.on_download_failed(data_id);
                return;
            }
        }
        if i < max_download_tries - 1 && !wait_between_download_tries.is_zero() {
            let sleep = wait_between_download_tries * i;
            log::debug!("Sleeping for {sleep:?  }");
            tokio::time::sleep(sleep).await;
        }
    }
    log::debug!(
        "Download of dataobj {data_id} fails and reaches the limit, marking dataobj as unreachable"
    );
    let interface = {
        let mut dm = dm_ref.get_mut();
        dm.download_info.remove(&data_id);
        dm.interface.clone()
    };
    interface.on_download_failed(data_id);
}

pub(crate) async fn download_manager_process<
    I: DownloadInterface + 'static,
    P: Ord + Debug + 'static,
>(
    dm_ref: DownloadManagerRef<I, P>,
    max_parallel_downloads: u32,
    max_download_tries: u32,
    wait_between_download_tries: Duration,
    idle_connection_timeout: Duration,
) {
    let notify = {
        let dm = dm_ref.get_mut();
        dm.notify_downloader.clone()
    };
    notify.notified().await;
    // First download request arrived

    if !idle_connection_timeout.is_zero() {
        let dm_ref2 = dm_ref.clone();
        spawn_local(async move {
            // Periodically close idle connections
            loop {
                tokio::time::sleep(idle_connection_timeout / 2).await;
                let mut dm = dm_ref2.get_mut();
                if !dm.idle_connections.is_empty() {
                    let now = Instant::now();
                    dm.idle_connections.values_mut().for_each(|v| {
                        v.retain(|(_, t)| now - *t < idle_connection_timeout);
                    });
                    dm.idle_connections.retain(|_, v| !v.is_empty());
                }
            }
        });
    }

    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(max_parallel_downloads as usize));
    loop {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let is_empty = {
            let mut dm = dm_ref.get_mut();
            if let Some((data_id, _)) = dm.download_queue.pop() {
                let abort = join_set.spawn_local(download_process(
                    dm_ref.clone(),
                    data_id,
                    permit,
                    max_download_tries,
                    wait_between_download_tries,
                ));
                let info = dm.download_info.get_mut(&data_id).unwrap();
                assert!(info.abort_handle.is_none());
                info.abort_handle = Some(abort);
                dm.download_queue.is_empty()
            } else {
                true
            }
        };
        if is_empty {
            notify.notified().await;
        }
        while join_set.try_join_next().is_some() { /* Do nothing */ }
    }
}
