use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::messages::{FromDataClientMessage, ToDataClientMessage};
use crate::internal::datasrv::DataObjectRef;
use crate::internal::messages::worker::FromWorkerMessage;
use crate::internal::worker::state::WorkerStateRef;
use crate::internal::worker::task::TaskState;
use crate::{Map, PriorityTuple, Set, TaskId, WorkerId, WrappedRcRefCell};
use orion::kex::SecretKey;
use priority_queue::PriorityQueue;
use std::fmt::Debug;
use std::iter::Inspect;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{spawn_local, AbortHandle, JoinSet};
use tokio::time::Instant;

const PROTOCOL_VERSION: u32 = 0;
const MAX_IDLE_CONNECTIONS_PER_WORKER: usize = 4;

pub(crate) type DataClientConnection = Connection<ToDataClientMessage, FromDataClientMessage>;

pub(crate) trait DownloadInterface {
    fn find_placement(&self, data_id: DataObjectId) -> oneshot::Receiver<Option<String>>;
    fn on_download_finished(&self, data_id: DataObjectId, data_ref: DataObjectRef);
    fn on_download_failed(&self, data_id: DataObjectId);
}

pub(crate) struct DownloadManager<I, P> {
    interface: I,
    download_queue: PriorityQueue<DataObjectId, P>,
    running_downloads: Map<DataObjectId, AbortHandle>,
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
            running_downloads: Default::default(),
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
        self.download_queue.remove(&data_id);
        if let Some(r) = self.running_downloads.remove(&data_id) {
            r.abort();
        }
    }

    pub fn download_object(&mut self, data_object_id: DataObjectId, priority: P) {
        log::debug!("Dataobj {data_object_id} requested; priority: {priority:?}");
        if let Some(old_priority) = self.download_queue.get_priority(&data_object_id) {
            log::debug!("Dataobj already in queue");
            if priority > *old_priority {
                log::debug!("Updating priority of download {data_object_id}");
                self.download_queue
                    .change_priority(&data_object_id, priority);
            }
            return;
        }
        if self.running_downloads.contains_key(&data_object_id) {
            log::debug!("Dataobj is already downloading");
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
    pub fn running_downloads(&self) -> &Map<DataObjectId, AbortHandle> {
        &self.running_downloads
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
        if let Some(connection) = dm.get_idle_connection(&addr) {
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
    let mut connection = get_connection(dm_ref, &addr).await?;
    let message = FromDataClientMessage::GetObject { data_id };
    let message = connection.send_and_receive(message).await?;
    match message {
        ToDataClientMessage::DataObject(data_obj) => {
            dm_ref.get_mut().return_connection(&addr, connection);
            Ok(data_obj)
        }
        ToDataClientMessage::DataObjectNotFound => Err(crate::Error::GenericError(
            "Object not found in remote side".to_string(),
        )),
    }
}

async fn download_process<I: DownloadInterface, P: Ord + Debug>(
    dm_ref: DownloadManagerRef<I, P>,
    data_id: DataObjectId,
    _permit: OwnedSemaphorePermit,
    max_download_tries: u32,
    wait_between_download_tries: Duration,
) {
    for i in 0..max_download_tries {
        log::debug!("Trying to resolve placement for {data_id}, try {i}");
        let resolver = {
            let mut dm = dm_ref.get();
            dm.interface.find_placement(data_id)
        };
        if let Ok(addr) = resolver.await {
            log::debug!("Placement for {data_id} was resolved as {addr:?}");
            if let Some(addr) = addr {
                match download_from_address(&dm_ref, &addr, data_id).await {
                    Ok(data_obj) => {
                        log::debug!("Download of {data_id} completed; size={}", data_obj.size());
                        let mut dm = dm_ref.get_mut();
                        dm.interface.on_download_finished(data_id, data_obj);
                        return;
                    }
                    Err(e) => {
                        log::debug!("Downloading of {data_id} failed: {e}");
                    }
                };
            } else {
                log::debug!("Placement for {data_id} is not resolvable.");
                let mut dm = dm_ref.get_mut();
                dm.interface.on_download_failed(data_id);
                return;
            }
        }
        tokio::time::sleep(wait_between_download_tries * i).await;
    }
    let dm = dm_ref.get();
    dm.interface.on_download_failed(data_id);
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
        let mut dm = dm_ref.get_mut();
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
        {
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
                    assert!(dm.running_downloads.insert(data_id, abort).is_none());
                    dm.download_queue.is_empty()
                } else {
                    true
                }
            };
            if is_empty {
                notify.notified().await;
            }
        };
    }
}
