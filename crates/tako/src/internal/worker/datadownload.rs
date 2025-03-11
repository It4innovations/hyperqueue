use crate::connection::Connection;
use crate::datasrv::DataObjectId;
use crate::internal::datasrv::messages::{DataObject, FromDataClientMessage, ToDataClientMessage};
use crate::internal::messages::worker::FromWorkerMessage;
use crate::internal::worker::state::WorkerStateRef;
use crate::{Map, PriorityTuple, Set, TaskId, WorkerId};
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Notify};
use tokio::task::{spawn_local, AbortHandle, JoinSet};

const PROTOCOL_VERSION: u32 = 0;
const MAX_PARALLEL_DOWNLOADS: usize = 4;
const MAX_DOWNLOAD_TRIES: usize = 10;

pub(crate) struct RunningDownloadHandle {
    subscribed_tasks: Set<TaskId>,
    priority: PriorityTuple,
    placement_resolver: Option<oneshot::Sender<WorkerId>>,
    abort: Option<AbortHandle>,
}

pub(crate) type DataClientConnection = Connection<ToDataClientMessage, FromDataClientMessage>;

pub(crate) struct Downloads {
    download_queue: priority_queue::PriorityQueue<DataObjectId, PriorityTuple>,
    running_downloads: Map<DataObjectId, RunningDownloadHandle>,
    notify_downloader: Rc<Notify>,
    idle_connections: Map<WorkerId, Vec<DataClientConnection>>,
}

impl Downloads {
    pub fn new() -> Self {
        Downloads {
            download_queue: Default::default(),
            running_downloads: Default::default(),
            notify_downloader: Rc::new(Notify::new()),
            idle_connections: Map::new(),
        }
    }

    pub fn get_idle_connection(&mut self, worker_id: WorkerId) -> Option<DataClientConnection> {
        self.idle_connections
            .get_mut(&worker_id)
            .and_then(|connections| connections.pop())
    }

    pub fn download_object(
        &mut self,
        data_object_id: DataObjectId,
        priority: PriorityTuple,
        task_id: TaskId,
    ) {
        if let Some(download) = self.running_downloads.get_mut(&data_object_id) {
            log::debug!("Added task {task_id} into download request of {data_object_id}");
            if priority > download.priority {
                log::debug!("Updating priority of download {data_object_id}");
                download.priority = priority;
                self.download_queue
                    .change_priority(&data_object_id, priority);
            }
            download.subscribed_tasks.insert(task_id);
            return;
        }
        log::debug!("Scheduling data object download {data_object_id}");
        self.download_queue.push(data_object_id, priority);
        let mut subscribed_tasks = Set::new();
        subscribed_tasks.insert(task_id);
        self.running_downloads.insert(
            data_object_id,
            RunningDownloadHandle {
                subscribed_tasks,
                priority,
                placement_resolver: None,
                abort: None,
            },
        );
        self.notify_downloader.notify_one();
    }
}

async fn get_connection(
    state_ref: &WorkerStateRef,
    worker_id: WorkerId,
) -> crate::Result<DataClientConnection> {
    let address = {
        let mut state = state_ref.get_mut();
        if let Some(connection) = state.data_node.downloads().get_idle_connection(worker_id) {
            log::debug!("Reusing connection to worker {worker_id}");
            return Ok(connection);
        }
        state
            .worker_addresses
            .get(&worker_id)
            .cloned()
            .ok_or_else(|| crate::Error::GenericError(format!("Cannot find worker {worker_id}")))?
    };
    log::debug!("Connecting to worker {worker_id} on address {address}");
    let socket = TcpStream::connect(&address).await?;
    let secret_key = state_ref.get().secret_key().cloned();
    Connection::init(
        socket,
        PROTOCOL_VERSION,
        "data-client",
        "data-node",
        secret_key,
    )
    .await
}

async fn download_from_worker(
    state_ref: &WorkerStateRef,
    worker_id: WorkerId,
    data_id: DataObjectId,
) -> crate::Result<Rc<DataObject>> {
    let mut connection = get_connection(state_ref, worker_id).await?;
    let message = FromDataClientMessage::GetObject { data_id };
    let message = connection.send_and_receive(message).await?;
    match message {
        ToDataClientMessage::DataObject(data_obj) => Ok(data_obj),
    }
}

async fn download_process(state_ref: WorkerStateRef, data_id: DataObjectId) {
    for i in 0..MAX_DOWNLOAD_TRIES {
        let (sender, receiver) = oneshot::channel();
        {
            log::debug!("Trying to resolve placement for {data_id}, try {i}");
            let mut state = state_ref.get_mut();
            if let Some(handle) = state
                .data_node
                .downloads()
                .running_downloads
                .get_mut(&data_id)
            {
                handle.placement_resolver = Some(sender);
            } else {
                return;
            }
            state
                .comm()
                .send_message_to_server(FromWorkerMessage::PlacementQuery(data_id));
        }
        let worker_id = receiver.await.unwrap();
        match download_from_worker(&state_ref, worker_id, data_id).await {
            Ok(_) => todo!(), // TODO: Return connection to the pool
            Err(e) => todo!(),
        };
        tokio::time::sleep(Duration::from_secs(i as u64)).await;
    }
    todo!() // FAIL TASKS
}

pub(crate) fn start_download_process(state_ref: WorkerStateRef) {
    spawn_local(async move {
        let notify = {
            let mut state = state_ref.get_mut();
            state.data_node.downloads().notify_downloader.clone()
        };
        notify.notified().await;
        let mut join_set = JoinSet::new();
        loop {
            {
                let mut state = state_ref.get_mut();
                while join_set.len() < MAX_PARALLEL_DOWNLOADS {
                    if let Some((data_id, _)) = state.data_node.downloads().download_queue.pop() {
                        let mut state = state_ref.get_mut();
                        let download = state
                            .data_node
                            .downloads()
                            .running_downloads
                            .get_mut(&data_id)
                            .unwrap();
                        assert!(download.placement_resolver.is_none());
                        assert!(download.abort.is_none());
                        let abort =
                            join_set.spawn_local(download_process(state_ref.clone(), data_id));
                        download.abort = Some(abort);
                    } else {
                        break;
                    }
                }
                state.data_node.downloads().running_downloads.is_empty()
            };

            tokio::select! {
                _ = notify.notified() => {}
                r = join_set.join_next() => {
                    if let Some(_) = r {
                        todo!()
                    } else {
                        notify.notified().await;
                    }
                }
            }
        }
    });
}
