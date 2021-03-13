use std::fs::File;
use std::path::PathBuf;
use std::process::Stdio;

use bytes::{Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{Future, FutureExt, SinkExt, StreamExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::transfer::transport::make_protocol_builder;
use crate::common::WrappedRcRefCell;

use crate::messages::worker::{
    FromWorkerMessage, TaskFailedMsg, TaskFinishedMsg,
};
use crate::worker::data::{
    DataObjectRef, DataObjectState, InSubworkersData, LocalData, Subscriber,
};
use crate::worker::messages;
use crate::worker::messages::{
    ComputeTaskMsg, DownloadRequestMsg, FromSubworkerMessage, RegisterSubworkerResponse,
    RemoveDataMsg, ToSubworkerMessage, UploadMsg,
};
use crate::worker::state::WorkerStateRef;
use crate::worker::task::{Task, TaskRef, TaskState};

use super::messages::RegisterSubworkerMessage;
use crate::common::data::SerializationType;
use crate::TaskId;
use crate::worker::reactor::{start_task, try_assign_tasks};
use smallvec::{smallvec, SmallVec};

#[derive(Debug, Clone)]
pub struct SubworkerPaths {
    /// Used for storing trace/profiling/log information.
    work_dir: PathBuf,
    /// Used for local communication (unix socket).
    local_dir: PathBuf,
}

impl SubworkerPaths {
    pub fn new(work_dir: PathBuf, local_dir: PathBuf) -> Self {
        Self {
            work_dir,
            local_dir,
        }
    }
}

pub(crate) type SubworkerId = u32;

pub struct Subworker {
    pub id: SubworkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
    pub running_task: Option<TaskRef>,
}

pub type SubworkerRef = WrappedRcRefCell<Subworker>;

impl Subworker {
    pub fn send_remove_data(&self, data_id: TaskId) {
        log::debug!("Removing data={} from subworker {}", data_id, self.id,);
        let message = ToSubworkerMessage::RemoveData(RemoveDataMsg { id: data_id });
        let msg_data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(msg_data.into()).unwrap();
    }

    pub fn send_download_request(&self, data_id: TaskId) {
        log::debug!(
            "Download request data={} for subworker {}",
            data_id,
            self.id,
        );
        let message = ToSubworkerMessage::DownloadRequest(DownloadRequestMsg { id: data_id });
        let msg_data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(msg_data.into()).unwrap();
    }

    pub fn send_data(&self, data_id: TaskId, data: Bytes, serializer: SerializationType) {
        log::debug!(
            "Uploading data={} (size={}) in subworker {}",
            data_id,
            data.len(),
            self.id,
        );
        let message = ToSubworkerMessage::Upload(UploadMsg {
            id: data_id,
            serializer,
        });
        let msg_data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(msg_data.into()).unwrap();
        self.sender.send(data).unwrap();
    }

    pub fn send_start_task(&self, task: &Task) {
        log::debug!("Starting task {} in subworker {}", task.id, self.id,);
        // Send message to subworker
        let message = ToSubworkerMessage::ComputeTask(ComputeTaskMsg {
            id: task.id,
            spec: &task.spec,
        });
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(data.into()).unwrap();
    }
}

impl SubworkerRef {
    pub fn new(id: SubworkerId, sender: tokio::sync::mpsc::UnboundedSender<Bytes>) -> Self {
        Self::wrap(Subworker {
            id,
            sender,
            running_task: None,
        })
    }
}

async fn subworker_handshake(
    state_ref: WorkerStateRef,
    mut listener: UnixListener,
    subworker_id: SubworkerId,
) -> Result<
    (
        SplitSink<Framed<UnixStream, LengthDelimitedCodec>, Bytes>,
        SplitStream<Framed<UnixStream, LengthDelimitedCodec>>,
    ),
    crate::Error,
> {
    if let Some(Ok(stream)) = listener.next().await {
        let mut framed = make_protocol_builder().new_framed(stream);
        let message = framed.next().await;

        if message.is_none() {
            panic!("Subworker did not sent register message");
        }
        let message = message.unwrap().unwrap();
        let register_message: RegisterSubworkerMessage = rmp_serde::from_slice(&message).unwrap();

        if register_message.subworker_id != subworker_id {
            panic!("Subworker registered with an invalid id");
        }

        let message = RegisterSubworkerResponse {
            worker: state_ref.get().listen_address.clone().into(),
        };
        framed
            .send(rmp_serde::to_vec_named(&message).unwrap().into())
            .await
            .unwrap();

        Ok(framed.split())
    } else {
        panic!("Listening on subworker socket failed");
    }
}

fn subworker_download_finished(
    state_ref: &WorkerStateRef,
    subworker_ref: &SubworkerRef,
    data: BytesMut,
    msg: messages::DownloadResponseMsg,
) {
    let state = state_ref.get();
    let data_id = msg.id;
    log::debug!(
        "Downloading data={} from subworker={} finished",
        data_id,
        subworker_ref.get().id
    );
    if let Some(data_ref) = state.data_objects.get(&data_id) {
        let mut data_obj = data_ref.get_mut();
        let bytes: Bytes = data.into();
        log::debug!(
            "Updating size, new size {}, old size {}",
            bytes.len(),
            data_obj.size
        );
        data_obj.size = bytes.len() as u64;
        match &mut data_obj.state {
            DataObjectState::LocalDownloading(ld) => {
                let mut subworkers = std::mem::take(&mut ld.subworkers);
                for subscriber in std::mem::take(&mut ld.subscribers) {
                    match subscriber {
                        Subscriber::Task(task_ref) => {
                            let mut task = task_ref.get_mut();
                            let start_task_at = match &mut task.state {
                                TaskState::Uploading(target_sw_ref, ref mut w) => {
                                    subworkers.push(target_sw_ref.clone());
                                    target_sw_ref.get_mut().send_data(
                                        data_id,
                                        bytes.clone(),
                                        msg.serializer.clone(),
                                    );
                                    assert!(*w > 0);
                                    *w -= 1;
                                    if *w == 0 {
                                        Some(target_sw_ref.clone())
                                    } else {
                                        None
                                    }
                                }
                                _ => unreachable!(),
                            };
                            if let Some(target_sw_ref) = start_task_at {
                                start_task(&target_sw_ref.get(), target_sw_ref.clone(), &mut task);
                            }
                        }
                        Subscriber::OneShot(shot) => {
                            let _ = shot.send((msg.serializer.clone(), bytes.clone()));
                        }
                    }
                }
                data_obj.state = DataObjectState::Local(LocalData {
                    serializer: msg.serializer,
                    bytes,
                    subworkers,
                })
            }
            DataObjectState::InSubworkers(_) | DataObjectState::Local(_) => {
                log::debug!("Downloading finished but object is not requested any more")
            }
            DataObjectState::Remote(_) | DataObjectState::Removed => {
                unreachable!()
            }
        }
    } else {
        log::debug!("Downloading finished but object is not requested any more")
    }
}

fn subworker_task_finished(
    state_ref: &WorkerStateRef,
    subworker_ref: &SubworkerRef,
    msg: messages::TaskFinishedMsg,
) {
    let mut state = state_ref.get_mut();
    {
        let task_ref = {
            let mut sw = subworker_ref.get_mut();
            log::debug!("Task {} finished in subworker {}", msg.id, sw.id);
            sw.running_task.take().unwrap()
        };
        state.free_subworkers.push(subworker_ref.clone());
        assert_eq!(task_ref.get().id, msg.id);
        state.remove_task(task_ref, true);

        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg {
            id: msg.id,
            size: msg.size,
        });
        state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());

        let subworkers: SmallVec<[SubworkerRef; 1]> = smallvec![subworker_ref.clone()];
        let data_ref = DataObjectRef::new(
            msg.id,
            msg.size,
            DataObjectState::InSubworkers(InSubworkersData { subworkers }),
        );
        state.add_data_object(data_ref);
    }
    try_assign_tasks(&mut state);
}

fn subworker_task_fail(
    state_ref: &WorkerStateRef,
    subworker_ref: &SubworkerRef,
    msg: messages::TaskFailedMsg,
) {
    let mut state = state_ref.get_mut();
    {
        let task_ref = {
            let mut sw = subworker_ref.get_mut();
            log::debug!("Task {} failed in subworker {}", msg.id, sw.id);
            sw.running_task.take().unwrap()
        };
        state.free_subworkers.push(subworker_ref.clone());
        assert_eq!(task_ref.get().id, msg.id);
        state.remove_task(task_ref, true);

        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg {
            id: msg.id,
            exception: msg.exception,
            traceback: msg.traceback,
        });
        state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());
    }
    try_assign_tasks(&mut state);
}

async fn run_subworker_message_loop(
    state_ref: WorkerStateRef,
    subworker_ref: SubworkerRef,
    mut stream: SplitStream<Framed<UnixStream, LengthDelimitedCodec>>,
) -> crate::Result<()> {
    while let Some(message) = stream.next().await {
        let message: FromSubworkerMessage = rmp_serde::from_slice(&message?)?;
        match message {
            FromSubworkerMessage::TaskFinished(msg) => {
                subworker_task_finished(&state_ref, &subworker_ref, msg);
            }
            FromSubworkerMessage::TaskFailed(msg) => {
                subworker_task_fail(&state_ref, &subworker_ref, msg);
            }
            FromSubworkerMessage::DownloadResponse(msg) => {
                if let Some(data) = stream.next().await {
                    let data = data?;
                    subworker_download_finished(&state_ref, &subworker_ref, data, msg);
                } else {
                    panic!("Subworker announced download but then closed the connection");
                }
            }
        };
    }
    Ok(())
}

async fn run_subworker(
    state_ref: WorkerStateRef,
    paths: SubworkerPaths,
    python_program: String,
    subworker_id: SubworkerId,
    ready_shot: oneshot::Sender<SubworkerRef>,
) -> Result<(), crate::Error> {
    let mut socket_path = paths.local_dir.clone();
    socket_path.push(format!("subworker-{}.sock", subworker_id));

    let listener = UnixListener::bind(&socket_path)?;

    let mut log_path = paths.work_dir.clone();
    let worker_id = std::env::var("RSDS_WORKER_ID").unwrap_or_else(|_| "0".into());
    log_path.push(format!("subworker-{}-{}.log", worker_id, subworker_id));
    let mut process_future = {
        let log_stdout = File::create(&log_path)?;
        let log_stderr = log_stdout.try_clone()?;

        let mut args = vec!["-m".to_string(), "rsds.subworker".to_string()];
        let mut program = python_program;

        if let Ok(cmd) = std::env::var("RSDS_SUBWORKER_PREFIX") {
            let cmd = cmd.replace("<I>", &subworker_id.to_string());
            let splitted: Vec<_> = cmd.split(" ").map(|i| i.to_string()).collect();
            args = [&splitted[1..], &["--".to_string()], &[program], &args[..]].concat();
            program = splitted[0].clone();
        }

        Command::new(program)
            .stdout(Stdio::from(log_stdout))
            .stderr(Stdio::from(log_stderr))
            .env("RSDS_SUBWORKER_SOCKET", &socket_path)
            .env("RSDS_SUBWORKER_ID", format!("{}", subworker_id))
            .args(&args)
            .current_dir(paths.work_dir)
            .spawn()?
    };

    std::mem::drop(socket_path);

    let (writer, reader) = tokio::select! {
        result = &mut process_future => {
            panic!("Subworker {} failed without registration: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = subworker_handshake(state_ref.clone(), listener, subworker_id) => {
            result?
        }
    };

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // TODO: pass writing end
    let subworker = SubworkerRef::new(subworker_id, queue_sender);
    if ready_shot.send(subworker.clone()).is_err() {
        panic!("Announcing subworker failed");
    }

    tokio::select! {
        result = process_future => {
            panic!("Subworker {} failed: {}, see {}", subworker_id, result?, log_path.display());
        },
        _result = crate::common::rpc::forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Sending a message to subworker failed");
        }
        r = run_subworker_message_loop(state_ref, subworker, reader) => {
            match r {
                Err(e) => panic!("Subworker {} loop failed: {}, log: {}", subworker_id, e, log_path.display()),
                Ok(()) => panic!("Subworker {} closed stream, see {}", subworker_id, log_path.display()),
            }

        }
    };

    //Ok(())
}

pub async fn start_subworkers(
    state: &WorkerStateRef,
    paths: SubworkerPaths,
    python_program: &str,
    count: u32,
) -> Result<(Vec<SubworkerRef>, impl Future<Output = usize>), crate::Error> {
    let mut ready = Vec::with_capacity(count as usize);
    let processes: Vec<_> = (0..count)
        .map(|i| {
            let (sx, rx) = oneshot::channel();
            ready.push(rx);
            run_subworker(
                state.clone(),
                paths.clone(),
                python_program.to_string(),
                i as SubworkerId,
                sx,
            )
            .boxed_local()
        })
        .collect();
    let mut all_processes =
        futures::future::select_all(processes).map(|(result, idx, _)| (result, idx));

    tokio::select! {
        (result, idx) = &mut all_processes => {
            panic!("Subworker {} terminated: {:?}", idx, result);
        }
        subworkers = futures::future::join_all(ready) => {
            Ok((subworkers.into_iter().map(|sw| sw.unwrap()).collect(), all_processes.map(|(_, idx)| idx)))
        }
    }
}
