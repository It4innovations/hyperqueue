use crate::common::data::SerializationType;
use crate::transfer::transport::{connect_to_worker};
use crate::common::Map;
use crate::server::core::{CoreRef, Core};
use crate::server::comm::{CommSenderRef};
use crate::server::task::{TaskRef, TaskRuntimeState};
use crate::common::trace::trace_task_new_finished;
use crate::transfer::fetch::fetch_data;
use crate::transfer::messages::{DataRequest, DataResponse, UploadDataMsg};
use bytes::BytesMut;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use rand::seq::SliceRandom;
use std::iter::FromIterator;

use crate::server::worker::WorkerId;
use crate::TaskId;


pub async fn gather(
    core_ref: &CoreRef,
    task_ids: &[TaskId],
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    let mut worker_map: Map<WorkerId, Vec<TaskId>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for task_id in task_ids {
            let task_ref = core.get_task_by_id_or_panic(*task_id);
            let task = task_ref.get();
            &task.get_placement().map(|ws| {
                let ws = Vec::<&WorkerId>::from_iter(ws.into_iter());
                ws.choose(&mut rng).map(|w| {
                    worker_map
                        .entry(**w)
                        .or_default()
                        .push(*task_id);
                })
            });
        }
    }

    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .into_iter()
            .map(|(w_id, keys)| get_data_from_worker(core_ref.get().get_worker_by_id_or_panic(w_id).listen_address.clone(), keys)),
    );

    let mut result = Vec::with_capacity(task_ids.len());
    while let Some(r) = worker_futures.next().await {
        result.append(&mut r?);
    }
    Ok(result)
}

pub async fn get_data_from_worker(
    worker_address: String,
    task_ids: Vec<TaskId>,
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    // TODO: Storing worker connection?
    // Directly resend to client?

    let mut stream = connect_to_worker(worker_address.clone()).await?;

    let mut result = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        log::debug!("Fetching {} from {}", &task_id, worker_address);
        let (s, data, serializer) = fetch_data(stream, task_id).await?;
        stream = s;
        log::debug!(
            "Fetched {} from {} ({} bytes)",
            &task_id,
            worker_address,
            data.len()
        );
        result.push((task_id, data, serializer));
    }
    Ok(result)
}

pub async fn update_data_on_worker(
    core: &CoreRef,
    worker_id: WorkerId,
    data: Vec<(TaskRef, BytesMut)>,
) -> crate::Result<()> {
    let address = core.get().get_worker_by_id_or_panic(worker_id).listen_address.clone();
    let mut stream = connect_to_worker(address).await?;

    for (task_ref, data_for_id) in data {
        let task_id = task_ref.get().id;
        let message = DataRequest::UploadData(UploadDataMsg {
            task_id,
            serializer: SerializationType::Pickle,
        });
        let size = data_for_id.len() as u64;
        let data = rmp_serde::to_vec_named(&message).unwrap();
        stream.send(data.into()).await?;
        stream.send(data_for_id.into()).await?;
        let data = stream.next().await.unwrap().unwrap();
        let message: DataResponse = rmp_serde::from_slice(&data).unwrap();
        match message {
            DataResponse::DataUploaded(msg) => {
                if msg.task_id != task_id {
                    panic!("Upload sanity check failed, different key returned");
                }
                if let Some(error) = msg.error {
                    panic!("Upload of {} failed: {}", &msg.task_id, error);
                }
                /* Ok */
            }
            _ => {
                panic!("Invalid response");
            }
        };
        match &mut task_ref.get_mut().state {
            TaskRuntimeState::Finished(ref mut finfo) => {
                finfo.placement.insert(worker_id);
            }
            _ => unreachable!(),
        };
        trace_task_new_finished(task_ref.get().id, size, worker_id);
    }

    Ok(())
}

fn scatter_tasks<D>(
    core: &Core,
    data: Vec<D>,
    workers: &[WorkerId],
    counter: usize,
) -> Vec<(WorkerId, Vec<D>)> {
    let cpus : Vec<usize> = workers.iter().map(|w_id| core.get_worker_by_id_or_panic(*w_id).ncpus as usize).collect();
    let total_cpus: usize = cpus.iter().sum();
    let mut counter = counter % total_cpus;

    let mut cpu = 0;
    let mut index = 0;
    for (i, _w_id) in workers.iter().enumerate() {
        let ncpus = cpus[i];
        if counter >= ncpus {
            counter -= ncpus;
        } else {
            cpu = counter;
            index = i;
            break;
        }
    }

    let mut worker_id = workers[index];
    let mut ncpus = cpus[index];

    let mut result: Map<WorkerId, Vec<D>> = Map::new();

    for d in data {
        result.entry(worker_id).or_default().push(d);
        cpu += 1;
        if cpu >= ncpus {
            cpu = 0;
            index += 1;
            index %= workers.len();
            worker_id = workers[index];
            ncpus = cpus[index];
        }
    }
    result.into_iter().collect()
}

pub async fn scatter(
    core_ref: &CoreRef,
    comm_ref: &CommSenderRef,
    workers: &[WorkerId],
    data: Vec<(TaskRef, BytesMut)>,
) {
    todo!()
    /*let counter = core_ref.get_mut().get_and_move_scatter_counter(data.len());
    let tasks: Vec<_> = data.iter().map(|(t_ref, _)| t_ref.clone()).collect();
    let placement = scatter_tasks(&core_ref.get(), data, workers, counter);
    let worker_futures = join_all(
        placement
            .into_iter()
            .map(|(worker, data)| update_data_on_worker(&core_ref, worker, data)),
    );

    // TODO: Proper error handling
    // Note that successful uploads are written in tasks
    worker_futures.await.iter().for_each(|x| assert!(x.is_ok()));

    let mut comm = comm_ref.get_mut();
    let mut core = core_ref.get_mut();
    let mut info = Vec::with_capacity(tasks.len());
    for t_ref in tasks {
        {
            let task = t_ref.get();
            info.push(NewFinishedTaskInfo {
                id: task.id,
                workers: task
                    .get_placement()
                    .unwrap()
                    .iter()
                    .copied()
                    .collect(),
                size: task.data_info().unwrap().size,
            });
        }
        core.add_task(t_ref);
    }
    let msg = ToSchedulerMessage::NewFinishedTask(info);
    comm.send_scheduler_message(msg);*/
}
