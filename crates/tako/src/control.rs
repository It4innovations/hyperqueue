use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orion::aead::SecretKey;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::events::EventProcessor;
use crate::gateway::{MultiNodeAllocationResponse, TaskSubmit, WorkerRuntimeInfo};
use crate::internal::common::error::DsError;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::scheduler::state::{run_scheduling_now, scheduler_loop};
use crate::internal::server::client::handle_new_tasks;
use crate::internal::server::comm::{Comm, CommSenderRef};
use crate::internal::server::core::{CoreRef, CustomConnectionHandler};
use crate::internal::server::explain::{
    TaskExplanation, task_explain_for_worker, task_explain_init,
};
use crate::internal::server::reactor::on_cancel_tasks;
use crate::internal::server::worker::DEFAULT_WORKER_OVERVIEW_INTERVAL;
use crate::resources::ResourceDescriptor;
use crate::{TaskId, WorkerId};

#[derive(Debug)]
pub struct WorkerTypeQuery {
    /// If True indicates that we do not have all information about this worker
    pub partial: bool,
    /// Worker resource descriptor
    pub descriptor: ResourceDescriptor,
    /// Worker time limit
    pub time_limit: Option<Duration>,
    /// Max number of workers for single-node tasks
    pub max_sn_workers: u32,
    /// How big allocations for multinode tasks can queue provide
    pub max_workers_per_allocation: u32,
    /// Number 0-1. Specifies the minimal utilization of the worker by single node tasks,
    /// if the worker is not utilized at least by `min_utilization` then it is not returned
    /// in `single_node_allocations`
    pub min_utilization: f32,
}

#[derive(Debug)]
pub struct NewWorkerAllocationResponse {
    /// Array of the same size as the number of queries, it returns the number of workers that should
    /// be spawned for the given query
    pub single_node_workers_per_query: Vec<usize>,
    pub multi_node_allocations: Vec<MultiNodeAllocationResponse>,
}

#[derive(Clone)]
pub struct ServerRef {
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
}

impl ServerRef {
    pub fn get_worker_listen_port(&self) -> u16 {
        self.core_ref.get().get_worker_listen_port()
    }

    pub fn add_new_tasks(&self, task_submit: TaskSubmit) -> crate::Result<()> {
        let mut core = self.core_ref.get_mut();
        let mut comm = self.comm_ref.get_mut();
        handle_new_tasks(&mut core, &mut comm, task_submit)
    }

    pub fn set_client_events(&self, client_events: Box<dyn EventProcessor>) {
        self.comm_ref.set_client_events(client_events);
    }

    pub fn cancel_tasks(&self, tasks: &[TaskId]) {
        log::debug!("Client asked for canceling tasks: {tasks:?}");
        let mut core = self.core_ref.get_mut();
        let mut comm = self.comm_ref.get_mut();
        on_cancel_tasks(&mut core, &mut *comm, tasks);
    }

    pub fn stop_worker(&self, worker_id: WorkerId) -> crate::Result<()> {
        let mut core = self.core_ref.get_mut();
        if let Some(ref mut worker) = core.get_worker_mut(worker_id) {
            worker.set_stopping_flag(true);
            let mut comm = self.comm_ref.get_mut();
            comm.send_worker_message(worker_id, &ToWorkerMessage::Stop);
            Ok(())
        } else {
            Err(format!("Worker with id {worker_id} not found").into())
        }
    }

    /* Ask scheduler for the information about how
      many workers of the given type is useful to spawn.

      In a situation that two worker types can be spawned to
      speed up a computation, but not both of them, then the priority
      is given by an order of by worker_queries, lesser index, higher priority

      Query:

      max_sn_workers defines how many of that worker type can outer system provides,
      if a big number is filled, it may be slow to compute the result.
      This is ment for single node tasks, i.e. they may or may not be in a same allocation.

      max_worker_per_allocation defines how many of that worker type
      we can get in one allocation at most.
      This is used for planning multi-node tasks.

      Note: This call may immediately call the full scheduler procedure.
      This should not bother the user of the call, except it is probably not a good
      idea to call this function often (several times per second) as it may bypass
      a scheduler time limitations.
    */
    pub fn new_worker_query(
        &self,
        queries: &[WorkerTypeQuery],
    ) -> crate::Result<NewWorkerAllocationResponse> {
        for query in queries {
            query.descriptor.validate(!query.partial)?;
        }
        let mut core = self.core_ref.get_mut();
        let mut comm = self.comm_ref.get_mut();
        if comm.get_scheduling_flag() {
            run_scheduling_now(&mut core, &mut comm, Instant::now())
        }
        Ok(compute_new_worker_query(&mut core, queries))
    }

    pub fn try_release_memory(&self) {
        let mut core = self.core_ref.get_mut();
        core.try_release_memory();
    }

    pub fn task_explain(&self, task_id: TaskId) -> crate::Result<TaskExplanation> {
        let core = self.core_ref.get();
        let Some(task) = core.find_task(task_id) else {
            return Err(DsError::from("Task not found"));
        };
        let resource_map = core.create_resource_map();
        let now = Instant::now();
        let mut explanation = task_explain_init(task);
        explanation.workers = core
            .get_workers()
            .map(|worker| {
                let group = core
                    .worker_groups()
                    .get(&worker.configuration.group)
                    .unwrap();
                Ok(task_explain_for_worker(
                    &resource_map,
                    task,
                    worker,
                    group,
                    now,
                ))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        explanation.workers.sort_by_key(|w| w.worker_id);
        Ok(explanation)
    }

    pub fn worker_info(&self, worker_id: WorkerId) -> Option<WorkerRuntimeInfo> {
        let core = self.core_ref.get();
        core.get_worker_map()
            .get(&worker_id)
            .map(|w| w.worker_info(core.task_map()))
    }

    pub fn add_worker_overview_listener(&self) {
        let mut core = self.core_ref.get_mut();
        let counter = core.worker_overview_listeners_mut();
        *counter += 1;
        if *counter == 1 {
            self.comm_ref.get_mut().broadcast_worker_message(
                &ToWorkerMessage::SetOverviewIntervalOverride(Some(
                    DEFAULT_WORKER_OVERVIEW_INTERVAL,
                )),
            );
        }
    }

    pub fn remove_worker_overview_listener(&self) {
        let mut core = self.core_ref.get_mut();
        let counter = core.worker_overview_listeners_mut();
        assert!(*counter > 0);
        *counter -= 1;
        if *counter == 0 {
            self.comm_ref
                .get_mut()
                .broadcast_worker_message(&ToWorkerMessage::SetOverviewIntervalOverride(None));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn server_start(
    listener: TcpListener,
    secret_key: Option<Arc<SecretKey>>,
    msd: Duration,
    panic_on_worker_lost: bool,
    idle_timeout: Option<Duration>,
    custom_conn_handler: Option<CustomConnectionHandler>,
    server_uid: String,
    worker_id_initial_value: WorkerId,
) -> crate::Result<(ServerRef, impl Future<Output = crate::Result<()>>)> {
    let listener_port = listener.local_addr()?.port();

    let scheduler_wakeup = Rc::new(Notify::new());

    let comm_ref = CommSenderRef::new(scheduler_wakeup.clone(), panic_on_worker_lost);

    let core_ref = CoreRef::new(
        listener_port,
        secret_key,
        idle_timeout,
        custom_conn_handler,
        server_uid,
        worker_id_initial_value,
    );
    let connections = crate::internal::server::rpc::connection_initiator(
        listener,
        core_ref.clone(),
        comm_ref.clone(),
    );

    let scheduler = scheduler_loop(core_ref.clone(), comm_ref.clone(), scheduler_wakeup, msd);

    let future = async move {
        tokio::select! {
            () = scheduler => {},
            r = connections => r?,
        };
        log::debug!("Waiting for scheduler to shut down...");
        log::info!("tako ends");
        Ok(())
    };

    Ok((ServerRef { core_ref, comm_ref }, future))
}
