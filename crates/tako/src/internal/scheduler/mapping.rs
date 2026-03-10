use crate::internal::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::internal::scheduler::solver::SchedulingSolution;
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::task::{ComputeTasksBuilder, Task, TaskRuntimeState};
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, Set, TaskId, WorkerId};
use fxhash::FxBuildHasher;
use hashbrown::hash_map::Entry;
use std::cmp::Reverse;

#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone, PartialEq, PartialOrd, Eq, Ord))]
pub struct WorkerTaskUpdate {
    pub(crate) assigned: Vec<(TaskId, ResourceVariantId)>,
    pub(crate) prefills: Vec<TaskId>,
    pub(crate) retracts: Vec<TaskId>,
}

#[derive(Debug, Default)]
pub struct WorkerTaskMapping {
    pub(crate) workers: Map<WorkerId, WorkerTaskUpdate>,
    pub(crate) mn_tasks_to_workers: Vec<TaskId>,
}

pub(crate) fn create_task_mapping(
    core: &mut Core,
    mut solution: SchedulingSolution,
) -> WorkerTaskMapping {
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        scheduler_state,
        ..
    } = core.split_mut();
    let mut mapping = WorkerTaskMapping::default();
    for ((resource_rq_id, v_id), mut counts) in solution.sn_counts.into_iter() {
        let rq = request_map.get(resource_rq_id).get(v_id);
        let sum = counts.iter().map(|(_, c)| c).sum::<u32>();
        let tasks = task_queues.get_mut(resource_rq_id).take_tasks(sum);
        let mut task_idx = 0;
        if !tasks.is_empty() {
            'outer: loop {
                for (w_id, c) in counts.iter_mut() {
                    if *c > 0 {
                        *c -= 1;
                        let task_id = tasks[task_idx];
                        worker_map.get_worker_mut(*w_id).insert_sn_task(task_id, rq);
                        let task = task_map.get_task_mut(task_id);
                        let new_state = match &task.state {
                            TaskRuntimeState::Waiting { .. } => {
                                log::debug!("Waiting task={} assigned to worker={}", task_id, w_id);
                                mapping
                                    .workers
                                    .entry(*w_id)
                                    .or_default()
                                    .assigned
                                    .push((task_id, v_id));
                                TaskRuntimeState::Assigned {
                                    worker_id: *w_id,
                                    rv_id: v_id,
                                }
                            }
                            TaskRuntimeState::Retracting {
                                worker_id: old_worker_id,
                            } => {
                                if old_worker_id != w_id {
                                    if let Some((old_target, v_id)) =
                                        scheduler_state.redirects.insert(task_id, (*w_id, v_id))
                                    {
                                        let rq = request_map.get(task.resource_rq_id).get(v_id);
                                        worker_map
                                            .get_worker_mut(old_target)
                                            .remove_sn_task(task_id, rq);
                                    }
                                }
                                TaskRuntimeState::Retracting {
                                    worker_id: *old_worker_id,
                                }
                            }
                            TaskRuntimeState::Prefilled {
                                worker_id: old_worker_id,
                            } => {
                                worker_map
                                    .get_mut(old_worker_id)
                                    .unwrap()
                                    .remove_prefill_task(task_id);
                                mapping
                                    .workers
                                    .entry(*old_worker_id)
                                    .or_default()
                                    .retracts
                                    .push(task_id);
                                assert!(
                                    scheduler_state
                                        .redirects
                                        .insert(task_id, (*w_id, v_id))
                                        .is_none()
                                );
                                TaskRuntimeState::Retracting {
                                    worker_id: *old_worker_id,
                                }
                            }
                            TaskRuntimeState::Assigned { worker_id, rv_id } => {
                                unreachable!()
                            }
                            TaskRuntimeState::Running { .. }
                            | TaskRuntimeState::RunningMultiNode(_)
                            | TaskRuntimeState::Finished => {
                                unreachable!()
                            }
                        };
                        task.state = new_state;
                        task_idx += 1;
                        if task_idx >= tasks.len() {
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    mapping.workers.iter_mut().for_each(|(_, up)| {
        up.assigned
            .sort_by_key(|(task, _)| Reverse(task_map.get_task(*task).priority()));
    });

    for ((resource_rq_id, _), worker_sets) in solution.mn_workers {
        for workers in worker_sets {
            let task_id = task_queues.get_mut(resource_rq_id).take_one().unwrap();
            log::debug!(
                "Multi-node task={} assigned to workers={:?}",
                task_id,
                workers
            );
            for (w_idx, w_id) in workers.iter().enumerate() {
                let worker = worker_map.get_worker_mut(*w_id);
                worker.set_mn_task(task_id, w_idx == 0);
            }
            let task = task_map.get_task_mut(task_id);
            mapping.mn_tasks_to_workers.push(task_id);
            let old_state =
                std::mem::replace(&mut task.state, TaskRuntimeState::RunningMultiNode(workers));
            assert!(matches!(
                old_state,
                TaskRuntimeState::Waiting { unfinished_deps: 0 }
            ));
        }
    }
    process_proactive_filling(core, &mut mapping);
    mapping
}

fn process_proactive_filling(core: &mut Core, mapping: &mut WorkerTaskMapping) {
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        scheduler_state,
        ..
    } = core.split_mut();
    let top_priority = task_queues.top_priority();
    for queue in task_queues.iter_mut() {
        if queue.top_priority() != Some(top_priority) {
            continue;
        };
        let size = queue
            .top_size_no_prefill()
            .saturating_sub(scheduler_state.config.proactive_filling_reserve);
        if size == 0 {
            continue;
        }
        let workers: Vec<_> = worker_map
            .values_mut()
            .filter(|worker| {
                let Some(sn) = worker.sn_assignment() else {
                    return false;
                };
                if !mapping
                    .workers
                    .get(&worker.id)
                    .map(|up| {
                        up.assigned.iter().any(|(t, _)| {
                            task_map.get_task(*t).resource_rq_id == queue.resource_rq_id
                        })
                    })
                    .unwrap_or(false)
                {
                    return false;
                }
                if sn
                    .prefilled_tasks
                    .iter()
                    .any(|t| task_map.get_task(*t).resource_rq_id == queue.resource_rq_id)
                {
                    return false;
                }
                true
            })
            .collect();
        if workers.is_empty() {
            continue;
        }
        let prefill_size =
            (size / workers.len() as u32).min(scheduler_state.config.proactive_filling_max);
        if prefill_size == 0 {
            continue;
        }
        for worker in workers {
            let tasks = queue.take_tasks_for_prefill(prefill_size);
            for task_id in &tasks {
                let task = task_map.get_task_mut(*task_id);
                assert!(task.is_waiting());
                task.state = TaskRuntimeState::Prefilled {
                    worker_id: worker.id,
                };
                worker.insert_prefill_task(*task_id);
            }
            mapping.workers.entry(worker.id).or_default().prefills = tasks;
        }
    }
    //task_queues.
}

impl WorkerTaskMapping {
    pub fn dump(&self) {
        println!("====== MAPPING =================");
        for (w, up) in &self.workers {
            print!("w{w}:");
            for (task_id, v) in &up.assigned {
                if v.is_first() {
                    print!(" {}", task_id)
                } else {
                    print!(" {}/{}", task_id, v)
                }
            }
            if !up.prefills.is_empty() {
                print!(" pf({})", up.prefills.len())
            }
            if !up.retracts.is_empty() {
                print!(" ret({})", up.retracts.len())
            }
            println!();
        }
    }

    pub fn send_messages(self, core: &mut Core, comm: &mut impl Comm) {
        for (worker_id, up) in self.workers {
            if !up.retracts.is_empty() {
                comm.send_worker_message(
                    worker_id,
                    &ToWorkerMessage::RetractTasks(TaskIdsMsg { ids: up.retracts }),
                );
            }
            let mut task_msg_builder = ComputeTasksBuilder::default();
            for task_id in &up.prefills {
                let task = core.get_task_mut(*task_id);
                if let Some(msg) = task_msg_builder.add_task(task, None, Vec::new()) {
                    comm.send_worker_message(worker_id, &msg);
                }
            }
            for (task_id, variant) in &up.assigned {
                let task = core.get_task_mut(*task_id);
                if let Some(msg) = task_msg_builder.add_task(task, Some(*variant), Vec::new()) {
                    comm.send_worker_message(worker_id, &msg);
                }
            }
            if let Some(msg) = task_msg_builder.into_last_message() {
                comm.send_worker_message(worker_id, &msg);
            }
        }
        for task_id in &self.mn_tasks_to_workers {
            let task = core.get_task(*task_id);
            let worker_ids = task.mn_placement().unwrap().to_vec();
            comm.send_worker_message(
                worker_ids[0],
                &ComputeTasksBuilder::single_task(task, 0.into(), worker_ids.to_vec()),
            );
        }
    }
}
