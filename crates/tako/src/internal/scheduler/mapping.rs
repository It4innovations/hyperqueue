use crate::internal::scheduler::solver::SchedulingSolution;
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::task::{ComputeTasksBuilder, Task, TaskRuntimeState};
use crate::internal::worker::task::TaskState;
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, Set, TaskId, WorkerId};
use fxhash::FxBuildHasher;
use hashbrown::hash_map::Entry;
use std::cmp::Reverse;

/// Always left at least this number of tasks out of filling
const PROACTIVE_FILLING_RESERVE: u32 = 64;
const PROACTIVE_FILLING_COUNT: u32 = 32;

#[derive(Debug, Default)]
pub struct WorkerTaskMapping {
    pub(crate) sn_tasks_to_workers: Map<WorkerId, Vec<(TaskId, ResourceVariantId)>>,
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
                        let task = task_map.get_task_mut(task_id);
                        let new_state = match &task.state {
                            TaskRuntimeState::Waiting { .. } => {
                                worker_map.get_worker_mut(*w_id).insert_sn_task(task_id, rq);
                                mapping
                                    .sn_tasks_to_workers
                                    .entry(*w_id)
                                    .or_default()
                                    .push((task_id, v_id));
                                TaskRuntimeState::Assigned {
                                    worker_id: *w_id,
                                    rv_id: v_id,
                                }
                            }
                            TaskRuntimeState::Retracting { source } => {
                                todo!()
                            }
                            TaskRuntimeState::Assigned { worker_id, rv_id } => {
                                unreachable!()
                            }
                            /*| TaskRuntimeState::Stealing { source, .. } => {
                                TaskRuntimeState::Stealing {
                                    source: *source,
                                    target: *w_id,
                                    rv_id: v_id,
                                }
                            }*/
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

    mapping
        .sn_tasks_to_workers
        .iter_mut()
        .for_each(|(_, tasks)| {
            tasks.sort_by_key(|(task, _)| Reverse(task_map.get_task(*task).priority()));
        });

    for ((resource_rq_id, _), worker_sets) in solution.mn_workers {
        for workers in worker_sets {
            let task_id = task_queues.get_mut(resource_rq_id).take_one().unwrap();
            for (w_idx, w_id) in workers.iter().enumerate() {
                let worker = worker_map.get_worker_mut(*w_id);
                worker.set_mn_task(task_id, w_idx != 0);
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

    mapping
}

/*fn process_proactive_filling(core: &mut Core, task: Task, mapping: &mut WorkerTaskMapping) {
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        ..
    } = core.split_mut();
    let top_priority = task_queues.top_priority();
    for queue in task_queues.iter_mut() {
        if queue.top_priority() != Some(top_priority) {
            continue;
        };
        let size = queue
            .top_size_without_filling()
            .saturating_sub(PROACTIVE_FILLING_RESERVE);
        for worker in worker_map.values_mut() {
            let Some(sn) = worker.sn_assignment() else {
                continue;
            };
            if !sn
                .assign_tasks
                .iter()
                .any(|t| task_map.get_task(*t).resource_rq_id == queue.resource_rq_id)
            {
                continue;
            }
            let n_tasks = sn
                .prefilled
                .get(&queue.resource_rq_id)
                .map(|ts| ts.len() as u32)
                .unwrap_or(0);
            let missing = PROACTIVE_FILLING_COUNT.saturating_sub(n_tasks);
            let out = mapping.sn_tasks_to_workers.entry(worker.id).or_default();
            for _ in 0..missing {
                let task_id = queue.take_one().unwrap();
                let task = task_map.get_task_mut(task_id);
                assert!(task.is_ready());
                task.state = TaskRuntimeState::Prefilled {
                    worker_id: worker.id,
                };
                worker.insert_prefill_task(task_id, queue.resource_rq_id);
                out.push((task_id, None))
            }
        }
    }
    //task_queues.
}*/

impl WorkerTaskMapping {
    pub fn dump(&mut self) {
        println!("=======================");
        for (w, ts) in &self.sn_tasks_to_workers {
            print!("w{w}:");
            for (task_id, v) in ts {
                if v.is_first() {
                    print!(" {}", task_id)
                } else {
                    print!(" {}/{}", task_id, v)
                }
            }
            println!();
        }
    }

    pub fn send_messages(&self, core: &mut Core, comm: &mut impl Comm) {
        for (worker_id, tasks) in &self.sn_tasks_to_workers {
            let mut task_msg_builder = ComputeTasksBuilder::default();
            for (task_id, variant) in tasks {
                let task = core.get_task_mut(*task_id);
                if let Some(msg) = task_msg_builder.add_task(task, *variant, Vec::new()) {
                    comm.send_worker_message(*worker_id, &msg);
                }
            }
            if let Some(msg) = task_msg_builder.into_last_message() {
                comm.send_worker_message(*worker_id, &msg);
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
