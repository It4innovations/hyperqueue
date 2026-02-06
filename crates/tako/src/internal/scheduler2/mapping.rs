use crate::internal::scheduler2::TaskQueue;
use crate::internal::scheduler2::solver::SchedulingSolution;
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::task::{ComputeTasksBuilder, Task, TaskRuntimeState};
use crate::internal::worker::task::TaskState;
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, Set, TaskId, WorkerId};
use fxhash::FxBuildHasher;
use hashbrown::hash_map::Entry;
use std::cmp::Reverse;

#[derive(Debug, Default)]
pub struct WorkerTaskMapping {
    pub(crate) sn_tasks_to_workers: Map<WorkerId, Vec<(TaskId, ResourceVariantId)>>,
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
    let mut result = WorkerTaskMapping::default();
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
                                result
                                    .sn_tasks_to_workers
                                    .entry(*w_id)
                                    .or_default()
                                    .push((task_id, v_id));
                                TaskRuntimeState::Assigned {
                                    worker_id: *w_id,
                                    rv_id: v_id,
                                }
                            }
                            TaskRuntimeState::Assigned { worker_id, rv_id } => {
                                unreachable!()
                            }
                            /*TaskRuntimeState::Retracting { source }
                            | TaskRuntimeState::Stealing { source, .. } => {
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

    result
        .sn_tasks_to_workers
        .iter_mut()
        .for_each(|(_, tasks)| {
            tasks.sort_by_key(|(task, _)| Reverse(task_map.get_task(*task).priority()));
        });
    result
}
// for batch in task_batches {
//     let resource_rq_id = batch.resource_rq_id;
//     let rqv = resource_map.get(batch.resource_rq_id);
//     for v_id in rqv.variant_ids() {
//         let mut counts: Vec<_> = workers
//             .iter()
//             .filter_map(|w| {
//                 placements
//                     .get(&(w.id, batch.resource_rq_id, v_id))
//                     .map(|(_, var_idx)| (w.id, values[*var_idx as usize].round() as u32))
//             })
//             .collect();
//         let sum = counts.iter().map(|(_, c)| c).sum::<u32>();
//         if sum == 0 {
//             continue;
//         }
//         let tasks = task_queues[resource_rq_id.as_usize()].take_tasks(sum);
//         let mut task_idx = 0;
//         'outer: loop {
//             for (w_id, c) in &mut counts {
//                 if *c > 0 {
//                     *c -= 1;
//                     result
//                         .task_to_workers
//                         .entry(*w_id)
//                         .or_default()
//                         .push((tasks[task_idx], v_id));
//                     task_idx += 1;
//                     if task_idx >= tasks.len() {
//                         break 'outer;
//                     }
//                 }
//             }
//         }
//     }
//     //workers.iter().map(|w| )
// }

impl WorkerTaskMapping {
    pub fn dump(&mut self) {
        println!("=======================");
        for (w, ts) in &self.sn_tasks_to_workers {
            print!("w{w}:");
            for (task_id, variant) in ts {
                if variant.is_first() {
                    print!(" {}", task_id)
                } else {
                    print!(" {}/{}", task_id, variant)
                }
            }
            println!();
        }
    }

    pub fn send_messages(&self, core: &mut Core, comm: &mut impl Comm) {
        for (worker_id, tasks) in &self.sn_tasks_to_workers {
            let mut task_msg_builder = ComputeTasksBuilder::default();
            for (task_id, variant) in tasks {
                if !variant.is_first() {
                    // TODO: Send variant to worker
                    todo!();
                }
                let task = core.get_task_mut(*task_id);
                if let Some(msg) = task_msg_builder.add_task(task, Vec::new()) {
                    comm.send_worker_message(*worker_id, &msg);
                }
            }
            if let Some(msg) = task_msg_builder.into_last_message() {
                comm.send_worker_message(*worker_id, &msg);
            }
        }
    }
}
